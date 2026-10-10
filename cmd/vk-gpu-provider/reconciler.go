package main

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
)

type DepReconciler struct {
	queue       workqueue.RateLimitingInterface
	syncer      *ResourceSyncer
	getPodSnap  func() map[string]*corev1.Pod
	resyncEvery time.Duration
}

type desiredState struct {
	secrets         map[string]bool
	configMaps      map[string]bool
	serviceAccounts map[string]bool
	services        map[string]*corev1.Service
}

func NewDepReconciler(syncer *ResourceSyncer, getPodSnap func() map[string]*corev1.Pod, resyncEvery time.Duration) *DepReconciler {
	return &DepReconciler{
		queue:       workqueue.NewRateLimitingQueue(workqueue.DefaultControllerRateLimiter()),
		syncer:      syncer,
		getPodSnap:  getPodSnap,
		resyncEvery: resyncEvery,
	}
}

func (r *DepReconciler) Enqueue(tenantNS string) {
	r.queue.Add(tenantNS)
}

func (r *DepReconciler) Run(ctx context.Context) {
	defer r.queue.ShutDown()
	klog.Info("Dependency reconciler started")

	go func() {
		ticker := time.NewTicker(r.resyncEvery)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				r.resyncAll()
			}
		}
	}()

	for r.processNext(ctx) {
	}
	klog.Info("Dependency reconciler stopped")
}

func (r *DepReconciler) processNext(ctx context.Context) bool {
	key, shutdown := r.queue.Get()
	if shutdown {
		return false
	}
	defer r.queue.Done(key)

	ns := key.(string)
	if err := r.reconcileNamespace(ctx, ns); err != nil {
		r.queue.AddRateLimited(key)
		klog.Errorf("Reconcile %s failed (will retry): %v", ns, err)
		return true
	}

	r.queue.Forget(key)
	return true
}

func (r *DepReconciler) resyncAll() {
	snap := r.getPodSnap()
	namespaces := make(map[string]bool)
	for key := range snap {
		ns, _, _ := splitKey(key)
		namespaces[ns] = true
	}
	for ns := range namespaces {
		r.queue.Add(ns)
	}
	if len(namespaces) > 0 {
		klog.V(4).Infof("Periodic resync enqueued %d namespace(s)", len(namespaces))
	}
}

func (r *DepReconciler) reconcileNamespace(ctx context.Context, tenantNS string) error {
	snap := r.getPodSnap()
	var pods []*corev1.Pod
	for key, pod := range snap {
		ns, _, _ := splitKey(key)
		if ns == tenantNS {
			pods = append(pods, pod)
		}
	}

	if len(pods) == 0 {
		return r.garbageCollect(ctx, tenantNS, &desiredState{
			secrets:         map[string]bool{},
			configMaps:      map[string]bool{},
			serviceAccounts: map[string]bool{},
			services:        map[string]*corev1.Service{},
		})
	}

	desired, err := r.computeDesiredState(ctx, tenantNS, pods)
	if err != nil {
		return fmt.Errorf("compute desired state for %s: %w", tenantNS, err)
	}

	var firstErr error
	collect := func(err error) {
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}

	for name := range desired.secrets {
		collect(r.syncer.syncSecret(ctx, tenantNS, name))
	}
	for name := range desired.configMaps {
		collect(r.syncer.syncConfigMap(ctx, tenantNS, name))
	}
	for name := range desired.serviceAccounts {
		collect(r.syncer.syncServiceAccount(ctx, tenantNS, name))
	}
	for _, svc := range desired.services {
		collect(r.syncer.syncService(ctx, tenantNS, svc))
	}

	collect(r.garbageCollect(ctx, tenantNS, desired))

	if firstErr == nil {
		klog.V(4).Infof("Reconciled namespace %s: %d secrets, %d configmaps, %d SAs, %d services",
			tenantNS, len(desired.secrets), len(desired.configMaps),
			len(desired.serviceAccounts), len(desired.services))
	}
	return firstErr
}

func (r *DepReconciler) computeDesiredState(ctx context.Context, tenantNS string, pods []*corev1.Pod) (*desiredState, error) {
	ds := &desiredState{
		secrets:         make(map[string]bool),
		configMaps:      make(map[string]bool),
		serviceAccounts: make(map[string]bool),
		services:        make(map[string]*corev1.Service),
	}

	for _, pod := range pods {
		secrets, configmaps := discoverReferences(pod)
		for _, name := range secrets {
			ds.secrets[name] = true
		}
		for _, name := range configmaps {
			ds.configMaps[name] = true
		}
		if pod.Spec.ServiceAccountName != "" && pod.Spec.ServiceAccountName != "default" {
			ds.serviceAccounts[pod.Spec.ServiceAccountName] = true
		}

		svcs, err := r.syncer.discoverHeadlessServices(ctx, pod)
		if err != nil {
			klog.Warningf("Failed to discover headless services for %s/%s: %v", tenantNS, pod.Name, err)
			continue
		}
		for _, svc := range svcs {
			ds.services[svc.Name] = svc
		}
	}

	return ds, nil
}

func (r *DepReconciler) garbageCollect(ctx context.Context, tenantNS string, desired *desiredState) error {
	actual, err := r.syncer.listManagedResources(ctx, tenantNS)
	if err != nil {
		return fmt.Errorf("list managed resources in %s: %w", tenantNS, err)
	}

	wns := r.syncer.workerNamespace(tenantNS)
	var firstErr error
	collect := func(err error) {
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}

	for _, name := range actual.secrets {
		if !desired.secrets[name] {
			err := r.syncer.workerClient.CoreV1().Secrets(wns).Delete(ctx, name, metav1.DeleteOptions{})
			if err != nil && !errors.IsNotFound(err) {
				collect(fmt.Errorf("delete orphaned secret %s/%s: %w", wns, name, err))
			} else if err == nil {
				klog.Infof("Garbage-collected orphaned secret %s/%s", wns, name)
			}
		}
	}
	for _, name := range actual.configMaps {
		if !desired.configMaps[name] {
			err := r.syncer.workerClient.CoreV1().ConfigMaps(wns).Delete(ctx, name, metav1.DeleteOptions{})
			if err != nil && !errors.IsNotFound(err) {
				collect(fmt.Errorf("delete orphaned configmap %s/%s: %w", wns, name, err))
			} else if err == nil {
				klog.Infof("Garbage-collected orphaned configmap %s/%s", wns, name)
			}
		}
	}
	for _, name := range actual.serviceAccounts {
		if !desired.serviceAccounts[name] {
			err := r.syncer.workerClient.CoreV1().ServiceAccounts(wns).Delete(ctx, name, metav1.DeleteOptions{})
			if err != nil && !errors.IsNotFound(err) {
				collect(fmt.Errorf("delete orphaned serviceaccount %s/%s: %w", wns, name, err))
			} else if err == nil {
				klog.Infof("Garbage-collected orphaned serviceaccount %s/%s", wns, name)
			}
		}
	}
	for _, name := range actual.services {
		if _, wanted := desired.services[name]; !wanted {
			err := r.syncer.workerClient.CoreV1().Services(wns).Delete(ctx, name, metav1.DeleteOptions{})
			if err != nil && !errors.IsNotFound(err) {
				collect(fmt.Errorf("delete orphaned service %s/%s: %w", wns, name, err))
			} else if err == nil {
				klog.Infof("Garbage-collected orphaned service %s/%s", wns, name)
			}
		}
	}

	return firstErr
}

func splitKey(key string) (namespace, name string, ok bool) {
	for i := 0; i < len(key); i++ {
		if key[i] == '/' {
			return key[:i], key[i+1:], true
		}
	}
	return key, "", false
}
