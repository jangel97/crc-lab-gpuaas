package main

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	coordinationv1 "k8s.io/api/coordination/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
)

const (
	labelManagedBy       = "app.kubernetes.io/managed-by"
	labelManagedByValue  = "vk-gpu-provider"
	labelSourceNamespace = "vk.gpuaas.io/source-namespace"
	labelSourceName      = "vk.gpuaas.io/source-name"
	labelSourcePod = "vk.gpuaas.io/source-pod"
)

type GPUProviderConfig struct {
	NodeName                  string
	WorkerNamespacePrefix     string
	GPUCount                  int
	DefaultRemoteStorageClass string
	TenantClient              kubernetes.Interface
	WorkerClient              kubernetes.Interface
}

type GPUProvider struct {
	cfg    GPUProviderConfig
	syncer *ResourceSyncer

	mu          sync.Mutex
	managedPods map[string]string // tenant "namespace/name" -> worker pod name
}

func NewGPUProvider(cfg GPUProviderConfig) *GPUProvider {
	return &GPUProvider{
		cfg:         cfg,
		syncer:      NewResourceSyncer(cfg.TenantClient, cfg.WorkerClient, cfg.WorkerNamespacePrefix, cfg.DefaultRemoteStorageClass),
		managedPods: make(map[string]string),
	}
}

func (p *GPUProvider) Run(ctx context.Context) error {
	if err := p.registerNode(ctx); err != nil {
		return fmt.Errorf("register node: %w", err)
	}
	klog.Infof("Virtual node %q registered", p.cfg.NodeName)

	go p.heartbeatLoop(ctx)

	workerLW := cache.NewFilteredListWatchFromClient(
		p.cfg.WorkerClient.CoreV1().RESTClient(),
		"pods",
		metav1.NamespaceAll,
		func(options *metav1.ListOptions) {
			options.LabelSelector = labelManagedBy + "=" + labelManagedByValue
		},
	)
	_, workerInformer := cache.NewInformer(workerLW, &corev1.Pod{}, 30*time.Second,
		cache.ResourceEventHandlerFuncs{
			AddFunc:    func(obj interface{}) { p.handleWorkerPodEvent(ctx, obj) },
			UpdateFunc: func(_, obj interface{}) { p.handleWorkerPodEvent(ctx, obj) },
		},
	)

	go workerInformer.Run(ctx.Done())
	if !cache.WaitForCacheSync(ctx.Done(), workerInformer.HasSynced) {
		return fmt.Errorf("worker informer cache sync failed")
	}
	klog.Info("Worker pod informer synced, managedPods rebuilt from existing worker pods")

	tenantLW := cache.NewFilteredListWatchFromClient(
		p.cfg.TenantClient.CoreV1().RESTClient(),
		"pods",
		metav1.NamespaceAll,
		func(options *metav1.ListOptions) {
			options.FieldSelector = fields.OneTermEqualSelector("spec.nodeName", p.cfg.NodeName).String()
		},
	)
	_, tenantInformer := cache.NewInformer(tenantLW, &corev1.Pod{}, 30*time.Second,
		cache.ResourceEventHandlerFuncs{
			AddFunc:    func(obj interface{}) { p.onTenantPodEvent(ctx, obj) },
			UpdateFunc: func(_, obj interface{}) { p.onTenantPodEvent(ctx, obj) },
			DeleteFunc: func(obj interface{}) { p.onTenantPodDelete(ctx, obj) },
		},
	)

	pvcLW := cache.NewFilteredListWatchFromClient(
		p.cfg.TenantClient.CoreV1().RESTClient(),
		"persistentvolumeclaims",
		metav1.NamespaceAll,
		func(options *metav1.ListOptions) {},
	)
	_, pvcInformer := cache.NewInformer(pvcLW, &corev1.PersistentVolumeClaim{}, 5*time.Minute,
		cache.ResourceEventHandlerFuncs{
			DeleteFunc: func(obj interface{}) { p.onControlPVCDelete(ctx, obj) },
		},
	)
	go pvcInformer.Run(ctx.Done())
	klog.Info("Control PVC deletion informer started")

	klog.Info("Starting tenant pod informer")
	tenantInformer.Run(ctx.Done())
	return ctx.Err()
}

func (p *GPUProvider) registerNode(ctx context.Context) error {
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: p.cfg.NodeName,
			Labels: map[string]string{
				"type":                   "virtual-kubelet",
				"kubernetes.io/role":     "agent",
				"kubernetes.io/os":       "linux",
				"kubernetes.io/arch":     "amd64",
				"node.kubernetes.io/gpu": "true",
			},
		},
		Spec: corev1.NodeSpec{
			Taints: []corev1.Taint{
				{
					Key:    "virtual-kubelet.io/provider",
					Value:  "gpu-provider",
					Effect: corev1.TaintEffectNoSchedule,
				},
				{
					Key:    "virtual-kubelet.io/provider",
					Value:  "gpu-provider",
					Effect: corev1.TaintEffectNoExecute,
				},
			},
		},
	}

	for attempt := 0; attempt < 5; attempt++ {
		existing, err := p.cfg.TenantClient.CoreV1().Nodes().Get(ctx, p.cfg.NodeName, metav1.GetOptions{})
		if err == nil {
			existing.Labels = node.Labels
			existing.Spec.Taints = node.Spec.Taints
			_, err = p.cfg.TenantClient.CoreV1().Nodes().Update(ctx, existing, metav1.UpdateOptions{})
			if errors.IsConflict(err) {
				time.Sleep(time.Duration(attempt+1) * time.Second)
				continue
			}
			if err != nil {
				return fmt.Errorf("update existing node: %w", err)
			}
		} else if errors.IsNotFound(err) {
			_, err = p.cfg.TenantClient.CoreV1().Nodes().Create(ctx, node, metav1.CreateOptions{})
			if err != nil && !errors.IsAlreadyExists(err) {
				return fmt.Errorf("create node: %w", err)
			}
		} else {
			return fmt.Errorf("get node: %w", err)
		}
		break
	}

	for attempt := 0; attempt < 5; attempt++ {
		err := p.updateNodeStatus(ctx)
		if err == nil {
			return nil
		}
		if errors.IsConflict(err) {
			time.Sleep(time.Duration(attempt+1) * time.Second)
			continue
		}
		return err
	}
	return p.updateNodeStatus(ctx)
}

func (p *GPUProvider) updateNodeStatus(ctx context.Context) error {
	node, err := p.cfg.TenantClient.CoreV1().Nodes().Get(ctx, p.cfg.NodeName, metav1.GetOptions{})
	if err != nil {
		return err
	}

	gpuQty := resource.MustParse(fmt.Sprintf("%d", p.cfg.GPUCount))
	node.Status = corev1.NodeStatus{
		Capacity: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("8"),
			corev1.ResourceMemory: resource.MustParse("24Gi"),
			corev1.ResourcePods:   resource.MustParse("20"),
			"nvidia.com/gpu":      gpuQty,
		},
		Allocatable: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("8"),
			corev1.ResourceMemory: resource.MustParse("24Gi"),
			corev1.ResourcePods:   resource.MustParse("20"),
			"nvidia.com/gpu":      gpuQty,
		},
		Conditions: []corev1.NodeCondition{
			{
				Type:               corev1.NodeReady,
				Status:             corev1.ConditionTrue,
				LastHeartbeatTime:  metav1.Now(),
				LastTransitionTime: metav1.Now(),
				Reason:             "KubeletReady",
				Message:            "vk-gpu-provider is ready",
			},
			{
				Type:               corev1.NodeMemoryPressure,
				Status:             corev1.ConditionFalse,
				LastHeartbeatTime:  metav1.Now(),
				LastTransitionTime: metav1.Now(),
			},
			{
				Type:               corev1.NodeDiskPressure,
				Status:             corev1.ConditionFalse,
				LastHeartbeatTime:  metav1.Now(),
				LastTransitionTime: metav1.Now(),
			},
			{
				Type:               corev1.NodePIDPressure,
				Status:             corev1.ConditionFalse,
				LastHeartbeatTime:  metav1.Now(),
				LastTransitionTime: metav1.Now(),
			},
		},
		NodeInfo: corev1.NodeSystemInfo{
			OperatingSystem: "linux",
			Architecture:    "amd64",
			KubeletVersion:  "v1.31.0-vk",
		},
		Addresses: []corev1.NodeAddress{
			{
				Type:    corev1.NodeInternalIP,
				Address: "127.0.0.1",
			},
		},
	}

	_, err = p.cfg.TenantClient.CoreV1().Nodes().UpdateStatus(ctx, node, metav1.UpdateOptions{})
	return err
}

func (p *GPUProvider) heartbeatLoop(ctx context.Context) {
	leaseName := p.cfg.NodeName
	leaseNS := "kube-node-lease"

	dur := int32(40)
	lease := &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{
			Name:      leaseName,
			Namespace: leaseNS,
		},
		Spec: coordinationv1.LeaseSpec{
			HolderIdentity:       &leaseName,
			LeaseDurationSeconds: &dur,
			RenewTime:            &metav1.MicroTime{Time: time.Now()},
		},
	}

	_, err := p.cfg.TenantClient.CoordinationV1().Leases(leaseNS).Create(ctx, lease, metav1.CreateOptions{})
	if err != nil && !errors.IsAlreadyExists(err) {
		klog.Errorf("Failed to create lease: %v", err)
	}

	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			p.renewLease(ctx, leaseName, leaseNS)
			if err := p.updateNodeStatus(ctx); err != nil {
				klog.Errorf("Failed to update node status: %v", err)
			}
		}
	}
}

func (p *GPUProvider) renewLease(ctx context.Context, name, ns string) {
	lease, err := p.cfg.TenantClient.CoordinationV1().Leases(ns).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		klog.Errorf("Failed to get lease: %v", err)
		return
	}
	lease.Spec.RenewTime = &metav1.MicroTime{Time: time.Now()}
	_, err = p.cfg.TenantClient.CoordinationV1().Leases(ns).Update(ctx, lease, metav1.UpdateOptions{})
	if err != nil {
		klog.Errorf("Failed to renew lease: %v", err)
	}
}

func (p *GPUProvider) onTenantPodEvent(ctx context.Context, obj interface{}) {
	pod, ok := obj.(*corev1.Pod)
	if !ok {
		return
	}
	p.handleTenantPod(ctx, pod)
}

func (p *GPUProvider) onTenantPodDelete(ctx context.Context, obj interface{}) {
	pod, ok := obj.(*corev1.Pod)
	if !ok {
		tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			return
		}
		pod, ok = tombstone.Obj.(*corev1.Pod)
		if !ok {
			return
		}
	}
	p.handleTenantPodDeleted(ctx, pod)
}

func (p *GPUProvider) onControlPVCDelete(ctx context.Context, obj interface{}) {
	pvc, ok := obj.(*corev1.PersistentVolumeClaim)
	if !ok {
		tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			return
		}
		pvc, ok = tombstone.Obj.(*corev1.PersistentVolumeClaim)
		if !ok {
			return
		}
	}

	if pvc.Spec.StorageClassName == nil || *pvc.Spec.StorageClassName != catapultStorageClass {
		return
	}

	klog.Infof("Control PVC deleted: %s/%s (storageClass=%s), cleaning up execution PVC",
		pvc.Namespace, pvc.Name, catapultStorageClass)
	if err := p.syncer.CleanupExecutionPVC(ctx, pvc.Namespace, pvc.Name); err != nil {
		klog.Errorf("Failed to cleanup execution PVC for %s/%s: %v", pvc.Namespace, pvc.Name, err)
	}
}

func (p *GPUProvider) handleWorkerPodEvent(ctx context.Context, obj interface{}) {
	workerPod, ok := obj.(*corev1.Pod)
	if !ok {
		return
	}

	sourceNS := workerPod.Labels[labelSourceNamespace]
	sourceName := workerPod.Labels[labelSourceName]
	if sourceNS == "" || sourceName == "" {
		return
	}

	key := sourceNS + "/" + sourceName
	p.mu.Lock()
	if _, exists := p.managedPods[key]; !exists {
		p.managedPods[key] = workerPod.Name
		klog.Infof("Recovered managed pod mapping: %s -> %s", key, workerPod.Name)
	}
	p.mu.Unlock()

	p.syncStatusToTenant(ctx, workerPod)
}

func (p *GPUProvider) handleTenantPod(ctx context.Context, pod *corev1.Pod) {
	if isSystemNamespace(pod.Namespace) {
		return
	}

	key := pod.Namespace + "/" + pod.Name

	p.mu.Lock()
	_, exists := p.managedPods[key]
	p.mu.Unlock()

	if pod.DeletionTimestamp != nil {
		if exists {
			p.handleTenantPodDeleted(ctx, pod)
		}
		return
	}

	if exists {
		return
	}

	if pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed {
		return
	}

	klog.Infof("New pod assigned to virtual node: %s", key)

	workerNS := workerNamespace(p.cfg.WorkerNamespacePrefix, pod.Namespace)
	if err := p.ensureNamespace(ctx, workerNS); err != nil {
		klog.Errorf("Failed to ensure worker namespace %s: %v", workerNS, err)
		p.setPodStatus(ctx, pod, corev1.PodFailed, "NamespaceCreateFailed", err.Error())
		return
	}

	pvcMap, err := p.syncer.ValidateAndSyncPVCs(ctx, pod)
	if err != nil {
		klog.Errorf("PVC validation failed for %s: %v", key, err)
		p.setPodStatus(ctx, pod, corev1.PodFailed, "InvalidPVCStorageClass", err.Error())
		return
	}

	if err := p.syncer.SyncResources(ctx, pod); err != nil {
		klog.Errorf("Failed to sync resources for %s: %v", key, err)
		p.setPodStatus(ctx, pod, corev1.PodFailed, "ResourceSyncFailed", err.Error())
		return
	}

	if err := p.syncer.SyncHeadlessServices(ctx, pod); err != nil {
		klog.Warningf("Failed to sync headless services for %s: %v", key, err)
	}

	workerPod := p.transformPod(pod, workerNS, pvcMap)
	created, err := p.cfg.WorkerClient.CoreV1().Pods(workerNS).Create(ctx, workerPod, metav1.CreateOptions{})
	if err != nil {
		if errors.IsAlreadyExists(err) {
			klog.Infof("Worker pod already exists for %s", key)
			p.mu.Lock()
			p.managedPods[key] = workerPod.Name
			p.mu.Unlock()
			return
		}
		klog.Errorf("Failed to create worker pod for %s: %v", key, err)
		p.setPodStatus(ctx, pod, corev1.PodFailed, "WorkerPodCreateFailed", err.Error())
		return
	}

	klog.Infof("Created worker pod %s/%s for tenant pod %s", workerNS, created.Name, key)
	p.mu.Lock()
	p.managedPods[key] = created.Name
	p.mu.Unlock()
}

func (p *GPUProvider) handleTenantPodDeleted(ctx context.Context, pod *corev1.Pod) {
	if isSystemNamespace(pod.Namespace) {
		return
	}

	key := pod.Namespace + "/" + pod.Name

	p.mu.Lock()
	workerPodName, exists := p.managedPods[key]
	delete(p.managedPods, key)
	p.mu.Unlock()

	if !exists {
		return
	}

	workerNS := workerNamespace(p.cfg.WorkerNamespacePrefix, pod.Namespace)
	klog.Infof("Tenant pod deleted: %s, cleaning up worker pod %s", key, workerPodName)

	err := p.cfg.WorkerClient.CoreV1().Pods(workerNS).Delete(ctx, workerPodName, metav1.DeleteOptions{})
	if err != nil && !errors.IsNotFound(err) {
		klog.Errorf("Failed to delete worker pod %s: %v", workerPodName, err)
	}

	if err := p.syncer.CleanupResources(ctx, pod.Namespace, pod.Name); err != nil {
		klog.Errorf("Failed to cleanup synced resources for %s: %v", key, err)
	}
}

func (p *GPUProvider) ensureNamespace(ctx context.Context, ns string) error {
	_, err := p.cfg.WorkerClient.CoreV1().Namespaces().Get(ctx, ns, metav1.GetOptions{})
	if err == nil {
		return nil
	}
	if !errors.IsNotFound(err) {
		return err
	}
	nsObj := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: ns,
			Labels: map[string]string{
				labelManagedBy: labelManagedByValue,
			},
		},
	}
	_, err = p.cfg.WorkerClient.CoreV1().Namespaces().Create(ctx, nsObj, metav1.CreateOptions{})
	if errors.IsAlreadyExists(err) {
		return nil
	}
	return err
}

func workerPodName(namespace, name string) string {
	return fmt.Sprintf("%s--%s", namespace, name)
}

func workerNamespace(prefix, tenantNS string) string {
	return prefix + tenantNS
}

func isSystemNamespace(ns string) bool {
	return strings.HasPrefix(ns, "openshift-") ||
		strings.HasPrefix(ns, "kube-") ||
		strings.HasPrefix(ns, "redhat-ods-") ||
		ns == "default" ||
		ns == "kueue-system"
}

var labelSkipSet = map[string]bool{
	"kueue.x-k8s.io/queue-name": true,
	labelManagedBy:               true,
}

var labelSkipPrefixes = []string{
	"openshift.io/",
	"pod-security.kubernetes.io/",
}

var annotationSkipPrefixes = []string{
	"openshift.io/",
	"kubernetes.io/",
	"k8s.ovn.org/",
}

func hasAnyPrefix(s string, prefixes []string) bool {
	for _, p := range prefixes {
		if strings.HasPrefix(s, p) {
			return true
		}
	}
	return false
}

func (p *GPUProvider) transformPod(pod *corev1.Pod, workerNS string, pvcNameMap map[string]string) *corev1.Pod {
	workerPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      workerPodName(pod.Namespace, pod.Name),
			Namespace: workerNS,
			Labels: map[string]string{
				labelManagedBy:       labelManagedByValue,
				labelSourceNamespace: pod.Namespace,
				labelSourceName:      pod.Name,
			},
			Annotations: map[string]string{},
		},
		Spec: *pod.Spec.DeepCopy(),
	}

	for k, v := range pod.Labels {
		if labelSkipSet[k] {
			continue
		}
		if hasAnyPrefix(k, labelSkipPrefixes) {
			continue
		}
		workerPod.Labels[k] = v
	}

	for k, v := range pod.Annotations {
		if hasAnyPrefix(k, annotationSkipPrefixes) {
			continue
		}
		workerPod.Annotations[k] = v
	}

	workerPod.Spec.NodeName = ""
	workerPod.Spec.NodeSelector = nil
	workerPod.Spec.Affinity = nil
	workerPod.Spec.Tolerations = nil
	workerPod.Spec.SchedulerName = ""
	workerPod.Spec.Priority = nil
	workerPod.Spec.PriorityClassName = ""

	if workerPod.Spec.ServiceAccountName == "default" || workerPod.Spec.ServiceAccountName == "" {
		workerPod.Spec.ServiceAccountName = "default"
	}

	workerPod.Spec.Volumes = filterVolumes(workerPod.Spec.Volumes)

	for i, v := range workerPod.Spec.Volumes {
		if v.PersistentVolumeClaim != nil {
			if execName, ok := pvcNameMap[v.PersistentVolumeClaim.ClaimName]; ok {
				workerPod.Spec.Volumes[i].PersistentVolumeClaim.ClaimName = execName
			}
		}
	}

	for i := range workerPod.Spec.Containers {
		workerPod.Spec.Containers[i].VolumeMounts = filterVolumeMounts(
			workerPod.Spec.Containers[i].VolumeMounts,
			workerPod.Spec.Volumes,
		)
	}
	for i := range workerPod.Spec.InitContainers {
		workerPod.Spec.InitContainers[i].VolumeMounts = filterVolumeMounts(
			workerPod.Spec.InitContainers[i].VolumeMounts,
			workerPod.Spec.Volumes,
		)
	}

	// SecurityContext is passed through as-is. We cannot distinguish
	// user-set fields from SCC-injected ones (see docs/security-context-handling.md).
	// If a value is incompatible with the worker cluster's SCC/PSA,
	// the pod fails visibly rather than losing user intent silently.

	return workerPod
}


func filterVolumes(volumes []corev1.Volume) []corev1.Volume {
	var filtered []corev1.Volume
	for _, v := range volumes {
		if v.Projected != nil {
			isServiceAccountProjected := false
			for _, src := range v.Projected.Sources {
				if src.ServiceAccountToken != nil {
					isServiceAccountProjected = true
					break
				}
			}
			if isServiceAccountProjected {
				continue
			}
		}
		if strings.HasPrefix(v.Name, "kube-api-access") {
			continue
		}
		filtered = append(filtered, v)
	}
	return filtered
}

func filterVolumeMounts(mounts []corev1.VolumeMount, volumes []corev1.Volume) []corev1.VolumeMount {
	volumeNames := make(map[string]bool)
	for _, v := range volumes {
		volumeNames[v.Name] = true
	}

	var filtered []corev1.VolumeMount
	for _, m := range mounts {
		if volumeNames[m.Name] {
			filtered = append(filtered, m)
		}
	}
	return filtered
}

func (p *GPUProvider) setPodStatus(ctx context.Context, pod *corev1.Pod, phase corev1.PodPhase, reason, message string) {
	pod = pod.DeepCopy()
	pod.Status.Phase = phase
	pod.Status.Reason = reason
	pod.Status.Message = message
	_, err := p.cfg.TenantClient.CoreV1().Pods(pod.Namespace).UpdateStatus(ctx, pod, metav1.UpdateOptions{})
	if err != nil {
		klog.Errorf("Failed to update tenant pod status %s/%s: %v", pod.Namespace, pod.Name, err)
	}
}

func (p *GPUProvider) syncStatusToTenant(ctx context.Context, workerPod *corev1.Pod) {
	sourceNS := workerPod.Labels[labelSourceNamespace]
	sourceName := workerPod.Labels[labelSourceName]
	if sourceNS == "" || sourceName == "" {
		return
	}

	tenantPod, err := p.cfg.TenantClient.CoreV1().Pods(sourceNS).Get(ctx, sourceName, metav1.GetOptions{})
	if err != nil {
		if !errors.IsNotFound(err) {
			klog.Errorf("Failed to get tenant pod %s/%s: %v", sourceNS, sourceName, err)
		}
		return
	}

	if tenantPod.Status.Phase == workerPod.Status.Phase &&
		tenantPod.Status.PodIP == workerPod.Status.PodIP &&
		!containerStatusChanged(tenantPod, workerPod) {
		return
	}

	tenantPod = tenantPod.DeepCopy()
	tenantPod.Status.Phase = workerPod.Status.Phase
	tenantPod.Status.Message = workerPod.Status.Message
	tenantPod.Status.Reason = workerPod.Status.Reason
	tenantPod.Status.Conditions = workerPod.Status.Conditions
	tenantPod.Status.ContainerStatuses = workerPod.Status.ContainerStatuses
	tenantPod.Status.InitContainerStatuses = workerPod.Status.InitContainerStatuses
	tenantPod.Status.StartTime = workerPod.Status.StartTime
	tenantPod.Status.PodIP = workerPod.Status.PodIP
	tenantPod.Status.PodIPs = workerPod.Status.PodIPs

	_, err = p.cfg.TenantClient.CoreV1().Pods(sourceNS).UpdateStatus(ctx, tenantPod, metav1.UpdateOptions{})
	if err != nil {
		klog.Errorf("Failed to sync status to tenant pod %s/%s: %v", sourceNS, sourceName, err)
	} else {
		klog.Infof("Synced status %s -> tenant pod %s/%s", workerPod.Status.Phase, sourceNS, sourceName)
	}
}

func containerStatusChanged(tenant, worker *corev1.Pod) bool {
	if len(tenant.Status.ContainerStatuses) != len(worker.Status.ContainerStatuses) {
		return true
	}
	for i := range worker.Status.ContainerStatuses {
		if i >= len(tenant.Status.ContainerStatuses) {
			return true
		}
		if worker.Status.ContainerStatuses[i].Ready != tenant.Status.ContainerStatuses[i].Ready {
			return true
		}
		if worker.Status.ContainerStatuses[i].RestartCount != tenant.Status.ContainerStatuses[i].RestartCount {
			return true
		}
	}
	return false
}
