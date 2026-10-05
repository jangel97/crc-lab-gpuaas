package main

import (
	"context"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"

	dto "github.com/prometheus/client_model/go"
	"github.com/virtual-kubelet/virtual-kubelet/errdefs"
	"github.com/virtual-kubelet/virtual-kubelet/node"
	"github.com/virtual-kubelet/virtual-kubelet/node/api"
	"github.com/virtual-kubelet/virtual-kubelet/node/api/statsv1alpha1"
)

const (
	labelManagedBy       = "app.kubernetes.io/managed-by"
	labelManagedByValue  = "vk-gpu-provider"
	labelSourceNamespace = "vk.gpuaas.io/source-namespace"
	labelSourceName      = "vk.gpuaas.io/source-name"
	labelSourcePod       = "vk.gpuaas.io/source-pod"
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
	managedPods map[string]string      // tenant "namespace/name" -> worker pod name
	podCache    map[string]*corev1.Pod // tenant "namespace/name" -> tenant-view pod with worker status
	notifyCb    func(*corev1.Pod)
}

func NewGPUProvider(cfg GPUProviderConfig) *GPUProvider {
	return &GPUProvider{
		cfg:         cfg,
		syncer:      NewResourceSyncer(cfg.TenantClient, cfg.WorkerClient, cfg.WorkerNamespacePrefix, cfg.DefaultRemoteStorageClass),
		managedPods: make(map[string]string),
		podCache:    make(map[string]*corev1.Pod),
	}
}

// Compile-time interface checks.
var (
	_ node.PodLifecycleHandler = (*GPUProvider)(nil)
	_ node.PodNotifier         = (*GPUProvider)(nil)
	_ node.NodeProvider        = (*GPUProvider)(nil)
)

// --- PodLifecycleHandler ---

func (p *GPUProvider) CreatePod(ctx context.Context, pod *corev1.Pod) error {
	if isSystemNamespace(pod.Namespace) {
		return nil
	}

	key := pod.Namespace + "/" + pod.Name

	p.mu.Lock()
	_, exists := p.managedPods[key]
	p.mu.Unlock()
	if exists {
		return nil
	}

	// TODO(upstream): VK library calls PopulateEnvironmentVariables before
	// CreatePod, resolving secretKeyRef/configMapKeyRef to literal values.
	// This breaks cross-cluster providers that need the original refs for
	// resource sync. Propose an opt-out in virtual-kubelet/virtual-kubelet.
	// Workaround: re-read the pod from the API server.
	freshPod, err := p.cfg.TenantClient.CoreV1().Pods(pod.Namespace).Get(ctx, pod.Name, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("re-read pod %s: %w", key, err)
	}
	pod = freshPod

	klog.Infof("New pod assigned to virtual node: %s", key)

	workerNS := workerNamespace(p.cfg.WorkerNamespacePrefix, pod.Namespace)
	if err := p.ensureNamespace(ctx, workerNS); err != nil {
		klog.Errorf("Failed to ensure worker namespace %s: %v", workerNS, err)
		p.storeAndNotify(pod, corev1.PodFailed, "NamespaceCreateFailed", err.Error())
		return nil
	}

	pvcMap, err := p.syncer.ValidateAndSyncPVCs(ctx, pod)
	if err != nil {
		klog.Errorf("PVC validation failed for %s: %v", key, err)
		p.storeAndNotify(pod, corev1.PodFailed, "InvalidPVCStorageClass", err.Error())
		return nil
	}

	if err := p.syncer.SyncResources(ctx, pod); err != nil {
		klog.Errorf("Failed to sync resources for %s: %v", key, err)
		return err
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
			return nil
		}
		klog.Errorf("Failed to create worker pod for %s: %v", key, err)
		return err
	}

	klog.Infof("Created worker pod %s/%s for tenant pod %s", workerNS, created.Name, key)
	p.mu.Lock()
	p.managedPods[key] = created.Name
	p.mu.Unlock()
	return nil
}

func (p *GPUProvider) UpdatePod(ctx context.Context, pod *corev1.Pod) error {
	return nil
}

func (p *GPUProvider) DeletePod(ctx context.Context, pod *corev1.Pod) error {
	if isSystemNamespace(pod.Namespace) {
		return nil
	}

	key := pod.Namespace + "/" + pod.Name

	p.mu.Lock()
	wpName, exists := p.managedPods[key]
	delete(p.managedPods, key)
	delete(p.podCache, key)
	p.mu.Unlock()

	if !exists {
		return nil
	}

	workerNS := workerNamespace(p.cfg.WorkerNamespacePrefix, pod.Namespace)
	klog.Infof("Tenant pod deleted: %s, cleaning up worker pod %s", key, wpName)

	err := p.cfg.WorkerClient.CoreV1().Pods(workerNS).Delete(ctx, wpName, metav1.DeleteOptions{})
	if err != nil && !errors.IsNotFound(err) {
		klog.Errorf("Failed to delete worker pod %s: %v", wpName, err)
	}

	if err := p.syncer.CleanupResources(ctx, pod.Namespace, pod.Name); err != nil {
		klog.Errorf("Failed to cleanup synced resources for %s: %v", key, err)
	}

	return nil
}

func (p *GPUProvider) GetPod(ctx context.Context, namespace, name string) (*corev1.Pod, error) {
	key := namespace + "/" + name
	p.mu.Lock()
	pod, ok := p.podCache[key]
	p.mu.Unlock()
	if !ok {
		return nil, errdefs.NotFoundf("pod %s not found", key)
	}
	return pod.DeepCopy(), nil
}

func (p *GPUProvider) GetPodStatus(ctx context.Context, namespace, name string) (*corev1.PodStatus, error) {
	key := namespace + "/" + name
	p.mu.Lock()
	pod, ok := p.podCache[key]
	p.mu.Unlock()
	if !ok {
		return nil, errdefs.NotFoundf("pod %s not found", key)
	}
	return pod.Status.DeepCopy(), nil
}

func (p *GPUProvider) GetPods(ctx context.Context) ([]*corev1.Pod, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	pods := make([]*corev1.Pod, 0, len(p.podCache))
	for _, pod := range p.podCache {
		pods = append(pods, pod.DeepCopy())
	}
	return pods, nil
}

// --- PodNotifier ---

func (p *GPUProvider) NotifyPods(ctx context.Context, cb func(*corev1.Pod)) {
	p.mu.Lock()
	p.notifyCb = cb
	p.mu.Unlock()
}

// --- NodeProvider ---

func (p *GPUProvider) Ping(ctx context.Context) error {
	_, err := p.cfg.WorkerClient.Discovery().ServerVersion()
	return err
}

func (p *GPUProvider) NotifyNodeStatus(ctx context.Context, cb func(*corev1.Node)) {
}

// --- nodeutil.Provider stubs (not used — no kubelet API server) ---

func (p *GPUProvider) GetContainerLogs(ctx context.Context, namespace, podName, containerName string, opts api.ContainerLogOpts) (io.ReadCloser, error) {
	return nil, fmt.Errorf("not supported")
}

func (p *GPUProvider) RunInContainer(ctx context.Context, namespace, podName, containerName string, cmd []string, attach api.AttachIO) error {
	return fmt.Errorf("not supported")
}

func (p *GPUProvider) AttachToContainer(ctx context.Context, namespace, podName, containerName string, attach api.AttachIO) error {
	return fmt.Errorf("not supported")
}

func (p *GPUProvider) GetStatsSummary(ctx context.Context) (*statsv1alpha1.Summary, error) {
	return nil, fmt.Errorf("not supported")
}

func (p *GPUProvider) GetMetricsResource(ctx context.Context) ([]*dto.MetricFamily, error) {
	return nil, fmt.Errorf("not supported")
}

func (p *GPUProvider) PortForward(ctx context.Context, namespace, pod string, port int32, stream io.ReadWriteCloser) error {
	return fmt.Errorf("not supported")
}

// --- Worker informer (our own, not managed by the VK library) ---

func (p *GPUProvider) startWorkerInformer(ctx context.Context) error {
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
			AddFunc:    func(obj interface{}) { p.handleWorkerPodEvent(obj) },
			UpdateFunc: func(_, obj interface{}) { p.handleWorkerPodEvent(obj) },
		},
	)

	go workerInformer.Run(ctx.Done())
	if !cache.WaitForCacheSync(ctx.Done(), workerInformer.HasSynced) {
		return fmt.Errorf("worker informer cache sync failed")
	}
	klog.Info("Worker pod informer synced, managedPods rebuilt from existing worker pods")
	return nil
}

func (p *GPUProvider) startPVCInformer(ctx context.Context) {
	pvcLW := cache.NewFilteredListWatchFromClient(
		p.cfg.TenantClient.CoreV1().RESTClient(),
		"persistentvolumeclaims",
		metav1.NamespaceAll,
		func(options *metav1.ListOptions) {},
	)
	_, pvcInformer := cache.NewInformer(pvcLW, &corev1.PersistentVolumeClaim{}, 5*time.Minute,
		cache.ResourceEventHandlerFuncs{
			DeleteFunc: func(obj interface{}) {
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
				klog.Infof("Control PVC deleted: %s/%s, cleaning up execution PVC", pvc.Namespace, pvc.Name)
				if err := p.syncer.CleanupExecutionPVC(ctx, pvc.Namespace, pvc.Name); err != nil {
					klog.Errorf("Failed to cleanup execution PVC for %s/%s: %v", pvc.Namespace, pvc.Name, err)
				}
			},
		},
	)
	go pvcInformer.Run(ctx.Done())
	klog.Info("Control PVC deletion informer started")
}

// --- Internal ---

func (p *GPUProvider) handleWorkerPodEvent(obj interface{}) {
	workerPod, ok := obj.(*corev1.Pod)
	if !ok {
		return
	}

	// TODO(upstream): PodController clears knownPods on delete, but if the
	// worker informer fires an update for a terminating pod before it's fully
	// gone, GetPod returns stale data and the library skips CreatePod for the
	// replacement. Propose a DeleteFunc handler or provider lifecycle hook.
	if workerPod.DeletionTimestamp != nil {
		return
	}

	sourceNS := workerPod.Labels[labelSourceNamespace]
	sourceName := workerPod.Labels[labelSourceName]
	if sourceNS == "" || sourceName == "" {
		return
	}

	key := sourceNS + "/" + sourceName

	tenantPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      sourceName,
			Namespace: sourceNS,
		},
		Status: *workerPod.Status.DeepCopy(),
	}

	p.mu.Lock()
	if _, exists := p.managedPods[key]; !exists {
		p.managedPods[key] = workerPod.Name
		klog.Infof("Recovered managed pod mapping: %s -> %s", key, workerPod.Name)
	}
	p.podCache[key] = tenantPod
	cb := p.notifyCb
	p.mu.Unlock()

	if cb != nil {
		cb(tenantPod)
	}
}

func (p *GPUProvider) storeAndNotify(pod *corev1.Pod, phase corev1.PodPhase, reason, message string) {
	failedPod := pod.DeepCopy()
	failedPod.Status.Phase = phase
	failedPod.Status.Reason = reason
	failedPod.Status.Message = message

	key := pod.Namespace + "/" + pod.Name
	p.mu.Lock()
	p.podCache[key] = failedPod
	cb := p.notifyCb
	p.mu.Unlock()

	if cb != nil {
		cb(failedPod)
	}
}

// ConfigureNode sets up the virtual node spec with GPU capacity and VK taints.
func (p *GPUProvider) ConfigureNode(n *corev1.Node) {
	n.Labels["kubernetes.io/os"] = "linux"
	n.Labels["kubernetes.io/arch"] = "amd64"
	n.Labels["node.kubernetes.io/gpu"] = "true"

	n.Spec.Taints = []corev1.Taint{
		{Key: "virtual-kubelet.io/provider", Value: "gpu-provider", Effect: corev1.TaintEffectNoSchedule},
		{Key: "virtual-kubelet.io/provider", Value: "gpu-provider", Effect: corev1.TaintEffectNoExecute},
	}

	gpuQty := resource.MustParse(fmt.Sprintf("%d", p.cfg.GPUCount))
	n.Status = corev1.NodeStatus{
		Phase: corev1.NodeRunning,
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
				Type: corev1.NodeReady, Status: corev1.ConditionTrue,
				LastHeartbeatTime: metav1.Now(), LastTransitionTime: metav1.Now(),
				Reason: "KubeletReady", Message: "vk-gpu-provider is ready",
			},
			{Type: corev1.NodeMemoryPressure, Status: corev1.ConditionFalse, LastHeartbeatTime: metav1.Now(), LastTransitionTime: metav1.Now()},
			{Type: corev1.NodeDiskPressure, Status: corev1.ConditionFalse, LastHeartbeatTime: metav1.Now(), LastTransitionTime: metav1.Now()},
			{Type: corev1.NodePIDPressure, Status: corev1.ConditionFalse, LastHeartbeatTime: metav1.Now(), LastTransitionTime: metav1.Now()},
		},
		NodeInfo: corev1.NodeSystemInfo{
			OperatingSystem: "linux",
			Architecture:    "amd64",
			KubeletVersion:  "v1.31.0-vk",
		},
		Addresses: []corev1.NodeAddress{
			{Type: corev1.NodeInternalIP, Address: "127.0.0.1"},
		},
	}
}

// --- Pod transformation (unchanged) ---

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
