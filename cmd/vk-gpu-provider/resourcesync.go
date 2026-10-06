// TODO: extract resource-sync logic into a standalone library (e.g. pkg/resourcesync)
// so it can be reused by other consumers (kueue-populator, AdmissionCheck controllers,
// syncer-service, etc.). The API surface: Syncer struct with SyncForPod, Cleanup, and
// PVC catapult handling. No virtual-kubelet or Kueue dependency.
// Ref: https://github.com/kubernetes-sigs/kueue/issues/16504#issuecomment-6019740204
package main

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
)

const (
	catapultStorageClass       = "catapult"
	catapultRemoteSCAnnotation = "catapult.redhat.com/remote-storage-class"
	labelSourcePVC             = "vk.gpuaas.io/source-pvc"
)

type ResourceSyncer struct {
	tenantClient              kubernetes.Interface
	workerClient              kubernetes.Interface
	workerNamespacePrefix     string
	defaultRemoteStorageClass string
}

func NewResourceSyncer(tenant, worker kubernetes.Interface, workerNSPrefix, defaultRemoteSC string) *ResourceSyncer {
	return &ResourceSyncer{
		tenantClient:              tenant,
		workerClient:              worker,
		workerNamespacePrefix:     workerNSPrefix,
		defaultRemoteStorageClass: defaultRemoteSC,
	}
}

func (s *ResourceSyncer) workerNamespace(tenantNS string) string {
	return s.workerNamespacePrefix + tenantNS
}

func executionPVCName(sourceNS, pvcName string) string {
	return fmt.Sprintf("%s--%s", sourceNS, pvcName)
}

func (s *ResourceSyncer) SyncResources(ctx context.Context, pod *corev1.Pod) error {
	secrets, configmaps := discoverReferences(pod)

	for _, name := range secrets {
		if err := s.syncSecret(ctx, pod.Namespace, pod.Name, name); err != nil {
			return fmt.Errorf("sync secret %q: %w", name, err)
		}
	}

	for _, name := range configmaps {
		if err := s.syncConfigMap(ctx, pod.Namespace, pod.Name, name); err != nil {
			return fmt.Errorf("sync configmap %q: %w", name, err)
		}
	}

	if pod.Spec.ServiceAccountName != "" && pod.Spec.ServiceAccountName != "default" {
		if err := s.syncServiceAccount(ctx, pod.Namespace, pod.Name, pod.Spec.ServiceAccountName); err != nil {
			return fmt.Errorf("sync serviceaccount %q: %w", pod.Spec.ServiceAccountName, err)
		}
	}

	return nil
}

func discoverReferences(pod *corev1.Pod) (secrets, configmaps []string) {
	secretSet := make(map[string]bool)
	cmSet := make(map[string]bool)

	allContainers := append(pod.Spec.Containers, pod.Spec.InitContainers...)
	for _, c := range allContainers {
		for _, env := range c.Env {
			if env.ValueFrom != nil {
				if env.ValueFrom.SecretKeyRef != nil {
					secretSet[env.ValueFrom.SecretKeyRef.Name] = true
				}
				if env.ValueFrom.ConfigMapKeyRef != nil {
					cmSet[env.ValueFrom.ConfigMapKeyRef.Name] = true
				}
			}
		}
		for _, envFrom := range c.EnvFrom {
			if envFrom.SecretRef != nil {
				secretSet[envFrom.SecretRef.Name] = true
			}
			if envFrom.ConfigMapRef != nil {
				cmSet[envFrom.ConfigMapRef.Name] = true
			}
		}
	}

	for _, v := range pod.Spec.Volumes {
		if v.Secret != nil {
			secretSet[v.Secret.SecretName] = true
		}
		if v.ConfigMap != nil {
			cmSet[v.ConfigMap.Name] = true
		}
	}

	for _, ips := range pod.Spec.ImagePullSecrets {
		secretSet[ips.Name] = true
	}

	for name := range secretSet {
		secrets = append(secrets, name)
	}
	for name := range cmSet {
		configmaps = append(configmaps, name)
	}
	return
}

func (s *ResourceSyncer) syncSecret(ctx context.Context, sourceNS, podName, name string) error {
	secret, err := s.tenantClient.CoreV1().Secrets(sourceNS).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		if errors.IsNotFound(err) {
			klog.Warningf("Secret %s/%s not found on tenant, skipping", sourceNS, name)
			return nil
		}
		return err
	}

	if secret.Type == corev1.SecretTypeServiceAccountToken {
		klog.Infof("Skipping SA token secret %s/%s", sourceNS, name)
		return nil
	}

	wns := s.workerNamespace(sourceNS)
	workerSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: wns,
			Labels: map[string]string{
				labelManagedBy:       labelManagedByValue,
				labelSourceNamespace: sourceNS,
				labelSourcePod:       podName,
			},
		},
		Type: secret.Type,
		Data: secret.Data,
	}

	existing, err := s.workerClient.CoreV1().Secrets(wns).Get(ctx, name, metav1.GetOptions{})
	if err == nil {
		existing.Data = workerSecret.Data
		existing.Labels = workerSecret.Labels
		existing.Type = workerSecret.Type
		_, err = s.workerClient.CoreV1().Secrets(wns).Update(ctx, existing, metav1.UpdateOptions{})
	} else if errors.IsNotFound(err) {
		_, err = s.workerClient.CoreV1().Secrets(wns).Create(ctx, workerSecret, metav1.CreateOptions{})
	}

	if err != nil {
		return err
	}
	klog.Infof("Synced secret %s -> %s/%s", name, wns, name)
	return nil
}

func (s *ResourceSyncer) syncConfigMap(ctx context.Context, sourceNS, podName, name string) error {
	cm, err := s.tenantClient.CoreV1().ConfigMaps(sourceNS).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		if errors.IsNotFound(err) {
			klog.Warningf("ConfigMap %s/%s not found on tenant, skipping", sourceNS, name)
			return nil
		}
		return err
	}

	wns := s.workerNamespace(sourceNS)
	workerCM := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: wns,
			Labels: map[string]string{
				labelManagedBy:       labelManagedByValue,
				labelSourceNamespace: sourceNS,
				labelSourcePod:       podName,
			},
		},
		Data:       cm.Data,
		BinaryData: cm.BinaryData,
	}

	existing, err := s.workerClient.CoreV1().ConfigMaps(wns).Get(ctx, name, metav1.GetOptions{})
	if err == nil {
		existing.Data = workerCM.Data
		existing.BinaryData = workerCM.BinaryData
		existing.Labels = workerCM.Labels
		_, err = s.workerClient.CoreV1().ConfigMaps(wns).Update(ctx, existing, metav1.UpdateOptions{})
	} else if errors.IsNotFound(err) {
		_, err = s.workerClient.CoreV1().ConfigMaps(wns).Create(ctx, workerCM, metav1.CreateOptions{})
	}

	if err != nil {
		return err
	}
	klog.Infof("Synced configmap %s -> %s/%s", name, wns, name)
	return nil
}

func (s *ResourceSyncer) syncServiceAccount(ctx context.Context, sourceNS, podName, name string) error {
	sa, err := s.tenantClient.CoreV1().ServiceAccounts(sourceNS).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		if errors.IsNotFound(err) {
			klog.Warningf("ServiceAccount %s/%s not found on tenant, skipping", sourceNS, name)
			return nil
		}
		return err
	}

	for _, ips := range sa.ImagePullSecrets {
		if err := s.syncSecret(ctx, sourceNS, podName, ips.Name); err != nil {
			klog.Warningf("Failed to sync SA image pull secret %s: %v", ips.Name, err)
		}
	}

	wns := s.workerNamespace(sourceNS)
	workerSA := &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: wns,
			Labels: map[string]string{
				labelManagedBy:       labelManagedByValue,
				labelSourceNamespace: sourceNS,
				labelSourcePod:       podName,
			},
		},
		ImagePullSecrets: sa.ImagePullSecrets,
	}

	existing, err := s.workerClient.CoreV1().ServiceAccounts(wns).Get(ctx, name, metav1.GetOptions{})
	if err == nil {
		existing.Labels = workerSA.Labels
		existing.ImagePullSecrets = workerSA.ImagePullSecrets
		_, err = s.workerClient.CoreV1().ServiceAccounts(wns).Update(ctx, existing, metav1.UpdateOptions{})
	} else if errors.IsNotFound(err) {
		_, err = s.workerClient.CoreV1().ServiceAccounts(wns).Create(ctx, workerSA, metav1.CreateOptions{})
	}

	if err != nil {
		return err
	}
	klog.Infof("Synced serviceaccount %s -> %s/%s", name, wns, name)
	return nil
}

func (s *ResourceSyncer) SyncHeadlessServices(ctx context.Context, pod *corev1.Pod) error {
	services, err := s.tenantClient.CoreV1().Services(pod.Namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("list services in %s: %w", pod.Namespace, err)
	}

	for i := range services.Items {
		svc := &services.Items[i]
		if svc.Spec.ClusterIP != "None" {
			continue
		}
		if !selectorMatchesPod(svc, pod) {
			continue
		}
		if err := s.syncService(ctx, pod.Namespace, pod.Name, svc); err != nil {
			return fmt.Errorf("sync headless service %q: %w", svc.Name, err)
		}
	}
	return nil
}

func selectorMatchesPod(svc *corev1.Service, pod *corev1.Pod) bool {
	if len(svc.Spec.Selector) == 0 {
		return false
	}
	for k, v := range svc.Spec.Selector {
		if pod.Labels[k] != v {
			return false
		}
	}
	return true
}

func (s *ResourceSyncer) syncService(ctx context.Context, sourceNS, podName string, svc *corev1.Service) error {
	wns := s.workerNamespace(sourceNS)
	workerSvc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      svc.Name,
			Namespace: wns,
			Labels: map[string]string{
				labelManagedBy:       labelManagedByValue,
				labelSourceNamespace: sourceNS,
				labelSourcePod:       podName,
			},
		},
		Spec: corev1.ServiceSpec{
			ClusterIP: "None",
			Selector:  svc.Spec.Selector,
			Ports:     svc.Spec.Ports,
		},
	}

	existing, err := s.workerClient.CoreV1().Services(wns).Get(ctx, svc.Name, metav1.GetOptions{})
	if err == nil {
		existing.Spec.Selector = workerSvc.Spec.Selector
		existing.Spec.Ports = workerSvc.Spec.Ports
		existing.Labels = workerSvc.Labels
		_, err = s.workerClient.CoreV1().Services(wns).Update(ctx, existing, metav1.UpdateOptions{})
	} else if errors.IsNotFound(err) {
		_, err = s.workerClient.CoreV1().Services(wns).Create(ctx, workerSvc, metav1.CreateOptions{})
	}

	if err != nil {
		return err
	}
	klog.Infof("Synced headless service %s -> %s/%s", svc.Name, wns, svc.Name)
	return nil
}

func (s *ResourceSyncer) ValidateAndSyncPVCs(ctx context.Context, pod *corev1.Pod) (map[string]string, error) {
	pvcMap := make(map[string]string)

	for _, v := range pod.Spec.Volumes {
		if v.PersistentVolumeClaim == nil {
			continue
		}
		claimName := v.PersistentVolumeClaim.ClaimName

		pvc, err := s.tenantClient.CoreV1().PersistentVolumeClaims(pod.Namespace).Get(ctx, claimName, metav1.GetOptions{})
		if err != nil {
			return nil, fmt.Errorf("get PVC %s/%s: %w", pod.Namespace, claimName, err)
		}

		scName := ""
		if pvc.Spec.StorageClassName != nil {
			scName = *pvc.Spec.StorageClassName
		}
		if scName != catapultStorageClass {
			return nil, fmt.Errorf(
				"PVC %s/%s uses storageClass %q, not %q — Catapult only syncs catapult-class PVCs",
				pod.Namespace, claimName, scName, catapultStorageClass,
			)
		}

		remoteSC := s.defaultRemoteStorageClass
		if ann := pvc.Annotations[catapultRemoteSCAnnotation]; ann != "" {
			remoteSC = ann
		}

		if err := s.syncCatapultPVC(ctx, pod.Namespace, claimName, pvc, remoteSC); err != nil {
			return nil, fmt.Errorf("sync catapult PVC %s/%s: %w", pod.Namespace, claimName, err)
		}

		pvcMap[claimName] = executionPVCName(pod.Namespace, claimName)
	}

	return pvcMap, nil
}

func (s *ResourceSyncer) syncCatapultPVC(ctx context.Context, sourceNS, pvcName string, sourcePVC *corev1.PersistentVolumeClaim, remoteStorageClass string) error {
	execName := executionPVCName(sourceNS, pvcName)
	wns := s.workerNamespace(sourceNS)

	_, err := s.workerClient.CoreV1().PersistentVolumeClaims(wns).Get(ctx, execName, metav1.GetOptions{})
	if err == nil {
		klog.Infof("Execution PVC %s already exists in %s, skipping", execName, wns)
		return nil
	}
	if !errors.IsNotFound(err) {
		return err
	}

	workerPVC := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      execName,
			Namespace: wns,
			Labels: map[string]string{
				labelManagedBy:       labelManagedByValue,
				labelSourceNamespace: sourceNS,
				labelSourcePVC:       pvcName,
			},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes:      sourcePVC.Spec.AccessModes,
			Resources:        sourcePVC.Spec.Resources,
			StorageClassName: &remoteStorageClass,
		},
	}

	_, err = s.workerClient.CoreV1().PersistentVolumeClaims(wns).Create(ctx, workerPVC, metav1.CreateOptions{})
	if err != nil {
		return err
	}
	klog.Infof("Created execution PVC %s/%s (storageClass=%s) for control PVC %s/%s",
		wns, execName, remoteStorageClass, sourceNS, pvcName)
	return nil
}

func (s *ResourceSyncer) CleanupExecutionPVC(ctx context.Context, sourceNS, pvcName string) error {
	execName := executionPVCName(sourceNS, pvcName)
	wns := s.workerNamespace(sourceNS)
	err := s.workerClient.CoreV1().PersistentVolumeClaims(wns).Delete(ctx, execName, metav1.DeleteOptions{})
	if err != nil && !errors.IsNotFound(err) {
		return fmt.Errorf("delete execution PVC %s/%s: %w", wns, execName, err)
	}
	if err == nil {
		klog.Infof("Deleted execution PVC %s/%s (control PVC %s/%s deleted)", wns, execName, sourceNS, pvcName)
	}
	return nil
}

func (s *ResourceSyncer) CleanupResources(ctx context.Context, sourceNS, podName string) error {
	labelSelector := fmt.Sprintf("%s=%s,%s=%s,%s=%s",
		labelManagedBy, labelManagedByValue,
		labelSourceNamespace, sourceNS,
		labelSourcePod, podName,
	)

	wns := s.workerNamespace(sourceNS)

	secrets, err := s.workerClient.CoreV1().Secrets(wns).List(ctx, metav1.ListOptions{
		LabelSelector: labelSelector,
	})
	if err == nil {
		for _, secret := range secrets.Items {
			if err := s.workerClient.CoreV1().Secrets(wns).Delete(ctx, secret.Name, metav1.DeleteOptions{}); err != nil && !errors.IsNotFound(err) {
				klog.Errorf("Failed to delete synced secret %s: %v", secret.Name, err)
			}
		}
	}

	configmaps, err := s.workerClient.CoreV1().ConfigMaps(wns).List(ctx, metav1.ListOptions{
		LabelSelector: labelSelector,
	})
	if err == nil {
		for _, cm := range configmaps.Items {
			if err := s.workerClient.CoreV1().ConfigMaps(wns).Delete(ctx, cm.Name, metav1.DeleteOptions{}); err != nil && !errors.IsNotFound(err) {
				klog.Errorf("Failed to delete synced configmap %s: %v", cm.Name, err)
			}
		}
	}

	sas, err := s.workerClient.CoreV1().ServiceAccounts(wns).List(ctx, metav1.ListOptions{
		LabelSelector: labelSelector,
	})
	if err == nil {
		for _, sa := range sas.Items {
			if err := s.workerClient.CoreV1().ServiceAccounts(wns).Delete(ctx, sa.Name, metav1.DeleteOptions{}); err != nil && !errors.IsNotFound(err) {
				klog.Errorf("Failed to delete synced serviceaccount %s: %v", sa.Name, err)
			}
		}
	}

	services, err := s.workerClient.CoreV1().Services(wns).List(ctx, metav1.ListOptions{
		LabelSelector: labelSelector,
	})
	if err == nil {
		for _, svc := range services.Items {
			if err := s.workerClient.CoreV1().Services(wns).Delete(ctx, svc.Name, metav1.DeleteOptions{}); err != nil && !errors.IsNotFound(err) {
				klog.Errorf("Failed to delete synced service %s: %v", svc.Name, err)
			}
		}
	}

	klog.Infof("Cleaned up synced resources for %s/%s", sourceNS, podName)
	return nil
}
