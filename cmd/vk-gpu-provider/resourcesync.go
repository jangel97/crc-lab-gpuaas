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

type ResourceSyncer struct {
	tenantClient    kubernetes.Interface
	workerClient    kubernetes.Interface
	workerNamespace string
}

func NewResourceSyncer(tenant, worker kubernetes.Interface, workerNS string) *ResourceSyncer {
	return &ResourceSyncer{
		tenantClient:    tenant,
		workerClient:    worker,
		workerNamespace: workerNS,
	}
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

func discoverReferences(pod *corev1.Pod) (secrets []string, configmaps []string) {
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

	workerSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: s.workerNamespace,
			Labels: map[string]string{
				labelManagedBy:       labelManagedByValue,
				labelSourceNamespace: sourceNS,
				labelSourcePod:       podName,
			},
		},
		Type: secret.Type,
		Data: secret.Data,
	}

	existing, err := s.workerClient.CoreV1().Secrets(s.workerNamespace).Get(ctx, name, metav1.GetOptions{})
	if err == nil {
		existing.Data = workerSecret.Data
		existing.Labels = workerSecret.Labels
		existing.Type = workerSecret.Type
		_, err = s.workerClient.CoreV1().Secrets(s.workerNamespace).Update(ctx, existing, metav1.UpdateOptions{})
	} else if errors.IsNotFound(err) {
		_, err = s.workerClient.CoreV1().Secrets(s.workerNamespace).Create(ctx, workerSecret, metav1.CreateOptions{})
	}

	if err != nil {
		return err
	}
	klog.Infof("Synced secret %s -> %s/%s", name, s.workerNamespace, name)
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

	workerCM := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: s.workerNamespace,
			Labels: map[string]string{
				labelManagedBy:       labelManagedByValue,
				labelSourceNamespace: sourceNS,
				labelSourcePod:       podName,
			},
		},
		Data:       cm.Data,
		BinaryData: cm.BinaryData,
	}

	existing, err := s.workerClient.CoreV1().ConfigMaps(s.workerNamespace).Get(ctx, name, metav1.GetOptions{})
	if err == nil {
		existing.Data = workerCM.Data
		existing.BinaryData = workerCM.BinaryData
		existing.Labels = workerCM.Labels
		_, err = s.workerClient.CoreV1().ConfigMaps(s.workerNamespace).Update(ctx, existing, metav1.UpdateOptions{})
	} else if errors.IsNotFound(err) {
		_, err = s.workerClient.CoreV1().ConfigMaps(s.workerNamespace).Create(ctx, workerCM, metav1.CreateOptions{})
	}

	if err != nil {
		return err
	}
	klog.Infof("Synced configmap %s -> %s/%s", name, s.workerNamespace, name)
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

	workerSA := &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: s.workerNamespace,
			Labels: map[string]string{
				labelManagedBy:       labelManagedByValue,
				labelSourceNamespace: sourceNS,
				labelSourcePod:       podName,
			},
		},
		ImagePullSecrets: sa.ImagePullSecrets,
	}

	existing, err := s.workerClient.CoreV1().ServiceAccounts(s.workerNamespace).Get(ctx, name, metav1.GetOptions{})
	if err == nil {
		existing.Labels = workerSA.Labels
		existing.ImagePullSecrets = workerSA.ImagePullSecrets
		_, err = s.workerClient.CoreV1().ServiceAccounts(s.workerNamespace).Update(ctx, existing, metav1.UpdateOptions{})
	} else if errors.IsNotFound(err) {
		_, err = s.workerClient.CoreV1().ServiceAccounts(s.workerNamespace).Create(ctx, workerSA, metav1.CreateOptions{})
	}

	if err != nil {
		return err
	}
	klog.Infof("Synced serviceaccount %s -> %s/%s", name, s.workerNamespace, name)
	return nil
}

func (s *ResourceSyncer) CleanupResources(ctx context.Context, sourceNS, podName string) error {
	labelSelector := fmt.Sprintf("%s=%s,%s=%s,%s=%s",
		labelManagedBy, labelManagedByValue,
		labelSourceNamespace, sourceNS,
		labelSourcePod, podName,
	)

	secrets, err := s.workerClient.CoreV1().Secrets(s.workerNamespace).List(ctx, metav1.ListOptions{
		LabelSelector: labelSelector,
	})
	if err == nil {
		for _, secret := range secrets.Items {
			if err := s.workerClient.CoreV1().Secrets(s.workerNamespace).Delete(ctx, secret.Name, metav1.DeleteOptions{}); err != nil && !errors.IsNotFound(err) {
				klog.Errorf("Failed to delete synced secret %s: %v", secret.Name, err)
			}
		}
	}

	configmaps, err := s.workerClient.CoreV1().ConfigMaps(s.workerNamespace).List(ctx, metav1.ListOptions{
		LabelSelector: labelSelector,
	})
	if err == nil {
		for _, cm := range configmaps.Items {
			if err := s.workerClient.CoreV1().ConfigMaps(s.workerNamespace).Delete(ctx, cm.Name, metav1.DeleteOptions{}); err != nil && !errors.IsNotFound(err) {
				klog.Errorf("Failed to delete synced configmap %s: %v", cm.Name, err)
			}
		}
	}

	sas, err := s.workerClient.CoreV1().ServiceAccounts(s.workerNamespace).List(ctx, metav1.ListOptions{
		LabelSelector: labelSelector,
	})
	if err == nil {
		for _, sa := range sas.Items {
			if err := s.workerClient.CoreV1().ServiceAccounts(s.workerNamespace).Delete(ctx, sa.Name, metav1.DeleteOptions{}); err != nil && !errors.IsNotFound(err) {
				klog.Errorf("Failed to delete synced serviceaccount %s: %v", sa.Name, err)
			}
		}
	}

	klog.Infof("Cleaned up synced resources for %s/%s", sourceNS, podName)
	return nil
}
