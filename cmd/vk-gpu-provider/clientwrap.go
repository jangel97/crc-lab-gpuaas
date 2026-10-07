package main

// TODO(upstream): This entire file is a workaround for virtual-kubelet v1.11.0
// not supporting status.podIP, status.hostIP, status.podIPs, or status.phase
// in downward API fieldRef env vars (internal/podutils/env.go function
// podFieldSelectorRuntimeValue). Knative's queue-proxy (SERVING_POD_IP,
// HOST_IP) and Istio's sidecar (INSTANCE_IP) both use these fieldRefs.
// The VK library's PopulateEnvironmentVariables runs BEFORE CreatePod and
// fails with "unsupported fieldPath: status.podIP", causing an infinite
// requeue. This wrapper strips unsupported fieldRefs from pods seen by the
// VK library's informer (List/Watch). Our CreatePod re-reads the original
// pod from the API server, so the worker pod retains the original fieldRefs
// and the worker kubelet resolves them normally.
//
// Upstream fix: add cases for status.podIP, status.hostIP, status.podIPs,
// status.phase (and spec.restartPolicy, spec.schedulerName) to
// podFieldSelectorRuntimeValue in internal/podutils/env.go. Once fixed,
// delete this file and remove the wrapper from main.go.

import (
	"context"
	"strings"
	"sync"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	corev1client "k8s.io/client-go/kubernetes/typed/core/v1"
)

func isFieldRefSupported(fieldPath string) bool {
	if strings.HasPrefix(fieldPath, "metadata.annotations[") ||
		strings.HasPrefix(fieldPath, "metadata.labels[") {
		return true
	}
	switch fieldPath {
	case "spec.nodeName", "spec.serviceAccountName",
		"metadata.name", "metadata.namespace", "metadata.uid",
		"metadata.annotations", "metadata.labels":
		return true
	}
	return false
}

func stripUnsupportedFieldRefs(pod *corev1.Pod) {
	stripContainerFieldRefs(pod.Spec.InitContainers)
	stripContainerFieldRefs(pod.Spec.Containers)
}

func stripContainerFieldRefs(containers []corev1.Container) {
	for i := range containers {
		for j := range containers[i].Env {
			env := &containers[i].Env[j]
			if env.ValueFrom != nil && env.ValueFrom.FieldRef != nil {
				if !isFieldRefSupported(env.ValueFrom.FieldRef.FieldPath) {
					env.Value = ""
					env.ValueFrom = nil
				}
			}
		}
	}
}

// fieldRefSafeClient wraps kubernetes.Interface to strip unsupported fieldRef
// env vars from pods returned by List and Watch. Only the pod informer is
// affected; all other API calls pass through unchanged.
type fieldRefSafeClient struct {
	kubernetes.Interface
}

func (c *fieldRefSafeClient) CoreV1() corev1client.CoreV1Interface {
	return &fieldRefSafeCoreV1{c.Interface.CoreV1()}
}

type fieldRefSafeCoreV1 struct {
	corev1client.CoreV1Interface
}

func (c *fieldRefSafeCoreV1) Pods(namespace string) corev1client.PodInterface {
	return &fieldRefSafePods{c.CoreV1Interface.Pods(namespace)}
}

type fieldRefSafePods struct {
	corev1client.PodInterface
}

func (p *fieldRefSafePods) List(ctx context.Context, opts metav1.ListOptions) (*corev1.PodList, error) {
	result, err := p.PodInterface.List(ctx, opts)
	if err != nil {
		return nil, err
	}
	for i := range result.Items {
		stripUnsupportedFieldRefs(&result.Items[i])
	}
	return result, nil
}

func (p *fieldRefSafePods) Watch(ctx context.Context, opts metav1.ListOptions) (watch.Interface, error) {
	w, err := p.PodInterface.Watch(ctx, opts)
	if err != nil {
		return nil, err
	}
	return newFieldRefSafeWatch(w), nil
}

type fieldRefSafeWatch struct {
	inner watch.Interface
	ch    chan watch.Event
	stop  sync.Once
}

func newFieldRefSafeWatch(inner watch.Interface) *fieldRefSafeWatch {
	w := &fieldRefSafeWatch{
		inner: inner,
		ch:    make(chan watch.Event),
	}
	go w.proxy()
	return w
}

func (w *fieldRefSafeWatch) proxy() {
	defer close(w.ch)
	for event := range w.inner.ResultChan() {
		if pod, ok := event.Object.(*corev1.Pod); ok {
			pod = pod.DeepCopy()
			stripUnsupportedFieldRefs(pod)
			event.Object = pod
		}
		w.ch <- event
	}
}

func (w *fieldRefSafeWatch) ResultChan() <-chan watch.Event {
	return w.ch
}

func (w *fieldRefSafeWatch) Stop() {
	w.stop.Do(func() {
		w.inner.Stop()
	})
}
