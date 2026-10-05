package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/klog/v2"

	"github.com/virtual-kubelet/virtual-kubelet/node"
	"github.com/virtual-kubelet/virtual-kubelet/node/nodeutil"
)

func main() {
	klog.InitFlags(nil)

	var (
		nodeName                  string
		workerKubeconfig          string
		workerNamespacePrefix     string
		gpuCount                  int
		tenantKubeconfig          string
		defaultRemoteStorageClass string
	)

	flag.StringVar(&nodeName, "nodename", "gpu-worker", "Name of the virtual node")
	flag.StringVar(&workerKubeconfig, "worker-kubeconfig", "", "Path to worker cluster kubeconfig")
	flag.StringVar(&workerNamespacePrefix, "worker-namespace-prefix", "", "Prefix for per-tenant worker namespaces (e.g. vk-tenant1-). Auto-generated if empty.")
	flag.IntVar(&gpuCount, "gpu-count", 1, "Number of GPUs to advertise")
	flag.StringVar(&tenantKubeconfig, "kubeconfig", "", "Path to tenant cluster kubeconfig (empty = in-cluster)")
	flag.StringVar(&defaultRemoteStorageClass, "default-remote-storage-class", "lvms-vg1",
		"Default StorageClass for execution PVCs on the GPU cluster")
	flag.Parse()

	if workerKubeconfig == "" {
		klog.Fatal("--worker-kubeconfig is required")
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	tenantClient, err := buildClient(tenantKubeconfig)
	if err != nil {
		klog.Fatalf("Failed to create tenant client: %v", err)
	}

	workerClient, err := buildClient(workerKubeconfig)
	if err != nil {
		klog.Fatalf("Failed to create worker client: %v", err)
	}

	if workerNamespacePrefix == "" {
		resolved, err := resolveNamespacePrefix(ctx, tenantClient)
		if err != nil {
			klog.Fatalf("Failed to resolve worker namespace prefix: %v", err)
		}
		workerNamespacePrefix = resolved
	}
	klog.Infof("Worker namespace prefix: %s", workerNamespacePrefix)

	vkNode, err := nodeutil.NewNode(nodeName,
		func(cfg nodeutil.ProviderConfig) (nodeutil.Provider, node.NodeProvider, error) {
			provider := NewGPUProvider(GPUProviderConfig{
				NodeName:                  nodeName,
				WorkerNamespacePrefix:     workerNamespacePrefix,
				GPUCount:                  gpuCount,
				DefaultRemoteStorageClass: defaultRemoteStorageClass,
				TenantClient:              tenantClient,
				WorkerClient:              workerClient,
			})
			provider.ConfigureNode(cfg.Node)

			if err := provider.startWorkerInformer(ctx); err != nil {
				return nil, nil, fmt.Errorf("start worker informer: %w", err)
			}
			provider.startPVCInformer(ctx)

			return provider, provider, nil
		},
		nodeutil.WithClient(tenantClient),
	)
	if err != nil {
		klog.Fatalf("Failed to create virtual kubelet node: %v", err)
	}

	go func() {
		if err := vkNode.Run(ctx); err != nil {
			klog.Fatalf("Virtual kubelet node exited with error: %v", err)
		}
	}()

	klog.Info("Virtual kubelet started")
	<-vkNode.Done()
	if err := vkNode.Err(); err != nil {
		klog.Fatalf("Virtual kubelet error: %v", err)
	}
}

func buildClient(kubeconfig string) (kubernetes.Interface, error) {
	var cfg *rest.Config
	var err error

	if kubeconfig != "" {
		cfg, err = clientcmd.BuildConfigFromFlags("", kubeconfig)
	} else {
		cfg, err = rest.InClusterConfig()
		if err != nil {
			home, _ := os.UserHomeDir()
			cfg, err = clientcmd.BuildConfigFromFlags("", home+"/.kube/config")
		}
	}
	if err != nil {
		return nil, err
	}

	cfg.TLSClientConfig.Insecure = true
	cfg.TLSClientConfig.CAData = nil
	cfg.TLSClientConfig.CAFile = ""

	return kubernetes.NewForConfig(cfg)
}

const prefixConfigMapName = "vk-gpu-provider-config"
const prefixConfigMapNS = "kube-system"

func resolveNamespacePrefix(ctx context.Context, client kubernetes.Interface) (string, error) {
	cm, err := client.CoreV1().ConfigMaps(prefixConfigMapNS).Get(ctx, prefixConfigMapName, metav1.GetOptions{})
	if err == nil {
		if p := cm.Data["worker-namespace-prefix"]; p != "" {
			return p, nil
		}
	} else if !errors.IsNotFound(err) {
		return "", fmt.Errorf("get configmap %s/%s: %w", prefixConfigMapNS, prefixConfigMapName, err)
	}

	existingCM := cm
	if errors.IsNotFound(err) {
		existingCM = nil
	}

	b := make([]byte, 4)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("generate random prefix: %w", err)
	}
	prefix := "vk-" + hex.EncodeToString(b) + "-"

	if existingCM != nil {
		existingCM.Data = map[string]string{"worker-namespace-prefix": prefix}
		_, err = client.CoreV1().ConfigMaps(prefixConfigMapNS).Update(ctx, existingCM, metav1.UpdateOptions{})
	} else {
		cmObj := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      prefixConfigMapName,
				Namespace: prefixConfigMapNS,
			},
			Data: map[string]string{"worker-namespace-prefix": prefix},
		}
		_, err = client.CoreV1().ConfigMaps(prefixConfigMapNS).Create(ctx, cmObj, metav1.CreateOptions{})
	}
	if err != nil {
		return "", fmt.Errorf("persist prefix configmap: %w", err)
	}

	klog.Infof("Generated and persisted worker namespace prefix: %s", prefix)
	return prefix, nil
}
