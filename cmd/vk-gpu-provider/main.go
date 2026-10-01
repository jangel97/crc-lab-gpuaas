package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"syscall"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/klog/v2"
)

func main() {
	klog.InitFlags(nil)

	var (
		nodeName         string
		workerKubeconfig string
		workerNamespace  string
		gpuCount         int
		tenantKubeconfig string
	)

	flag.StringVar(&nodeName, "nodename", "gpu-worker", "Name of the virtual node")
	flag.StringVar(&workerKubeconfig, "worker-kubeconfig", "", "Path to worker cluster kubeconfig")
	flag.StringVar(&workerNamespace, "worker-namespace", "vk-workloads", "Namespace on worker for pods")
	flag.IntVar(&gpuCount, "gpu-count", 1, "Number of GPUs to advertise")
	flag.StringVar(&tenantKubeconfig, "kubeconfig", "", "Path to tenant cluster kubeconfig (empty = in-cluster)")
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

	provider := NewGPUProvider(GPUProviderConfig{
		NodeName:        nodeName,
		WorkerNamespace: workerNamespace,
		GPUCount:        gpuCount,
		TenantClient:    tenantClient,
		WorkerClient:    workerClient,
	})

	if err := provider.Run(ctx); err != nil {
		klog.Fatalf("Provider exited with error: %v", err)
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
