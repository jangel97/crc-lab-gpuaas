package main

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/hex"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"runtime"
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
		taintValue                string
		insecureSkipTLSVerify     bool
		kubeletPort               int
		kubeletCertPath           string
	)

	flag.StringVar(&nodeName, "nodename", "gpu-worker", "Name of the virtual node")
	flag.StringVar(&workerKubeconfig, "worker-kubeconfig", "", "Path to worker cluster kubeconfig")
	flag.StringVar(&workerNamespacePrefix, "worker-namespace-prefix", "", "Prefix for per-tenant worker namespaces (e.g. vk-tenant1-). Auto-generated if empty.")
	flag.IntVar(&gpuCount, "gpu-count", 1, "Number of GPUs to advertise")
	flag.StringVar(&tenantKubeconfig, "kubeconfig", "", "Path to tenant cluster kubeconfig (empty = in-cluster)")
	flag.StringVar(&defaultRemoteStorageClass, "default-remote-storage-class", "lvms-vg1",
		"Default StorageClass for execution PVCs on the GPU cluster")
	flag.StringVar(&taintValue, "taint-value", "catapult",
		"Value for the virtual-kubelet.io/provider taint")
	flag.BoolVar(&insecureSkipTLSVerify, "insecure-skip-tls-verify", false,
		"Skip TLS certificate verification for both tenant and worker API servers")
	flag.IntVar(&kubeletPort, "kubelet-port", 10350,
		"Port for the kubelet API server (logs/exec). Must differ from the real kubelet port 10250")
	flag.StringVar(&kubeletCertPath, "kubelet-cert", "",
		"Path to PEM file with cert+key for the kubelet API TLS (e.g. node's kubelet-server-current.pem)")
	flag.Parse()

	if workerKubeconfig == "" {
		klog.Fatal("--worker-kubeconfig is required")
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	tenantClient, err := buildClient(tenantKubeconfig, insecureSkipTLSVerify)
	if err != nil {
		klog.Fatalf("Failed to create tenant client: %v", err)
	}

	workerClient, err := buildClient(workerKubeconfig, insecureSkipTLSVerify)
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

	nodeIP := os.Getenv("NODE_IP")

	var nodeOpts []nodeutil.NodeOpt

	if kubeletCertPath != "" {
		mux := http.NewServeMux()
		nodeOpts = append(nodeOpts,
			nodeutil.WithNodeConfig(nodeutil.NodeConfig{
				NumWorkers:     runtime.NumCPU(),
				HTTPListenAddr: fmt.Sprintf(":%d", kubeletPort),
				Handler:        mux,
				NodeSpec: corev1.Node{
					ObjectMeta: metav1.ObjectMeta{
						Name: nodeName,
						Labels: map[string]string{
							"type":                   "virtual-kubelet",
							"kubernetes.io/role":     "agent",
							"kubernetes.io/hostname": nodeName,
						},
					},
					Status: corev1.NodeStatus{
						Phase: corev1.NodePending,
						Conditions: []corev1.NodeCondition{
							{Type: corev1.NodeReady},
							{Type: corev1.NodeDiskPressure},
							{Type: corev1.NodeMemoryPressure},
							{Type: corev1.NodePIDPressure},
							{Type: corev1.NodeNetworkUnavailable},
						},
					},
				},
			}),
			nodeutil.WithClient(tenantClient),
			nodeutil.AttachProviderRoutes(mux),
			nodeutil.WithTLSConfig(func(cfg *tls.Config) error {
				cert, err := tls.LoadX509KeyPair(kubeletCertPath, kubeletCertPath)
				if err != nil {
					return fmt.Errorf("load kubelet cert %s: %w", kubeletCertPath, err)
				}
				cfg.Certificates = []tls.Certificate{cert}
				return nil
			}),
		)
		klog.Infof("Kubelet API server will listen on :%d with cert from %s", kubeletPort, kubeletCertPath)
	} else {
		nodeOpts = append(nodeOpts, nodeutil.WithClient(tenantClient))
	}

	vkNode, err := nodeutil.NewNode(nodeName,
		func(cfg nodeutil.ProviderConfig) (nodeutil.Provider, node.NodeProvider, error) {
			provider := NewGPUProvider(GPUProviderConfig{
				NodeName:                  nodeName,
				WorkerNamespacePrefix:     workerNamespacePrefix,
				GPUCount:                  gpuCount,
				DefaultRemoteStorageClass: defaultRemoteStorageClass,
				TaintValue:                taintValue,
				NodeIP:                    nodeIP,
				KubeletPort:               int32(kubeletPort),
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
		nodeOpts...,
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

func buildClient(kubeconfig string, insecure bool) (kubernetes.Interface, error) {
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

	if insecure {
		cfg.TLSClientConfig.Insecure = true
		cfg.TLSClientConfig.CAData = nil
		cfg.TLSClientConfig.CAFile = ""
	}

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
