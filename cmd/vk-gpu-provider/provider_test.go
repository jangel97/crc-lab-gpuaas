package main

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
)

func basePod() *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pod",
			Namespace: "team-a",
			Labels:    map[string]string{"app": "train"},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{
				Name:  "main",
				Image: "nvidia/cuda:12.0-base",
			}},
			RestartPolicy: corev1.RestartPolicyNever,
		},
	}
}

func transform(pod *corev1.Pod) *corev1.Pod {
	p := &GPUProvider{cfg: GPUProviderConfig{WorkerNamespacePrefix: "vk-test-"}}
	return p.transformPod(pod, "vk-test-team-a", nil)
}

// --- Category 1: Definitely user-set (SCC only validates, never mutates) ---

func TestTransform_Privileged_Preserved(t *testing.T) {
	pod := basePod()
	pod.Spec.Containers[0].SecurityContext = &corev1.SecurityContext{
		Privileged: ptr.To(true),
	}

	w := transform(pod)

	sc := w.Spec.Containers[0].SecurityContext
	if sc == nil || sc.Privileged == nil || !*sc.Privileged {
		t.Fatal("privileged=true must be preserved (SCC never mutates this field)")
	}
}

func TestTransform_RunAsGroup_Preserved(t *testing.T) {
	pod := basePod()
	pod.Spec.SecurityContext = &corev1.PodSecurityContext{
		RunAsGroup: ptr.To(int64(1000)),
	}

	w := transform(pod)

	if w.Spec.SecurityContext == nil || w.Spec.SecurityContext.RunAsGroup == nil {
		t.Fatal("runAsGroup must be preserved (not SCC-managed)")
	}
	if *w.Spec.SecurityContext.RunAsGroup != 1000 {
		t.Fatalf("expected runAsGroup=1000, got %d", *w.Spec.SecurityContext.RunAsGroup)
	}
}

// --- Category 2: Previously SCC-stripped, now preserved (pass-through) ---

func TestTransform_SELinux_Preserved(t *testing.T) {
	pod := basePod()
	pod.Spec.SecurityContext = &corev1.PodSecurityContext{
		SELinuxOptions: &corev1.SELinuxOptions{
			Level: "s0:c26,c10",
		},
	}
	pod.Spec.Containers[0].SecurityContext = &corev1.SecurityContext{
		SELinuxOptions: &corev1.SELinuxOptions{
			Level: "s0:c26,c10",
		},
	}

	w := transform(pod)

	if w.Spec.SecurityContext.SELinuxOptions == nil || w.Spec.SecurityContext.SELinuxOptions.Level != "s0:c26,c10" {
		t.Fatal("pod-level seLinuxOptions must be preserved (pass-through)")
	}
	if w.Spec.Containers[0].SecurityContext.SELinuxOptions == nil || w.Spec.Containers[0].SecurityContext.SELinuxOptions.Level != "s0:c26,c10" {
		t.Fatal("container-level seLinuxOptions must be preserved (pass-through)")
	}
}

func TestTransform_SupplementalGroups_Preserved(t *testing.T) {
	pod := basePod()
	pod.Spec.SecurityContext = &corev1.PodSecurityContext{
		SupplementalGroups: []int64{1000620000},
	}

	w := transform(pod)

	if len(w.Spec.SecurityContext.SupplementalGroups) != 1 || w.Spec.SecurityContext.SupplementalGroups[0] != 1000620000 {
		t.Fatal("supplementalGroups must be preserved (pass-through)")
	}
}

// --- Category 3: Ambiguous fields (preserved — may be user or SCC) ---

func TestTransform_ExplicitRunAsUser_Preserved(t *testing.T) {
	pod := basePod()
	pod.Spec.Containers[0].SecurityContext = &corev1.SecurityContext{
		RunAsUser: ptr.To(int64(1001)),
	}

	w := transform(pod)

	sc := w.Spec.Containers[0].SecurityContext
	if sc == nil || sc.RunAsUser == nil || *sc.RunAsUser != 1001 {
		t.Fatal("explicit runAsUser=1001 must be preserved")
	}
}

func TestTransform_SCCGeneratedRunAsUser_AlsoPreserved(t *testing.T) {
	// This is the ambiguity: SCC set runAsUser=1000620000 (namespace range min),
	// but we can't distinguish it from a user who explicitly set the same value.
	// Catapult preserves it — worker SCC validates and rejects if incompatible.
	pod := basePod()
	pod.Spec.Containers[0].SecurityContext = &corev1.SecurityContext{
		RunAsUser: ptr.To(int64(1000620000)),
	}

	w := transform(pod)

	sc := w.Spec.Containers[0].SecurityContext
	if sc == nil || sc.RunAsUser == nil || *sc.RunAsUser != 1000620000 {
		t.Fatal("runAsUser=1000620000 preserved (ambiguous: could be user-set or SCC-generated)")
	}
}

func TestTransform_FSGroup_Preserved(t *testing.T) {
	// Ambiguous: SCC generates fsGroup from range.Min if nil, but users
	// set fsGroup for shared volume permissions. We preserve.
	pod := basePod()
	pod.Spec.SecurityContext = &corev1.PodSecurityContext{
		FSGroup: ptr.To(int64(1000620000)),
	}

	w := transform(pod)

	if w.Spec.SecurityContext == nil || w.Spec.SecurityContext.FSGroup == nil {
		t.Fatal("fsGroup must be preserved (ambiguous: may be user intent for volume permissions)")
	}
	if *w.Spec.SecurityContext.FSGroup != 1000620000 {
		t.Fatalf("expected fsGroup=1000620000, got %d", *w.Spec.SecurityContext.FSGroup)
	}
}

func TestTransform_AllowPrivilegeEscalation_Preserved(t *testing.T) {
	pod := basePod()
	pod.Spec.Containers[0].SecurityContext = &corev1.SecurityContext{
		AllowPrivilegeEscalation: ptr.To(false),
	}

	w := transform(pod)

	sc := w.Spec.Containers[0].SecurityContext
	if sc == nil || sc.AllowPrivilegeEscalation == nil || *sc.AllowPrivilegeEscalation {
		t.Fatal("allowPrivilegeEscalation=false must be preserved")
	}
}

func TestTransform_SeccompProfile_Preserved(t *testing.T) {
	runtimeDefault := corev1.SeccompProfileTypeRuntimeDefault
	pod := basePod()
	pod.Spec.SecurityContext = &corev1.PodSecurityContext{
		SeccompProfile: &corev1.SeccompProfile{
			Type: runtimeDefault,
		},
	}

	w := transform(pod)

	if w.Spec.SecurityContext == nil || w.Spec.SecurityContext.SeccompProfile == nil {
		t.Fatal("seccompProfile must be preserved")
	}
	if w.Spec.SecurityContext.SeccompProfile.Type != runtimeDefault {
		t.Fatalf("expected seccompProfile RuntimeDefault, got %s", w.Spec.SecurityContext.SeccompProfile.Type)
	}
}

func TestTransform_RunAsNonRoot_Preserved(t *testing.T) {
	pod := basePod()
	pod.Spec.SecurityContext = &corev1.PodSecurityContext{
		RunAsNonRoot: ptr.To(true),
	}

	w := transform(pod)

	if w.Spec.SecurityContext == nil || w.Spec.SecurityContext.RunAsNonRoot == nil || !*w.Spec.SecurityContext.RunAsNonRoot {
		t.Fatal("runAsNonRoot=true must be preserved")
	}
}

// --- Category 4: Capabilities (SCC always merges, preserved as-is) ---

func TestTransform_Capabilities_Preserved(t *testing.T) {
	// SCC merges defaultAddCapabilities and requiredDropCapabilities
	// unconditionally. We cannot separate user caps from SCC caps.
	// This test shows the combined result is preserved.
	pod := basePod()
	pod.Spec.Containers[0].SecurityContext = &corev1.SecurityContext{
		Capabilities: &corev1.Capabilities{
			Add:  []corev1.Capability{"NET_BIND_SERVICE", "SYS_PTRACE"},
			Drop: []corev1.Capability{"ALL"},
		},
	}

	w := transform(pod)

	sc := w.Spec.Containers[0].SecurityContext
	if sc == nil || sc.Capabilities == nil {
		t.Fatal("capabilities must be preserved")
	}
	if len(sc.Capabilities.Add) != 2 {
		t.Fatalf("expected 2 add capabilities, got %d", len(sc.Capabilities.Add))
	}
	if len(sc.Capabilities.Drop) != 1 || sc.Capabilities.Drop[0] != "ALL" {
		t.Fatal("drop ALL must be preserved")
	}
}

func TestTransform_ReadOnlyRootFilesystem_Preserved(t *testing.T) {
	pod := basePod()
	pod.Spec.Containers[0].SecurityContext = &corev1.SecurityContext{
		ReadOnlyRootFilesystem: ptr.To(true),
	}

	w := transform(pod)

	sc := w.Spec.Containers[0].SecurityContext
	if sc == nil || sc.ReadOnlyRootFilesystem == nil || !*sc.ReadOnlyRootFilesystem {
		t.Fatal("readOnlyRootFilesystem=true must be preserved")
	}
}

// --- Combined: realistic OpenShift pod with full SCC mutation ---

func TestTransform_FullSCCMutatedPod(t *testing.T) {
	// Simulates a pod after OpenShift restricted-v2 SCC admission:
	// SCC filled in seLinuxOptions, runAsUser, fsGroup, supplementalGroups,
	// seccompProfile, allowPrivilegeEscalation, and capabilities.
	pod := basePod()
	pod.Spec.SecurityContext = &corev1.PodSecurityContext{
		SELinuxOptions:     &corev1.SELinuxOptions{Level: "s0:c26,c10"},
		RunAsUser:          ptr.To(int64(1000620000)),
		RunAsNonRoot:       ptr.To(true),
		FSGroup:            ptr.To(int64(1000620000)),
		SupplementalGroups: []int64{1000620000},
		SeccompProfile:     &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault},
	}
	pod.Spec.Containers[0].SecurityContext = &corev1.SecurityContext{
		SELinuxOptions: &corev1.SELinuxOptions{Level: "s0:c26,c10"},
		RunAsUser:      ptr.To(int64(1000620000)),
		Capabilities: &corev1.Capabilities{
			Drop: []corev1.Capability{"ALL"},
		},
		AllowPrivilegeEscalation: ptr.To(false),
	}

	w := transform(pod)

	// All SecurityContext fields preserved (pass-through)
	if w.Spec.SecurityContext.SELinuxOptions == nil || w.Spec.SecurityContext.SELinuxOptions.Level != "s0:c26,c10" {
		t.Error("pod seLinuxOptions should be preserved")
	}
	if len(w.Spec.SecurityContext.SupplementalGroups) != 1 || w.Spec.SecurityContext.SupplementalGroups[0] != 1000620000 {
		t.Error("supplementalGroups should be preserved")
	}
	if w.Spec.Containers[0].SecurityContext.SELinuxOptions == nil || w.Spec.Containers[0].SecurityContext.SELinuxOptions.Level != "s0:c26,c10" {
		t.Error("container seLinuxOptions should be preserved")
	}

	if w.Spec.SecurityContext.RunAsUser == nil || *w.Spec.SecurityContext.RunAsUser != 1000620000 {
		t.Error("runAsUser should be preserved (ambiguous)")
	}
	if w.Spec.SecurityContext.FSGroup == nil || *w.Spec.SecurityContext.FSGroup != 1000620000 {
		t.Error("fsGroup should be preserved (ambiguous)")
	}
	if w.Spec.SecurityContext.RunAsNonRoot == nil || !*w.Spec.SecurityContext.RunAsNonRoot {
		t.Error("runAsNonRoot should be preserved")
	}
	if w.Spec.SecurityContext.SeccompProfile == nil {
		t.Error("seccompProfile should be preserved")
	}
	csc := w.Spec.Containers[0].SecurityContext
	if csc.RunAsUser == nil || *csc.RunAsUser != 1000620000 {
		t.Error("container runAsUser should be preserved (ambiguous)")
	}
	if csc.Capabilities == nil || len(csc.Capabilities.Drop) == 0 {
		t.Error("capabilities should be preserved")
	}
	if csc.AllowPrivilegeEscalation == nil || *csc.AllowPrivilegeEscalation {
		t.Error("allowPrivilegeEscalation should be preserved")
	}
}

func TestTransform_NilSecurityContext(t *testing.T) {
	pod := basePod()
	// No SecurityContext at all — should not panic
	w := transform(pod)

	if w.Spec.SecurityContext != nil {
		t.Error("nil pod SecurityContext should stay nil")
	}
	if w.Spec.Containers[0].SecurityContext != nil {
		t.Error("nil container SecurityContext should stay nil")
	}
}
