# Prerequisites

Local lab setup on a gaming PC for GPUaaS spike testing. Two SNO VMs
(tenant + GPU worker) with RTX 5090 passthrough to the worker VM.

## Hardware

| Component | Spec |
|-----------|------|
| CPU | Intel Core Ultra 9 285K (24 cores) |
| RAM | 62 GB |
| GPU | NVIDIA GeForce RTX 5090 (32 GB VRAM) |
| Disk | 1.6 TB NVMe, ~973 GB free |
| Motherboard | ASUS PRIME Z890-P WIFI |
| OS | Ubuntu 24.04.3 LTS (kernel 6.17.0-1032-oem) |
| Network | Ethernet (enp132s0), WiFi (wlp131s0) |

**Hardware-specific notes.** Several fixes in this guide are specific to this
hardware combination. On different hardware, some steps may not be needed or
may need adjustment:

- Sections 2 and 9-11 are specific to the **RTX 5090** (Blackwell, 32 GB BAR).
  Smaller GPUs may work with stock QEMU 8.2 and without the IOMMU/BAR workarounds.
- The `maxphysaddr limit='42'` in section 11 matches the **Arrow Lake** IOMMU
  address width (42-bit MGAW). Other Intel/AMD platforms have different widths.
  Check with: `sudo dmesg | grep -i 'mgaw'`.
- `intel_iommu=on` is Intel-specific. AMD systems use `amd_iommu=on` (often
  enabled by default).
- The PCI IDs in section 8 (`10de:2b85`, `10de:22e8`) and PCI slot addresses
  (`02:00.0`, `02:00.1`) are specific to this system. Find yours with
  `lspci -nn | grep NVIDIA`.

## 1. Enable VT-d in BIOS

Reboot, press **Delete** to enter BIOS.

Navigate to: **Advanced > System Agent (SA) Configuration > VT-d** and set to **Enabled**.

Save and exit. This enables IOMMU at the hardware level, required for GPU passthrough.

## 2. Enable IOMMU in the kernel

Four kernel flags are needed for GPU passthrough:

- `intel_iommu=on` -- activates the IOMMU (VT-d) so the kernel can isolate
  device DMA access per IOMMU group. Required for any PCI passthrough.
- `iommu=pt` -- sets IOMMU to passthrough mode. Without it, the kernel maps
  all device DMA through its own translation tables. GPUs with large BARs
  (the RTX 5090 has a 32 GB BAR) fail because the translation tables can't
  handle addresses that large. Passthrough mode lets QEMU/VFIO manage the
  mapping directly, bypassing that limit.
- `pci=realloc` -- lets the kernel reassign PCI BAR addresses at boot. Needed
  when the BIOS-assigned addresses for a large-BAR GPU overlap or conflict
  with other MMIO regions. Without it, the GPU BAR may not be properly
  mapped in the host address space.
- `vfio-pci.disable_idle_d3=1` -- prevents the vfio-pci driver from putting
  the GPU into D3 power state when idle. Some GPUs (including RTX 50-series)
  fail to reinitialize after D3, causing passthrough to break on VM start.

Edit GRUB config:

```bash
sudo nano /etc/default/grub
```

Change:

```
GRUB_CMDLINE_LINUX_DEFAULT="quiet splash"
```

To:

```
GRUB_CMDLINE_LINUX_DEFAULT="quiet splash intel_iommu=on iommu=pt pci=realloc vfio-pci.disable_idle_d3=1"
```

Apply and reboot:

```bash
sudo update-grub
sudo reboot
```

## 3. Verify IOMMU is active

After reboot, check that DMAR tables are loaded and IOMMU groups exist:

```bash
sudo dmesg | grep -iE 'iommu|dmar' | head -10
ls /sys/kernel/iommu_groups/ | wc -l
```

Expected: DMAR entries in dmesg, and a non-zero count of IOMMU groups.

## 4. Passwordless sudo

```bash
sudo visudo -f /etc/sudoers.d/jmorenas
```

Add this single line:

```
jmorenas ALL=(ALL) NOPASSWD:ALL
```

Verify:

```bash
sudo whoami
# should print: root (no password prompt)
```

## 5. SSH access

The PC is reachable at `192.168.1.129` (Ethernet) from the local network.
SSH key authentication is already configured.

```bash
ssh 192.168.1.129 "echo ok"
```

## 6. Pull secret

Download your pull secret from https://console.redhat.com/openshift/create/local
and save it to the repo root:

```bash
# On the gaming PC:
cat > ~/crc-lab-gpuaas/pull-secret.json << 'EOF'
<paste pull secret here>
EOF
```

This file is gitignored. The playbooks read it from `pull-secret.json` in the
project root.

## 7. Install Ansible

```bash
sudo apt-get install -y ansible
```

## 8. VFIO setup (GPU passthrough)

This binds the RTX 5090 to vfio-pci so it can be passed through to the worker
VM. After this, the GPU is no longer available for display. Use SSH only.

```bash
# Load vfio-pci at boot
sudo tee /etc/modules-load.d/vfio-pci.conf > /dev/null <<EOF
vfio
vfio_iommu_type1
vfio_pci
EOF

# Bind GPU to vfio-pci
sudo tee /etc/modprobe.d/vfio.conf > /dev/null <<EOF
options vfio-pci ids=10de:2b85,10de:22e8
softdep nvidia pre: vfio-pci
softdep nouveau pre: vfio-pci
EOF

# Blacklist nvidia/nouveau
sudo tee /etc/modprobe.d/blacklist-nvidia.conf > /dev/null <<EOF
blacklist nvidia
blacklist nvidia_drm
blacklist nvidia_modeset
blacklist nvidia_uvm
blacklist nouveau
EOF

# Switch to multi-user target (no GUI, saves resources)
sudo systemctl set-default multi-user.target

# Rebuild initramfs and reboot
sudo update-initramfs -u
sudo reboot
```

After reboot, verify the GPU is bound to vfio-pci:

```bash
lspci -nnk -s 02:00.0 | grep "Kernel driver"
# Expected: Kernel driver in use: vfio-pci
```

To reverse this and restore the GPU to desktop use:

```bash
sudo rm /etc/modules-load.d/vfio-pci.conf /etc/modprobe.d/vfio.conf /etc/modprobe.d/blacklist-nvidia.conf
sudo systemctl set-default graphical.target
sudo update-initramfs -u
sudo reboot
```

## 9. Build QEMU 9.2 from source (RTX 5090 passthrough)

Ubuntu 24.04 ships QEMU 8.2, which treats `VFIO_MAP_DMA` ENOENT errors on
GPU BAR MMIO regions as fatal. The RTX 5090 has a 32 GB BAR that triggers
this during VM startup. QEMU 9.0+ treats these errors as non-fatal warnings,
so passthrough works. Building from source is required.

Install build dependencies:

```bash
sudo apt-get install -y build-essential ninja-build pkg-config \
  libglib2.0-dev libpixman-1-dev libcap-ng-dev libattr1-dev \
  libaio-dev liburing-dev libslirp-dev python3-venv flex bison
```

Clone, configure, build, and install:

```bash
cd /tmp
git clone --depth 1 --branch v9.2.3 https://gitlab.com/qemu-project/qemu.git qemu-9.2.3
cd qemu-9.2.3
mkdir build && cd build
../configure \
  --target-list=x86_64-softmmu \
  --prefix=/usr/local \
  --enable-kvm \
  --enable-vhost-net \
  --enable-cap-ng \
  --enable-attr \
  --enable-linux-aio \
  --enable-linux-io-uring \
  --enable-slirp
make -j$(nproc)
sudo make install
```

Replace the system QEMU binary with a symlink to the new build. A symlink
is used instead of a copy so that QEMU resolves its firmware/BIOS paths
relative to the real binary location (`/usr/local/share/qemu/`).

```bash
sudo mv /usr/bin/qemu-system-x86_64 /usr/bin/qemu-system-x86_64.bak
sudo ln -s /usr/local/bin/qemu-system-x86_64 /usr/bin/qemu-system-x86_64
qemu-system-x86_64 --version
# Expected: QEMU emulator version 9.2.3
```

Update the libvirtd AppArmor profile to allow executing binaries from
`/usr/local/bin/`:

```bash
sudo sed -i '/\/usr\/bin\/\* PUx,/a\  /usr/local/bin/* PUx,' \
  /etc/apparmor.d/usr.sbin.libvirtd
sudo apparmor_parser -r /etc/apparmor.d/usr.sbin.libvirtd
sudo systemctl restart libvirtd
```

Update existing VM definitions to use a machine type that QEMU 9.2 provides
(the Ubuntu-patched `pc-q35-noble` type does not exist in upstream QEMU):

```bash
for vm in sno-tenant sno-worker; do
  sudo virsh dumpxml $vm > /tmp/$vm.xml
  sed -i 's/pc-q35-noble/pc-q35-9.2/g' /tmp/$vm.xml
  sudo virsh define /tmp/$vm.xml
done
```

## 10. Libvirtd memlock override

GPU passthrough requires mapping the entire GPU BAR into guest memory via
DMA. The default memlock limit (often 8 GB) is too low for a 32 GB GPU.
Without this override, `VFIO_MAP_DMA` fails with ENOMEM.

```bash
sudo mkdir -p /etc/systemd/system/libvirtd.service.d
sudo tee /etc/systemd/system/libvirtd.service.d/memlock.conf > /dev/null <<EOF
[Service]
LimitMEMLOCK=infinity
EOF

sudo systemctl daemon-reload
sudo systemctl restart libvirtd
```

## 11. Worker VM XML tweaks for RTX 5090

Three XML-level fixes are needed for the RTX 5090. The playbook
(`create-vm.yml`) applies these automatically when `gpu_passthrough: true`.
The manual steps below are for reference or if recreating a VM outside the
playbook.

**maxphysaddr limit.** Arrow Lake (Intel Core Ultra 9 285K) has a 42-bit
IOMMU address width (MGAW). By default QEMU may expose a wider physical
address space to the guest. If the guest allocates GPU memory at an address
above 2^42 (~4 TB), the IOMMU cannot map it and DMA fails. The limit
constrains the guest to stay within bounds.

**x-no-geforce-quirks.** QEMU applies legacy PCI quirks for GeForce cards
that interfere with Blackwell (RTX 50-series) initialization. This flag
disables them.

**rom bar off.** SeaBIOS tries to execute PCI option ROMs during boot. The
RTX 5090's VBIOS hangs when run inside a passthrough VM, freezing the guest
in real mode (CS=C000, option ROM space). Since the GPU is used for compute
only (not display), disabling the ROM BAR prevents SeaBIOS from loading it.
The guest OS GPU driver initializes the GPU directly without the VBIOS.

```bash
# Dump, patch, redefine
sudo virsh dumpxml sno-worker > /tmp/worker.xml

# Add maxphysaddr inside the <cpu> element
python3 -c "
import xml.etree.ElementTree as ET
tree = ET.parse('/tmp/worker.xml')
cpu = tree.find('cpu')
ET.SubElement(cpu, 'maxphysaddr', mode='passthrough', limit='42')
tree.write('/tmp/worker.xml', xml_declaration=True)
"

# Add rom bar=off to both GPU hostdev entries
sed -i '/<hostdev mode=.subsystem. type=.pci. managed=.yes.>/,/<\/hostdev>/{
  /<address type=.pci. domain=.0x0000. bus=.0x05./a\      <rom bar="off"/>
  /<address type=.pci. domain=.0x0000. bus=.0x06./a\      <rom bar="off"/>
}' /tmp/worker.xml

# Add qemu:commandline for x-no-geforce-quirks (if not already present)
if ! grep -q 'x-no-geforce-quirks' /tmp/worker.xml; then
  sed -i '/<\/domain>/i\
  <qemu:commandline xmlns:qemu="http://libvirt.org/schemas/domain/qemu/1.0">\
    <qemu:arg value="-global"/>\
    <qemu:arg value="vfio-pci.x-no-geforce-quirks=on"/>\
  </qemu:commandline>' /tmp/worker.xml
fi

sudo virsh define /tmp/worker.xml
```

Verify the worker starts without DMA errors:

```bash
sudo virsh start sno-worker
sudo dmesg | grep -i 'vfio.*error'
# Expected: no output (no errors)
```

## Running the playbooks

```bash
cd ~/crc-lab-gpuaas
ansible-playbook -i inventory.yml playbooks/01-create-vms.yml
ansible-playbook -i inventory.yml playbooks/02-wait-and-discover.yml
ansible-playbook -i inventory.yml playbooks/03-configure-clusters.yml
ansible-playbook -i inventory.yml playbooks/04-peer-clusters.yml
```

## Monitoring

Watch the SNO install progress:

```bash
# Install logs
tail -f ~/crc-lab-gpuaas/.workdir/tenant/.openshift_install.log
tail -f ~/crc-lab-gpuaas/.workdir/worker/.openshift_install.log

# VM console (Ctrl+] to detach)
sudo virsh console sno-tenant
sudo virsh console sno-worker

# VM status
sudo virsh list --all
```

## Teardown

```bash
ansible-playbook -i inventory.yml playbooks/teardown.yml
```
