# Challenge executor

This is the executor for the challenge. It is a docker image that can be run on any machine that has docker installed.

Fine-tuning is encouraged.

## Setup

To build the image, run:

```bash
docker-compose build
```

To list devices available for hashcat, run:

```bash
docker run --rm computehorde/executor:v1 -I
```

Only CPU will be available by default.

### Nvidia

> This was tested on AWS EC2 with Nvidia T4 GPU.

First, ensure that [nvidia drivers are installed and operational](https://docs.nvidia.com/datacenter/tesla/tesla-installation-notes/index.html) on executor machine itself. After this is done, run `nvidia-smi` to verify that the GPU is seen by the os.

Running this docker image on Nvidia GPU requires [Nvidia Container Toolkit](https://github.com/NVIDIA/nvidia-container-toolkit). Follow [installing the nvidia container toolkit](https://docs.nvidia.com/datacenter/cloud-native/container-toolkit/latest/install-guide.html#installing-the-nvidia-container-toolkit) manual.

After it is installed, verify that nvidia GPU is recognized by docker container:

```bash
> docker run --rm --runtime=nvidia --gpus all --entrypoint nvidia-smi computehorde/executor:v1

+---------------------------------------------------------------------------------------+
| NVIDIA-SMI 545.23.08              Driver Version: 545.23.08    CUDA Version: 12.3     |
|-----------------------------------------+----------------------+----------------------+
| GPU  Name                 Persistence-M | Bus-Id        Disp.A | Volatile Uncorr. ECC |
| Fan  Temp   Perf          Pwr:Usage/Cap |         Memory-Usage | GPU-Util  Compute M. |
|                                         |                      |               MIG M. |
|=========================================+======================+======================|
|   0  Tesla T4                       On  | 00000000:00:1E.0 Off |                    0 |
| N/A   30C    P8              10W /  70W |      2MiB / 15360MiB |      0%      Default |
|                                         |                      |                  N/A |
+-----------------------------------------+----------------------+----------------------+
```

And also verify that hashcat sees the GPU as well:

```bash
> docker run --rm --runtime=nvidia --gpus all computehorde/executor:v1 -I

hashcat (v6.2.5) starting in backend information mode

CUDA Info:
==========

CUDA.Version.: 12.3

Backend Device ID #1 (Alias: #2)
  Name...........: Tesla T4
  Processor(s)...: 40
  Clock..........: 1590
  Memory.Total...: 14929 MB
  Memory.Free....: 14824 MB
  PCI.Addr.BDFe..: 0000:00:03.6

OpenCL Info:
============

OpenCL Platform ID #1
  Vendor..: NVIDIA Corporation
  Name....: NVIDIA CUDA
  Version.: OpenCL 3.0 CUDA 12.3.68

  Backend Device ID #2 (Alias: #1)
    Type...........: GPU
    Vendor.ID......: 32
    Vendor.........: NVIDIA Corporation
    Name...........: Tesla T4
    Version........: OpenCL 3.0 CUDA
    Processor(s)...: 40
    Clock..........: 1590
    Memory.Total...: 14929 MB (limited to 3732 MB allocatable in one block)
    Memory.Free....: 14784 MB
    OpenCL.Version.: OpenCL C 1.2
    Driver.Version.: 545.23.08
    PCI.Addr.BDF...: 00:03.6

OpenCL Platform ID #2
  Vendor..: The pocl project
  Name....: Portable Computing Language
  Version.: OpenCL 2.0 pocl 1.8  Linux, None+Asserts, RELOC, LLVM 11.1.0, SLEEF, DISTRO, POCL_DEBUG

  Backend Device ID #3
    Type...........: CPU
    Vendor.ID......: 128
    Vendor.........: GenuineIntel
    Name...........: pthread-Intel(R) Xeon(R) Platinum 8259CL CPU @ 2.50GHz
    Version........: OpenCL 1.2 pocl HSTR: pthread-x86_64-pc-linux-gnu-cascadelake
    Processor(s)...: 4
    Clock..........: 2699
    Memory.Total...: 13641 MB (limited to 2048 MB allocatable in one block)
    Memory.Free....: 6788 MB
    OpenCL.Version.: OpenCL C 1.2 pocl
    Driver.Version.: 1.8

```

## Usage

The image automatically launches `hashcat`. If you want to run something else, use `--entrypoint` flag, i.e. to run bash, type:

```bash
docker run --rm --entrypoint=/bin/bash -ti computehorde/executor:v1
```

Below is a command to run hashcat on test challenge. Output should be `passwd`, it takes up to 1min on T1 GPU:

```bash
docker run --rm -ti \
    --runtime=nvidia --gpus all \
    computehorde/executor:v1 \
    --attack-mode 3 \
    --workload-profile 3 \
    --optimized-kernel-enable \
    --hash-type 1410 \
    --hex-salt \
    -1 ?l?d?u \
    --quiet \
    aad072a7927f39651ccde815d3a0ead2a14aa19559ae89561a370bce20738baf:bec0fba4fecb176a ?1?1?1?1?1?1
```

Instead of challenge string, one could specify path to text file with list of challenges.
