packer {
  required_plugins {
    amazon = {
      version = ">= 1.0.0"
      source = "github.com/hashicorp/amazon"
    }
  }
}

local "ts" {
  expression = formatdate("YYYYMMDDhhmm", timestamp())
}

source "amazon-ebs" "docker-optimized" {
  ami_name    = "docker-optimized-${local.ts}"

  source_ami_filter {
    filters = {
      virtualization-type = "hvm"
      name = "ubuntu-minimal/images/hvm-ssd-gp3/ubuntu-resolute-26.04-amd64-minimal-*"
      root-device-type = "ebs"
    }

    owners = [
      "099720109477"
    ]

    most_recent = true
  }

  instance_type         = "t3.medium"
  ssh_username          = "ubuntu"
  force_deregister      = true
  encrypt_boot          = true

  launch_block_device_mappings {
    device_name           = "/dev/sda1"
    encrypted             = true
    volume_size           = 20
    volume_type           = "gp3"
    delete_on_termination = true
  }
}

build {
  sources = [
    "source.amazon-ebs.docker-optimized"
  ]

  provisioner "shell" {
    environment_vars = [
      "DEBIAN_FRONTEND=noninteractive"
    ]

    inline = [
      "sleep 15",

      "sudo apt-get clean",
      "sudo apt-get update",
      "sudo apt-get install -y ca-certificates curl gnupg lsb-release unzip jq rng-tools",

      "curl https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip -o awscliv2.zip",
      "unzip awscliv2.zip",
      "sudo ./aws/install",
      "rm -rf ./aws ./awscliv2.zip",

      "curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg",
      "echo \"deb [arch=amd64 signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable\" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null",
      "sudo apt-get update",
      "sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin",
      "sudo gpasswd -a ubuntu docker",
      "sudo mkdir -p /etc/docker/",
      "sudo service docker restart"
    ]
  }
}
