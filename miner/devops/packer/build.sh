#!/bin/sh

# initialize packer script and building image
packer init .

packer build docker-optimized.pkr.hcl