# Miner nginx image

Since there is no simple way to mount files from runner into nginx container, we have to build a custom nginx image with miner configuration baked in.

This image is automatically built/published when runner is built/published using `build-image.sh` / `publish-image.sh` scripts.