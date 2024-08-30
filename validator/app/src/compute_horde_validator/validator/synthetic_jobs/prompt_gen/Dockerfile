# Start from a CUDA-enabled base image
FROM nvidia/cuda:12.2.2-runtime-ubuntu22.04

# Set working directory
WORKDIR /app

# Install Python and system dependencies
RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip

# Install PyTorch and CUDA
RUN pip3 install torch torchvision torchaudio --index-url https://download.pytorch.org/whl/cu124

# Install Hugging Face Transformers and other necessary packages
RUN pip3 install transformers sentencepiece accelerate bitsandbytes

# Create an output folder
RUN mkdir /app/output

# Copy your Python script into the container
COPY llama/ /app/llama/
COPY generate_prompts.py .
COPY seeds.py .

# Set the entrypoint to run your script
ENTRYPOINT ["python3", "generate_prompts.py"]
