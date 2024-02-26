import sys

from PIL import Image
from transformers import BlipForConditionalGeneration, BlipProcessor

processor = BlipProcessor.from_pretrained('./Salesforce__blip-image-captioning-base', local_files_only=True)
model = BlipForConditionalGeneration.from_pretrained('./Salesforce__blip-image-captioning-base', local_files_only=True).to("cuda")

image_path = sys.argv[1]

with open(image_path, 'rb') as fp:
    raw_image = Image.open(fp).convert('RGB')

# unconditional image captioning
inputs = processor(raw_image, return_tensors="pt").to("cuda")

out = model.generate(**inputs)
print(processor.decode(out[0], skip_special_tokens=True))
