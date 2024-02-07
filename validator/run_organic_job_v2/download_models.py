from transformers import BlipProcessor, BlipForConditionalGeneration

processor = BlipProcessor.from_pretrained("Salesforce/blip-image-captioning-base")
model = BlipForConditionalGeneration.from_pretrained("Salesforce/blip-image-captioning-base")

processor.save_pretrained('./Salesforce__blip-image-captioning-base')
model.save_pretrained('./Salesforce__blip-image-captioning-base')
