from torchvision import datasets, transforms
from transformers import DistilBertTokenizer

transform = transforms.Compose([transforms.ToTensor(), transforms.Lambda(lambda x: x.view(-1))])
train_dataset = datasets.MNIST(root='./data', train=True, download=True, transform=transform)
test_dataset = datasets.MNIST(root='./data', train=False, download=True, transform=transform)

tokenizer = DistilBertTokenizer.from_pretrained('distilbert-base-uncased')

tokenizer.save_pretrained('./distilbert-base-uncased')
