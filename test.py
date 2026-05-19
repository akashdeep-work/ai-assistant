import torch
from torch.utils.data import Dataset
from PIL import Image
import os
import torch.nn as nn
import torch.nn.functional as F

class MyFirstDataset(Dataset):
    def __init__(self, folder_path, transform=None):
        self.folder_path = folder_path
        self.transform = transform
        self.image_filenames = os.listdir(folder_path)

    def __len__(self):
        return len(self.image_filenames)
    
    def __getitem__(self, idx):
        img_name = self.image_filenames[idx]
        img_path = os.path.join(self.folder_path,img_name)

        image = Image.open(img_path).convert('RGB')

        if self.transform:
            image = self.transform(image)
        return image, 0 if img_name.lower().startswith('cat') else 1
    


# class MyFirstCNN(nn.Module):
#     def __init__(self, *args, **kwargs):
#         super().__init__(*args, **kwargs)

#         self.cnn = 