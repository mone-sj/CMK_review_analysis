import torch
import os

def gpuNumber_set():
    try:
        #os.environ["CUDA_DEVICE_ORDER"]="PCI_BUS_ID"
        os.environ["CUDA_VISIBLE_DEVICES"]= "0,1"
        device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
        print(device)
    except:
        device=torch.device('cpu')
    return device
