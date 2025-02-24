from transformers import AutoTokenizer, AutoModelForCausalLM
from datasets import load_dataset
import torch
from torch.utils.data import DataLoader
from transformers import DataCollatorForSeq2Seq, AdamW, get_linear_schedule_with_warmup
from peft import LoraModel, get_peft_model, LoraConfig

# Load the dataset
dataset_path = "/Users/zakwanzahid/PycharmProjects/GenAI/datasets/WildChat-1M"
ds = load_dataset(dataset_path)
ds["train"] = ds["train"].select(range(100))
print(ds["train"].column_names)

