import sys
import torch
from transformers import AutoTokenizer, AutoModelForCausalLM
from datasets import load_dataset
from torch.utils.data import DataLoader
from transformers import DataCollatorForSeq2Seq, AdamW, get_linear_schedule_with_warmup

# Ensure 'bitsandbytes' is removed from sys.modules
if 'bitsandbytes' in sys.modules:
    del sys.modules['bitsandbytes']

# Load the dataset
dataset_path = "/Users/zakwanzahid/PycharmProjects/GenAI/datasets/WildChat-1M"
ds = load_dataset(dataset_path)
ds["train"] = ds["train"].select(range(500))

# Load TinyLlama model and tokenizer
tinyllama_model_path = "/Users/zakwanzahid/PycharmProjects/GenAI/models/TinyLlama/TinyLlama-1.1B-Chat-v1.0"
tokenizer_tinyllama = AutoTokenizer.from_pretrained(tinyllama_model_path)
model_tinyllama = AutoModelForCausalLM.from_pretrained(tinyllama_model_path)

# Skip quantization and proceed with the model as is (no optimization for now)
model_tinyllama = model_tinyllama.to(torch.device("cpu"))


# Preprocessing function
def preprocess_data(examples):
    # Flatten conversation (if it's a list of dicts) into a list of strings
    if isinstance(examples['conversation'], list):
        conversation = [str(item['text']) if isinstance(item, dict) and 'text' in item else str(item) for item in examples['conversation']]
    else:
        conversation = str(examples['conversation'])  # If it's not a list, make sure it's a string

    # Tokenize conversation, apply padding and truncation
    tokenized = tokenizer_tinyllama(conversation, truncation=True, padding="max_length", max_length=512)

    # Use the input_ids as labels for language modeling (next-token prediction)
    tokenized['labels'] = tokenized['input_ids'].copy()

    return tokenized



# Apply the map function to preprocess the dataset
train_data = ds["train"].map(preprocess_data, batched=True)

# Convert train_data to a PyTorch-friendly format
train_data.set_format(type='torch', columns=['input_ids', 'attention_mask', 'labels'])

# Set up DataCollator and DataLoader
data_collator = DataCollatorForSeq2Seq(tokenizer_tinyllama, model=model_tinyllama)
train_dataloader = DataLoader(train_data, batch_size=8, collate_fn=data_collator)

# Set up optimizer and learning rate scheduler
optimizer = AdamW(model_tinyllama.parameters(), lr=5e-5)
num_epochs = 3
scheduler = get_linear_schedule_with_warmup(
    optimizer,
    num_warmup_steps=0,
    num_training_steps=len(train_dataloader) * num_epochs
)

# Training loop
model_tinyllama.train()

for epoch in range(num_epochs):
    print(f"Starting epoch {epoch + 1}/{num_epochs}...")
    for batch_idx, batch in enumerate(train_dataloader):
        print(f"Processing batch {batch_idx + 1}/{len(train_dataloader)}...")

        optimizer.zero_grad()

        # Move data to CPU (if it's not already)
        input_ids = batch['input_ids'].to(torch.device("cpu"))
        attention_mask = batch['attention_mask'].to(torch.device("cpu"))

        # Forward pass through the model
        outputs = model_tinyllama(input_ids, attention_mask=attention_mask, labels=input_ids)
        loss = outputs.loss

        # Backpropagation
        loss.backward()
        optimizer.step()
        scheduler.step()

        print(f"Epoch: {epoch + 1}/{num_epochs}, Batch: {batch_idx + 1}/{len(train_dataloader)}, Loss: {loss.item()}")

# Save the fine-tuned model
model_tinyllama.save_pretrained("fine_tuned_tinyllama_no_quantization")
tokenizer_tinyllama.save_pretrained("fine_tuned_tinyllama_no_quantization")

print("Fine-tuning complete! Model saved.")
