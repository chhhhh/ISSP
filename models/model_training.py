# models/model_training.py

from transformers import AutoTokenizer, AutoModelForSeq2SeqLM
from torch.utils.data import DataLoader, Dataset
import torch
import torch.nn.functional as F


class CustomDataset(Dataset):
    def __init__(self, texts, targets):
        self.texts = texts
        self.targets = targets

    def __len__(self):
        return len(self.texts)

    def __getitem__(self, idx):
        inputs = tokenizer(self.texts[idx], return_tensors="pt", padding=True, truncation=True)
        targets = tokenizer(self.targets[idx], return_tensors="pt", padding=True, truncation=True)
        return {
            "input_ids": inputs.input_ids,
            "attention_mask": inputs.attention_mask,
            "labels": targets.input_ids
        }


def train_model():
    # 加载预训练模型和分词器
    tokenizer = AutoTokenizer.from_pretrained("juierror/text-to-sql-with-table-schema")
    model = AutoModelForSeq2SeqLM.from_pretrained("juierror/text-to-sql-with-table-schema")

    # 准备数据集
    train_texts = ["input text 1", "input text 2"]
    train_targets = ["output text 1", "output text 2"]
    train_dataset = CustomDataset(train_texts, train_targets)
    train_loader = DataLoader(train_dataset, batch_size=2, shuffle=True)

    # 定义优化器和设备
    optimizer = torch.optim.AdamW(model.parameters(), lr=1e-5)
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    model.to(device)

    # 训练模型
    model.train()
    for epoch in range(5):
        for batch in train_loader:
            input_ids = batch["input_ids"].to(device)
            attention_mask = batch["attention_mask"].to(device)
            labels = batch["labels"].to(device)

            outputs = model(input_ids=input_ids, attention_mask=attention_mask, labels=labels)
            loss = outputs.loss
            loss.backward()
            optimizer.step()
            optimizer.zero_grad()

        print(f"Epoch {epoch + 1}, Loss: {loss.item()}")

    # 保存微调后的模型
    model.save_pretrained("models/saved_model")
