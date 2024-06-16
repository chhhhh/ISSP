from transformers import AutoTokenizer, AutoModelForSequenceClassification
import torch


class MyModelService:
    def __init__(self, model_dir):
        self.model_dir = model_dir
        self.config_path = f"{model_dir}/config.json"
        self.model_path = f"{model_dir}/pytorch_model.bin"
        self.tokenizer_path = f"{model_dir}/special_tokens_map.json"

        # 加载配置文件和模型
        self.config = AutoConfig.from_pretrained(self.config_path)
        self.model = AutoModelForSequenceClassification.from_pretrained(self.model_path, config=self.config)

        # 加载分词器
        self.tokenizer = AutoTokenizer.from_pretrained(self.tokenizer_path)

    def predict(self, text):
        inputs = self.tokenizer(text, return_tensors="pt", truncation=True, padding=True)
        outputs = self.model(**inputs)
        predictions = torch.argmax(outputs.logits, dim=-1)
        return predictions


# 单例模式管理模型服务
model_service = MyModelService("saved_model")
