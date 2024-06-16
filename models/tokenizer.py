# tokenizer.py

from nltk.tokenize import word_tokenize
import re

# 初始化NLTK分词器
nltk.download('punkt')

def preprocess_text(text):
    # 文本预处理：转换为小写、去除特殊字符
    text = text.lower()
    text = re.sub(r'[^a-zA-Z0-9\s]', '', text)
    return text

def tokenize_text(text):
    # 使用NLTK进行分词
    tokens = word_tokenize(text)
    return tokens

if __name__ == "__main__":
    # 示例代码：如何使用分词器和预处理函数
    example_text = "This is an example text for tokenization."
    processed_text = preprocess_text(example_text)
    tokens = tokenize_text(processed_text)
    print(f"Processed text: {processed_text}")
    print(f"Tokens: {tokens}")
