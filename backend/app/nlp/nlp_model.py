from transformers import AutoTokenizer, AutoModelForCausalLM
from deep_translator import GoogleTranslator

model_name = "PipableAI/pip-sql-1.3b"

tokenizer = AutoTokenizer.from_pretrained(model_name)
model = AutoModelForCausalLM.from_pretrained(model_name)
translator = GoogleTranslator()


def generate_sql_query(query_cn):
    translated_query = translator.translate(query_cn, src='zh-cn', dest='en')
    schema = """..."""  # 定义schema的具体内容
    prompt = f"""<schema>{schema}</schema>
    <question>{translated_query}</question>
    <sql>"""

    inputs = tokenizer.encode(prompt, return_tensors='pt', padding=True, truncation=True)
    outputs = model.generate(inputs, max_length=512, num_return_sequences=1, pad_token_id=tokenizer.eos_token_id)
    sql_query_en = tokenizer.decode(outputs[0], skip_special_tokens=True)

    return sql_query_en
