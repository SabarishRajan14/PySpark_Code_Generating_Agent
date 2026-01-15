from pyspark.sql import SparkSession
from metadata_gen import generate_metadata_using_llm
from Data_quality_rules_gen import rule_generation
from code_generation import generate_code
from running_generated_code import execute_generated_code
from pathlib import Path
from langchain_groq import ChatGroq
import os
import re
from dotenv import load_dotenv

load_dotenv()

groq_api = os.getenv('GROQ_KEY')

try:
    llm = ChatGroq(
        model = 'llama-3.1-8b-instant',
        temperature = 0.0,
        api_key = groq_api
    )
    print('LLM Loaded.')
except Exception as e:
    raise RuntimeError(f'Error loading LLM:{e}')

spark = SparkSession.builder.appName('main').getOrCreate()
path = "C:/Users/aksme/Desktop/SAS_trail/PySpark_Code_Generating_Agent/Fake_data.csv"
df = spark.read.csv(path, header = True)

file_name = Path(path).name

initial_metadata = generate_metadata_using_llm(df=df, file_name = file_name, llm = llm)

quality_rules = rule_generation(df = df, metadata=initial_metadata.model_dump_json(indent = 2), llm = llm)

code = generate_code(df = df, metadata=initial_metadata.model_dump_json(indent = 2), quality_rules=quality_rules, llm = llm)

#print(f'\n Initial metadata:\n {initial_metadata.model_dump_json(indent = 2)}\n')

#print(f'The Data Quality rules to be applied on the dataset is as follows: \n{quality_rules}')

#print(f'💻The generated code is 💻: \n {code}')

print('\n Checking if the code is executable or not...')


start = "<CODE>"
end = "</CODE>"

if start not in code or end not in code:
    raise ValueError('Executable block not found')

code_string = code.split(start)[1].split(end)[0].strip()

result = execute_generated_code(rules=quality_rules, sample_df= df.limit(20), metadata = initial_metadata.model_dump_json(indent = 2), code_string = code_string)
print(f'\n Result of running the program:\n')
print(result)