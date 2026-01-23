from pyspark.sql import SparkSession
from metadata_gen import generate_metadata_using_llm
from Data_quality_rules_gen import rule_generation
from code_generation import generate_code
from running_generated_code import execute_generated_code
from pathlib import Path
from langchain_groq import ChatGroq
from langchain_google_genai import ChatGoogleGenerativeAI
import os
from dotenv import load_dotenv

load_dotenv()

groq_api = os.getenv('GROQ_KEY')
gemini_api = os.getenv('GEMINI_KEY')
try:
    Groq = ChatGroq(
        model = 'llama-3.1-8b-instant',
        temperature = 0.0,
        api_key = groq_api
    )
    print('Groq Loaded.')
except Exception as e:
    raise RuntimeError(f'Error loading LLM:{e}')

try: 
    Gemini = ChatGoogleGenerativeAI(
        model = 'gemini-3-flash-preview',
        temperature = 0.0,
        api_key = gemini_api
    )
    print('Gemini Loaded.')
except Exception as e:
    raise RuntimeError('Error loading Gemini')

spark = SparkSession.builder.appName('main').getOrCreate()
path = "C:/Users/aksme/Desktop/SAS_trail/PySpark_Code_Generating_Agent/Fake_data.csv"
df = spark.read.csv(path, header = True)

file_name = Path(path).name

initial_metadata = generate_metadata_using_llm(spark = spark, df=df, file_name = file_name, llm = Groq)

quality_rules = rule_generation(spark=spark, df = df, metadata=initial_metadata.model_dump_json(indent = 2), llm = Groq)

max_tries = 1
error = None
while max_tries < 6:

    #groq_rate_limiter()

    code = generate_code(
        df = df, 
        metadata=initial_metadata.model_dump_json(indent = 2), 
        quality_rules=quality_rules, 
        error=error
        #llm = Gemini
    )


    print(f'Generated code :\n {code}')
    start = "<CODE>"
    end = "</CODE>"

    if start not in code or end not in code:
        error = 'ValueError: Executable block not found'
        print(f'\n 🛑Error (attempt {max_tries}) \n Error : {error}\n...Retrying ...\n')
        max_tries += 1
        continue

    code_string = code.split(start)[1].split(end)[0].strip()

    result = execute_generated_code(spark = spark, rules=quality_rules, sample_df= df.limit(20), metadata = initial_metadata.model_dump_json(indent = 2), code_string = code_string)

    if result['success'] == False:
        error = result['error']
        print(f'\n 🛑Error (attempt {max_tries}) \n Error : {error}\n...Retrying ...\n')
        max_tries += 1
    else:
        print('\n ✅Success: \n')
        print(result["output"], "\n")
        break

    if max_tries >5:
        raise RuntimeError('🛑🛑🛑🛑🛑Exhausted Max Tries.🛑🛑🛑🛑🛑')




#print(f'\n Initial metadata:\n {initial_metadata.model_dump_json(indent = 2)}\n')

#print(f'The Data Quality rules to be applied on the dataset is as follows: \n{quality_rules}')

#print(f'💻The generated code is 💻: \n {code}')



