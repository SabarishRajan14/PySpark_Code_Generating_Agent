from langchain_classic.schema import HumanMessage, SystemMessage
from langchain_groq import ChatGroq
from pyspark.sql import DataFrame, SparkSession
import json
from pydantic import BaseModel
from typing import List, Optional, Literal
import ast

class QualityRule(BaseModel):
    rule_type: str
    expression: Optional[str] = None
    severity:Literal["low", "medium", "high"]
    description: str

class ColumnMetadata(BaseModel):
    column_name: str
    semantic_type: str
    pii_category: Optional[str]
    recommended_quality_rules: List[QualityRule]

class Metadata(BaseModel):
    dataset_name: str
    contains_pii: bool
    business_domain: str

class MetadataResponse(BaseModel):
    dataset_metadata: Metadata
    columns: List[ColumnMetadata]



metadata_format = '''
{
  'dataset_metadata': {
    'dataset_name': 'string',
    'contains_pii': 'boolean',
    'business_domain': 'string'
  },
  'columns': [
    {
      'column_name': 'string',
      'semantic_type': 'string',
      'pii_category': 'string | none',
      'recommended_quality_rules': [
        {
          'rule_type': 'string',
          'expression': 'string | null',
          'severity': 'low | medium | high',
          'description': 'string'
        }
      ]
    }
  ]
}
'''

metadata_example = '''{
  'dataset_metadata': {
    'contains_pii': True,
    'business_domain': 'customer_data'
  },
  'columns': [
    {
      'column_name': 'phone',
      'semantic_type': 'phone',
      'pii_category': 'direct_identifier',
      'recommended_quality_rules': [
        {
          'rule_type': 'regex',
          'expression': '^[0-9\\s\\+\\-\\(\\)\\.x]+$',
          'severity': 'high',
          'description': 'Phone must allow numbers, spaces, dots, and extensions'
        }
      ]
    },
    {
      'column_name': 'company',
      'semantic_type': 'company',
      'pii_category': 'indirect_identifier',
      'recommended_quality_rules': [
        {
          'rule_type': 'regex',
          'expression': '^[a-zA-Z0-9\\s,\\.\\-&]+$',
          'severity': 'high',
          'description': 'Company names may include hyphens and ampersands'
        }
      ]
    }
  ]
}'''

def generate_raw_metadata(df : DataFrame, file_name : str, llm : ChatGroq):
    # gets metadata for the pyspark dataframe 

    schema = df.schema.simpleString()
    columns = df.columns
    stats = {
        col : {
            "null_ratio" : df.filter(df[col].isNull()).count()/ df.count(),
            "distinct_ratio" : df.select(col).distinct().count() / df.count()
        } for col in columns
    }
    
    sample = df.limit(10).toPandas().to_dict(orient = "records")

    
    context = f'''
Given below is the information about the PySpark dataframe for the LLM to generate metadata.

------------------------------
Schema:
{schema}
------------------------------
Statistical data :
{json.dumps(stats, indent = 2)}
------------------------------
Sample data:
{json.dumps(sample, indent =2)}
'''
    
    prompt = f'''
You are a data governance and data quality agent.

Rules:
- Return ONLY valid Python dict literal
- Use single quotes
- Do NOT include explanations or comments
- Follow the schema EXACTLY
- Do NOT invent new fields

Dataset name : {file_name}

Columns :
{columns}

Context of the dataset :
{context}

Task :
1. Infer semantic type for each column.
2. Identify if column contains PII.
3. Propose data quality rules. 
   - CRITICAL: Ensure 'regex' expressions are inclusive of common variations found in the sample data (e.g., periods in names, extensions 'x' or dots in phones, hyphens/ampersands in company names, and commas/slashes in job titles).

Output strictly in Python dict literal with this structure:

{metadata_format}

An example of the metadata that is supposed to be generated is :
{metadata_example}
'''
    
    message = [
        SystemMessage(content = "You are a strict Python dict literal generator. Return only valid Python dict literal."),
        HumanMessage(content = prompt)
    ]
    

    #structured_llm = llm.with_structured_output(MetadataResponse)
    response = llm.invoke(message)

    return response.content




def validate_response(metadata:dict):
    return MetadataResponse(**metadata)

"""def correct_json_response(error_msg : str, json_response : dict, llm : ChatGroq):
    fix_prompt = f'''
You are an expert JSON repair agent. 

The following JSON is invalid:
{json_response}

The error is :
{error_msg}

Rulse:
- Only return VALID JSON
- Don NOT add or remove fields
- Preserve original values
- Fix syntax only
'''
    
    response = llm.with_structured_output(MetadataResponse).invoke(fix_prompt)
    return response.model_dump()
"""
def generate_metadata_using_llm(spark : SparkSession, df: DataFrame, file_name : str, llm: ChatGroq, max_tries = 5):

    print('\n Generating Metadata...')
    try:
        print('\rGetting Raw Metadata...', end = "", flush = True)
        raw_metadata = generate_raw_metadata(df=df, file_name=file_name, llm = llm) 
        #print(f'\rGenerated Metadata : \n {raw_metadata}', end = "")

        print('\rParsing the raw metadata...', end = "")   
        parsed_metadata = ast.literal_eval(raw_metadata)

        print('\rValidating Parsed Metadata...', end = "", flush = True)
        validated_metadata = validate_response(parsed_metadata)
            

        print('\r✅ Metadata generation successful.          ', end ="\n")
        return validated_metadata
        
    except Exception as e:
        raise RuntimeError("🛑ERROR OCCURED WHILE METADATA GENERATION. (MAX TRIES EXHAUSTED)🛑")