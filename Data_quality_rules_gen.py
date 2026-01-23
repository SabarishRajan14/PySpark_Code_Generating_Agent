from pyspark.sql import SparkSession, DataFrame
from langchain_groq import ChatGroq
from langchain_classic.schema import HumanMessage, SystemMessage
import json




def rule_generation(spark : SparkSession, df: DataFrame, metadata: dict, llm: ChatGroq):
    print('\n Generating Data Quality Rules...')

    # 1. Get Schema information so LLM knows column types (String, Long, etc.)
    schema_json = df.schema.json()
    
    # 2. Convert sample to string for the prompt
    sample_data = df.limit(20).toPandas().to_dict(orient='records')

    prompt = f'''
You are an expert Data Governance and Data Quality agent for PySpark environments.

### INPUT CONTEXT:
1. **Schema (Technical Types):**
{schema_json}

2. **Business Metadata:**
{json.dumps(metadata, indent=2)}

3. **Sample Data (First 20 rows):**
{json.dumps(sample_data, indent=2)}

### YOUR TASK:
Generate a list of Data Quality rules. Each rule must be a machine-evaluable PySpark condition.

### CONSTRAINTS (CRITICAL):
- **Numeric Rules:** Only apply 'outlier' or 'range_check' to numeric columns (Integer, Long, Double). 
- **Correlation:** NEVER use 'correlation_check' on String columns.
- **Regex Robustness:** Analyze the sample data carefully. Ensure regex patterns for names, phones, and jobs are inclusive of punctuation (.,), special characters (&, /), and extensions (x) seen in the sample.
- **Syntactic Accuracy:** Use PySpark SQL syntax for conditions (e.g., `col('email').rlike('...')` or `col('price') > 0`).

### RULE CATEGORIES:
1) **Basic Data Quality:** Completeness (null checks), Uniqueness.
2) **Semantic Integrity:** Regex for specific types (Email, Phone, Name).
3) **Formatting:** Consistency in patterns.
4) **Logical Checks:** Cross-column dependencies (e.g., ship_date > order_date).

### OUTPUT FORMAT:
Return ONLY a valid JSON list of objects. No explanations.
Example Structure:
{{
    "rule_id": "dq_01",
    "rule_category": "Semantic Integrity",
    "rule_type": "regex",
    "target_columns": ["phone"],
    "condition": "col('phone').rlike('^[0-9\\\\s\\\\+\\\\-\\\\(\\\\)\\\\.x]+$')",
    "evaluation_metric": "violation_ratio",
    "threshold": 0.05,
    "severity": "high",
    "recommended_action": "Flag for manual review"
}}
'''
    
    message = [
        SystemMessage(content="You are a strict PySpark Data Quality rule generator. You only output valid JSON."),
        HumanMessage(content=prompt)
    ]

    response = llm.invoke(message)

    print('✅✅ Rules generated ✅✅')
    return response.content