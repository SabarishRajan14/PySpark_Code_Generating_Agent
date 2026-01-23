from langchain_groq import ChatGroq
from langchain_google_genai import ChatGoogleGenerativeAI
from transformers import AutoModelForCausalLM
from pyspark.sql import DataFrame, SparkSession

try:
    llm = AutoModelForCausalLM.from_pretrained(
        "deepseek-ai/deepseek-coder-6.7b-base"
    )
    print('✅Deepseek Coder loaded')
except Exception as e:
    raise RuntimeError(f'🛑🛑🛑Error loading DeepSeek Coding model \n:{e}')

spark = SparkSession.builder.appName('Code_generation').getOrCreate()

def generate_code(df : DataFrame, metadata: dict,quality_rules: dict, error: str ,llm = llm):
    
    print('\n Generating Code...')

    sample_df = df.limit(20).toPandas().to_dict(orient='records')

    prompt = f'''
You are a Senior Data Quality Engineer specializing in building metadata-driven pipelines in PySpark.

### INPUTS:
- **Metadata**: {metadata}
- **Sample Data**: {sample_df}
- **Data Quality Rules**: {quality_rules}

### PREVIOUS EXECUTION ERROR (IMPORTANT):
The previously generated PySpark code failed during execution with the following error.
Analyze this error carefully and correct the code accordingly. Do NOT repeat the same mistake.

{error}

### TASK:
Generate a modular PySpark script that applies the provided Data Quality rules to a DataFrame named `df`.

### CRITICAL TECHNICAL REQUIREMENTS:
1. **Syntax Consistency**: 
   - Use `pyspark.sql.functions` imported **only** as:
     `from pyspark.sql import functions as F`
   - Use `pyspark.sql.types` where needed.
   - Do NOT import `F` from `pyspark.sql.functions`.
   - If using `F.expr()`, ensure conditions use standard SQL operators
     (`AND`, `OR`, `NOT`, `RLIKE`, `IS NULL`)
     and NOT Python operators (`&&`, `||`, `!`).
   - For regex (`RLIKE`), ensure backslashes are properly escaped
     (e.g., `\\\\d` for digits).

2. **Rule Categorization**:
   - **Row-Level Rules (Completeness, Regex, Range)**:
     Add a violation column per rule using:
     `F.when(~(condition), 1).otherwise(0).alias("violation_{{rule_id}}")`
   - **Dataset-Level Rules (Uniqueness, Correlation)**:
     Do NOT apply row-by-row.
     Compute these as aggregate metrics separately.

3. **Handling Complex Rules**:
   - **Uniqueness**:
     Compare:
     `df.count()` vs
     `df.select(target_columns).distinct().count()`
   - **Regex**:
     Ensure compatibility with Spark SQL `RLIKE`.

4. **Output Expectations**:
   - Implement a function:
     `apply_dq_rules(df, metadata, quality_rules)`
   - The function must return:
     1. A DataFrame with all audit / violation columns appended.
     2. A summary structure (dict or DataFrame) with failure counts
        for every `rule_id`.
   - Do NOT invent rules.
   - Do NOT ignore any provided rule.
   - Include all required imports.
   - The script must be **self-contained and executable**.

### OUTPUT FORMAT:
- Comment the `rule_id` clearly above the code implementing it.
- Return **only** the Python code enclosed between the markers below.
- Do NOT include explanations or markdown.

***You MUST return your response in the following exact format.
Do NOT include any text before or after.***

<CODE>
# valid executable Python code only
</CODE>

If you include anything outside these tags, the response is invalid.


'''
    
    llm_answer = llm.invoke(prompt)

    raw_output = llm_answer.content

    if isinstance(raw_output, str):
        code = raw_output
        print('✅✅ Code Generated ✅✅')
    
    elif isinstance(raw_output, list):
        code = "".join(
            part.get("text", "") for part in raw_output if isinstance(part, dict)
        )
        print('✅✅ Code Generated ✅✅')
    else:
        raise TypeError(f'Unexpected content type : {type(raw_output)}')

    

    return str(code)