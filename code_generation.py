from langchain_groq import ChatGroq
from pyspark.sql import DataFrame, SparkSession

spark = SparkSession.builder.appName('Code_generation').getOrCreate()

def generate_code(df : DataFrame, metadata: dict,quality_rules: dict, error: str, llm: ChatGroq):
    
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

***Return only the Python code block between the markers (IMPORTANT)***
<CODE>
...
</CODE>

'''
    
    code = llm.invoke(prompt)

    print('✅✅ Code Generated ✅✅')

    return code.content