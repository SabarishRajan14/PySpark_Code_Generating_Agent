from langchain_groq import ChatGroq
from pyspark.sql import DataFrame, SparkSession

spark = SparkSession.builder.appName('Code_generation').getOrCreate()

def generate_code(df : DataFrame, metadata: dict,quality_rules: dict, llm: ChatGroq):

    sample_df = df.limit(20).toPandas().to_dict(orient='records')

    prompt = f'''
You are a Senior Data Quality Engineer specializing in building metadata-driven pipelines in PySpark.

### INPUTS:
- **Metadata**: {metadata}
- **Sample Data**: {sample_df}
- **Data Quality Rules**: {quality_rules}

### TASK:
Generate a modular PySpark script that applies the provided Data Quality rules to a DataFrame named `df`.

### CRITICAL TECHNICAL REQUIREMENTS:
1. **Syntax Consistency**: 
   - Use `pyspark.sql.functions` (imported as `F`) and `pyspark.sql.types`.
   - If using `F.expr()`, ensure conditions use standard SQL operators (`AND`, `OR`, `NOT`, `RLIKE`, `IS NULL`) instead of Python operators (`&&`, `||`, `!`).
   - For regex (`RLIKE`), ensure backslashes are properly escaped (e.g., `\\\\d` for digits).

2. **Rule Categorization**:
   - **Row-Level Rules (Completeness, Regex, Range)**: Add a violation column for each rule, e.g., `F.when(~(condition), 1).otherwise(0).alias("violation_{{rule_id}}")`.
   - **Dataset-Level Rules (Uniqueness, Correlation)**: Do NOT apply these row-by-row. Calculate them as aggregate counts or summary metrics separately.

3. **Handling Complex Rules**:
   - **Uniqueness**: Compare `df.count()` against `df.select(target_columns).distinct().count()`.
   - **Regex**: Ensure the regex patterns are compatible with the `RLIKE` function.

4. **Output Expectations**:
   - Provide a function `apply_dq_rules(df)` that returns two items:
     1. A DataFrame with audit columns appended.
     2. A summary dictionary/DataFrame showing the failure count for every `rule_id`.
   - Do NOT invent rules or ignore any provided rules.
   - Include all necessary imports.

   The code should explicitly write the unction that applies all the rules on the dataframe.

   ***The code should take the sample dataframe, the data quality rules and the metadata as arguments and apply the data quality rules to the dataframe.***

### OUTPUT FORMAT:
Comment the `rule_id` clearly above the code that implements it. Return only the Python code block between the markers

<CODE>
...
</CODE>
'''
    
    code = llm.invoke(prompt)

    print('✅✅ Code Generated ✅✅')

    return code.content