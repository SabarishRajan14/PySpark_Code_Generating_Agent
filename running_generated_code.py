import subprocess
import tempfile
from pyspark.sql import DataFrame
import os

def execute_generated_code(rules:dict, sample_df : DataFrame ,metadata:dict, code_string:str):

    with tempfile.NamedTemporaryFile(delete=False, suffix=".py") as tmp:
        tmp.write(code_string.encode('utf-8'))
        tmp_path = tmp.name

    arg = [rules, sample_df, metadata]

    try:

        content = ['python', tmp_path]+arg

        result = subprocess.run(
            content,
            capture_output=True,
            text=True,
            timeout=300
        )

        return {
            "success":result.returncode == 0,
            "stdout":result.stdout,
            "stderr":result.stderr
        }
    except subprocess.TimeoutExpired:
        return {'sucsess':False, "stderr":'Execution timed out!!!'}
    except Exception as e:
        return {'success':False, "stderr":e}
    finally:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)