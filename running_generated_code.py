import traceback
import io
import contextlib

def execute_generated_code(spark, rules, sample_df, metadata, code_string):
    
    std_out = io.StringIO()

    result = {
        "success":True,
        "output":"",
        "error":None
    }

    local_scope = {
        "spark" : spark,
        "df" : sample_df,
        "metadata" : metadata,
        "quality_rules" : rules,
        "__name__" : "__main__"
    }

    try:
        with contextlib.redirect_stdout(std_out):
            exec(code_string, {}, local_scope)
    except Exception:
        result["success"] = False
        result["error"] = traceback.format_exc()

    output = std_out.getvalue()

    result["output"] = output   

    return result
