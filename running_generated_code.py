import subprocess
import tempfile
import sys
import os

def execute_generated_code(rules, sample_df, metadata, code_string):
    with tempfile.NamedTemporaryFile(
        mode="w",
        suffix=".py",
        delete=False
    ) as f:

        temp_file = f.name

        f.write("""
import traceback

try:
""")

        # indent generated code
        for line in code_string.splitlines():
            f.write(f"    {line}\n")

        f.write("""
except Exception:
    traceback.print_exc()
    raise
""")

    result = subprocess.run(
        [sys.executable, temp_file],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True
    )

    os.remove(temp_file)

    if result.returncode != 0:
        return {
            "success": False,
            "stderr": result.stderr,
            "stdout": result.stdout
        }

    return {
        "success": True,
        "stdout": result.stdout
    }
