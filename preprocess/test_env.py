import sys
import pandas
import htcondor
import deap

print("--- Python Environment Test ---")
print(f"Python Executable Path: {sys.executable}")

print("\nSuccessfully imported:")
print(f"  - pandas (version: {pandas.__version__})")
print(f"  - deap (version: {deap.__version__})")
print(f"  - HTCondor (version: {htcondor.__version__})")

if "/opt/conda/envs/pyswatplus_util/" in sys.executable:
    print("\nSUCCESS: Running inside the 'pyswatplus_util' Conda environment.")
else:
    print("\nFAILURE: Not running inside the correct Conda environment.")

print("---------------------------------")