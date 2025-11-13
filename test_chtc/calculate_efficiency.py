print("Starting efficiency calculation...")
with open('model_output.txt', 'r') as f:
    model_result = float(f.read())
efficiency = 1.0 / (1.0 + model_result)
print(f"Calculated efficiency: {efficiency}")
with open('efficiency.txt', 'w') as f:
    f.write(str(efficiency))
print("Efficiency calculation finished.")