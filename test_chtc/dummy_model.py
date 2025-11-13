import time
import os

print("Starting dummy model...")

if not os.path.exists("TxtInOut/file.cio"):
    print("Error: TxtInOut/file.cio not found!")
    exit(1)
print("Found TxtInOut/file.cio.")

# 2. 读取输入参数
with open('params.txt', 'r') as f:
    val1 = float(f.readline())
    val2 = float(f.readline())
result = val1 + val2
print(f"Calculation: {val1} + {val2} = {result}")

# 3. 模拟耗时
print("Simulating work for 10 seconds...")
time.sleep(10)

# 4. 写入模型输出
with open('model_output.txt', 'w') as f:
    f.write(str(result))
print("Dummy model finished.")