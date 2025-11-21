import os
import pySWATPlus

# Replace this with the path to your project's TxtInOut folder
txtinout_dir = r'../TxtInOut'

txtinout_reader = pySWATPlus.TxtinoutReader(
    tio_dir=txtinout_dir
)

cpu_path = r'../TxtInOut_Copy'
if not os.path.exists(cpu_path):
    os.makedirs(cpu_path)
cursim_dir = txtinout_reader.copy_required_files(
            sim_dir=cpu_path)

txtinout_reader.run_swat()