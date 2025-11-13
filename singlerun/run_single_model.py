import pySWATPlus
# Replace this with the path to your project's TxtInOut folder
txtinout_dir = r'D:\data_m\manitowoc_test30m\manitowoc_test30mv4\Scenarios\Default\TxtInOut'

txtinout_reader = pySWATPlus.TxtinoutReader(
    tio_dir=txtinout_dir
)
cpu_path = r'D:\data_m\manitowoc_test30m\manitowoc_test30mv4\Scenarios\Default\TxtInOut-1111'
cursim_dir = txtinout_reader.copy_required_files(
            sim_dir=cpu_path)
txtinout_reader.run_swat()