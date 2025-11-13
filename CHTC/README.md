+ 在WSL中，将TxtInOut文件夹压缩：`tar -czf TxtInOut.tar.gz TxtInOut`
+ 在WSL中，将swatplus镜像转换为Apptainer格式的单个文件：`apptainer build swatplus_61.0.2.11.sif docker-daemon://crazyzlj/swatplus:debian-61.0.2.11`
+ 在WSL中，将swatplus_utility镜像转换为Apptainer格式的单个文件：`apptainer build swatplus_utility-0.1.sif docker-daemon://crazyzlj/swatplus_utility:0.1-debian-61.0.2.11`
+ 登录CHTC，创建目录`/home/lzhu267/swatplus_proj/single_exec`
+ 将`TxtInOut.tar.gz`和`swatplus_61.0.2.11.sif`上传到该文件夹
+ 创建一个名为 run_swatplus.sh 的文件，上传至CHTC，并用`chmod +x run_swatplus.sh`修改权限
+ 创建一个名为 test_swatplus.sub 的文件，上传至CHTC
+ 提交作业：`condor_submit test_swatplus.sub`,系统会返回一个作业ID，例如：Submitting job(s). 1 job(s) submitted to cluster 2549232.
+ 监控作业状态：`condor_q`,你会看到你的作业状态从 I (Idle, 排队中) 变为 R (Running, 运行中)。当它从列表中消失时，表示已运行完毕。
+ 检查结果：如果成功：会在 `/home/lzhu267/swatplus_proj/single_exec` 文件夹中看到HTCondor传回的三个文件：basin_wb_day.txt, channel_sd_day.txt 和 channel_sd_mon.txt,还会看到 swatplus_test.log, swatplus_test.out 和 swatplus_test.err 文件; 如果失败：首先检查 swatplus_test.err 文件，它会包含程序运行的错误信息。其次检查 swatplus_test.out，它会包含 run_swatplus.sh 脚本中 echo 语句的输出，这对于调试解压或执行步骤非常有用。

+ 提交DAG作业：condor_submit_dag controller.dag，也可以把DAGMan产生的log文件都放到一个文件夹内：condor_submit_dag -outfile_dir dag_logs controller.dag

+ 查看某作业ID的实时错误输出：condor_tail -stderr <你的作业ID>

docker build 