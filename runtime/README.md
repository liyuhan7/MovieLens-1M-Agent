# Hadoop 容器运行环境

此目录只用于准备环境；**不含数据清洗、评分或报告的实现**。

- `Dockerfile` 使用本机已有 `wurstmeister/kafka:latest`（OpenJDK 11）及 Apache Hadoop 3.3.6 官方二进制包构建镜像 `movielens-hadoop:3.3.6`。
- `hadoop-3.3.6.tar.gz` 约 697 MiB，按需从 Apache 镜像下载；不建议纳入代码仓库或提交成果。
- 默认 `file:///` 文件系统与 `local` MapReduce 框架，在容器中执行真实 Hadoop MapReduce 作业，而不是 Docker 多节点 HDFS/YARN 集群。正式迭代实施时必须注明该执行模式，不能称为分布式集群。

检查环境：

```powershell
# 在项目根目录（ml-1m 所在目录）运行；使用工作区完整路径即可支持中文目录
$root = (Get-Location).Path
docker build -t movielens-hadoop:3.3.6 -f ml-1m/runtime/Dockerfile ml-1m/runtime
docker run --rm -v "${root}/ml-1m:/project:ro" movielens-hadoop:3.3.6 'hadoop version && hadoop jar "$HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-examples-3.3.6.jar"'
```

**不要**在构建完成前将数据检查结果称作 Hadoop 作业结果：环境验证只验证 Hadoop 本身可启动。
