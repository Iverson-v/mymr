#!/bin/bash

# 获取当前脚本的目录
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

# 定义JAR包和配置文件的路径
JAR_PATH="$SCRIPT_DIR/mymr-1.0.jar"
MASTER_CONF="$SCRIPT_DIR/master.conf"
SLAVE_CONF="$SCRIPT_DIR/slave.conf"
URL_TOP_N_PATH="$SCRIPT_DIR/urltopn.conf"

# 启动本地的 ThriftServer
java -cp $JAR_PATH com.ksc.wordcount.thrift.server.ThriftServer $MASTER_CONF &
sleep 2
# 读取slave.conf并通过ssh在对应的服务器上启动Executor
while IFS= read -r line; do
    # 跳过以#开头的行
    [[ $line = \#* ]] && continue

    # 使用ssh在对应的服务器上启动Executor
    ssh -f $(echo $line | awk '{print $1}') "nohup java -cp $JAR_PATH com.ksc.wordcount.worker.Executor $MASTER_CONF $line &"


done < "$SLAVE_CONF"

#给thriftServer发送请求
java -cp $JAR_PATH com.ksc.wordcount.thrift.client.SendApplicationClient $URL_TOP_N_PATH &

