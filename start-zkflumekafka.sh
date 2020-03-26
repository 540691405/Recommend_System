#!/bin/bash

zkServer.sh start

#打开新的窗口

sleep 5
gnome-terminal -x bash -c "\
kafka-server-start.sh $KAFKA_HOME/config/server.properties \
"

#新窗口

sleep 5
gnome-terminal -x bash -c "\
flume-ng agent \
--name avro-memory-kafka \
--conf $FLUME_HOME/conf \
--conf-file /home/zzh/zzh/Program/Recommend_System/avro-memory-kafka.conf \
-Dflume.root.logger=INFO,console \
"

#新窗口
sleep 5
gnome-terminal -x bash -c " \
flume-ng agent \
--name exec-memory-avro \
--conf $FLUME_HOME/conf \
--conf-file /home/zzh/zzh/Program/Recommend_System/exec-memory-avro.conf \
-Dflume.root.logger=INFO,console \
"

sleep 5
gnome-terminal -x bash -c "\
kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic TencentRec \
"
