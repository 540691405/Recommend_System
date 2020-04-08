# Recommend_System
第一个实时itemcf

ItemcF文件夹下，
RealTimeUpdatItemCF.py
是可以提交集群运行的文件

可以选择通过参数选择模式如下：

  normal ：运行算法
  
  show ： 仅展示每次计算中间结果和结果
  
  evaluate ： 对不同K值以及你的数据集进行自动划分评估
  
  
可以通过参数选择算法：

  ItemCF ： 普通的ItemCF ，计算全体用户推荐列表
  
  StreamingRecommend ： 普通使用已有Topk的结果实时为当前行为用户推荐结果，需要先运行一次ItemCF
  
  RealTimeUpdateItemCF  ： 实时更新ItemCF并用新相似度为当前行为用户推荐结果，需要先运行一次ItemCF
  
剩余参数可以参考：

       Usage: RealTimeUpdateItemCF.py <mode> <algorithm> <pathOfRecord> <tempFileSaveMode> <tempFilePath> <MySQLPath> <MySQLDatabase>           <MySQLUser> <MySQLPasswd> <N> [K] [interval] [kafkaMetadataBrokerList]
            <> : must      []:optional

            <mode> parameter :
                normal : run normal algorithm
                show : for just show result
                evaluate : for evaluation algorithum


            <algorithm>  parameter:
                ItemCF : normal ItemCF
                RealTimeUpdateItemCF : a spark streaming algorithum , need run after ItemCF
                StreamingRecommend : just recommend , need run after ItemCF

            <pathOfRecord> parameter :
                if mode is ItemCF , is the table name  of userhistory in mysql
                if mode is Recommend Only or RealTimeUpdateItemCF ,
                    is the path of Kafka 's metadata.broker.list  such as  :   host:port

            <tempFileSaveMode> parameter:
                redis : save in redis
                parquet : save in parquet


            <tempFIlePath> :
                redis : host:port
                parquet : path

            <MySQLPath>  parameter : your MySQL host:port
            <MySQLDatabase> parameter : choose database to write
            <MySQLUser> parameter : your MySQL user
            <MySQLPasswd> parameter : your MySQL password
            <N>  parameter : recommend list length

            [K] optional parameter : TopK 's K
            [interval] optional parameter : interval of SparkStreaming interval




