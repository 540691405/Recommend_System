import sys
import time
import math
import os

# os.environ['JAVA_HOME'] = '/usr/local/java/java-8-openjdk-amd64'
# os.environ['PYSPARK_PYTHON'] = '/home/zzh/app/python/python3.6.5/bin/python3'

from pyspark import RDD, Accumulator, SparkContext, SparkConf, StorageLevel
from pyspark.sql.window import Window

from pyspark.sql import functions
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import udf
from pyspark.sql.types import FloatType, Row
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

import PreProcessingUtils


class MyIO:
    # 所有IO方法父类
    spark: SparkSession = None

    def __init__(self, spark: SparkSession):
        self.spark = spark

    # Read Dataframe
    def write(self, table: str, df: DataFrame, mode: str = 'overwrite'):
        pass

    # write Dataframe
    def read(self, table: str):
        pass


class MyMySQLIO(MyIO):
    url: str = None
    auth_mysql = dict()

    def __init__(self, spark: SparkSession, url: str, auth_mysql: dict):
        MyIO.__init__(self, spark=spark)
        self.url = url
        self.auth_mysql['user'] = auth_mysql['user']
        self.auth_mysql['password'] = auth_mysql['password']

    def write(self, table: str, df: DataFrame, mode: str = 'overwrite'):
        if mode != 'overwrite' and mode != 'append':
            ex = Exception('Wrong write mode ,Please choice mode : overwrite/append')
            raise ex

        print("##### Writing To MySQL : table={tablename} #####".format(tablename=table))
        df.write.jdbc(url=self.url, table=table, mode=mode, properties=self.auth_mysql)
        print("##### Writing To MySQL Over : table={tablename} #####".format(tablename=table))

    def read(self, table: str):
        print("##### Reading from MySQL : table={tablename} #####".format(tablename=table))
        df = self.spark.read.format('jdbc') \
            .option('url', self.url) \
            .option('dbtable', table) \
            .option('user', self.auth_mysql['user']) \
            .option('password', self.auth_mysql['password']) \
            .load()
        # print("Read table :"+table+' from MySQL FAILED!!')
        print("##### Reading from MySQL Over : table={tablename} #####".format(tablename=table))
        return df


class MyParquetIO(MyIO):
    filepath: str = None

    def __init__(self, spark: SparkSession, filepath: str):
        MyIO.__init__(self, spark=spark)
        self.filepath = filepath

    def write(self, table: str, df: DataFrame, mode: str = 'overwrite'):
        if self.filepath[-1] != '/':
            filepath = self.filepath + '/'
        else:
            filepath = self.filepath

        print("##### Writing to parquet : table={tablename} #####".format(tablename=table))
        df.write.mode(mode).save(filepath + table + '.parquet')
        print("##### Writing to parquet Over : table={tablename} #####".format(tablename=table))

    def read(self, table: str):
        if self.filepath[-1] != '/':
            filepath = self.filepath + '/'
        else:
            filepath = self.filepath

        print("##### Reading from parquet : table={tablename} #####".format(tablename=table))
        df = self.spark.read.parquet(filepath + table + '.parquet')
        print("##### Reading from parquet Over : table={tablename} #####".format(tablename=table))
        return df


class MyRedisIO(MyIO):

    def __init__(self, spark: SparkSession):
        MyIO.__init__(self, spark=spark)

    def write(self, table: str, df: DataFrame, mode: str = 'overwrite'):
        print("##### Writing to Redis : table={tablename} #####".format(tablename=table))
        df.write.mode(mode).format("org.apache.spark.sql.redis").option("table", table).save()
        print("##### Writing to Redis Over : table={tablename} #####".format(tablename=table))

    def read(self, table: str):
        print("##### Reading from Redis : table={tablename} #####".format(tablename=table))
        df = self.spark.read.format("org.apache.spark.sql.redis").option("table", table).load()
        print("##### Reading from Redis Over : table={tablename} #####".format(tablename=table))
        return df


class Algorithm:
    spark: SparkSession = None
    K: int = None
    N: int = None
    result: list = None

    # list of result contain of DF

    def __init__(self, spark: SparkSession, K=K, N=N):
        self.spark = spark
        self.K = K
        self.N = N

    # 计算方法
    def calculate(self, myMySQLIO: MyMySQLIO, tempFileIO: MyIO):
        pass

    # 写入方法
    def save(self, myMySQLIO: MyMySQLIO, tempFileIO: MyIO):
        pass

    # 展示方法
    def show(self, myMySQLIO: MyMySQLIO, tempFileIO: MyIO):
        pass

    # 评估方法
    def evaluate(self, myMySQLIO: MyMySQLIO, tempFileIO: MyIO, klist: list, testRatio: float = 0.1):
        pass


class ItemCF(Algorithm):
    recordPath: str = None

    def __init__(self, spark: SparkSession, K, N, recordPath: str):
        Algorithm.__init__(self, spark=spark, K=K, N=N)
        self.recordPath = recordPath

    def itemCount(user_history_df: DataFrame):
        """
        计算itemcount 的 方法
        :param user_history_df: 用户历史记录DataFrame
        :return: itemcount 的Dataframe
        """
        itemCount_df = user_history_df.groupBy('item').agg({'rating': 'sum', '*': 'count'}) \
            .select('item', 'sum(rating)', 'count(1)') \
            .toDF('item', 'itemCount', 'count')
        # 计算出每个 item count
        # print('this is itemCount_df')
        # item_count_df.show(5)

        return itemCount_df

    def pairCount(user_history_df: DataFrame):
        """
        计算pairCount的方法
        :param user_history_df: 用户历史数据Dataframe
        :return: pairCount的Dataframe
        """

        # 用 udf 来解决 new column
        def co_rating_fun(rating_p, rating_q):
            if rating_p < rating_q:
                return rating_p
            else:
                return rating_q

        co_rating_udf = udf(lambda x, y: co_rating_fun(x, y), FloatType())
        # 定义udf 返回值是 int (spark 里)

        co_rating_df = user_history_df.toDF('user', 'item_p', 'rating_p') \
            .join(user_history_df.toDF('user', 'item_q', 'rating_q'), 'user') \
            .filter('item_p != item_q') \
            .withColumn('co_rating', co_rating_udf('rating_p', 'rating_q')).select('user', 'item_p', 'item_q',
                                                                                   'co_rating')
        # print('this is co_rating_df')
        # co_rating_df.show(5)

        pairCount_df = co_rating_df.groupBy('item_p', 'item_q') \
            .agg({'co_rating': 'sum', '*': 'count'}) \
            .toDF('item_p', 'item_q', 'nij', 'pairCount')
        # 给每个pair count 来agg count，用来记录n来realtime purning
        # print('this is pairCount_df')
        # pairCount_df.show(5)

        # 未记录nij的版本
        # pair_count_df = co_rating_df.groupBy('item_p', 'item_q').sum('co_rating').toDF('item_p', 'item_q', 'pairCount')
        # # pair count of item p
        # # print('this is pairCount_df')
        # # pairCount_df.show(5)

        return pairCount_df

    def similarity(itemCount_df: DataFrame, pairCount_df: DataFrame):
        """
        利用itemCount和pairCount计算相似度的方法
        :param itemCount_df: itemCount的Dataframe
        :param pairCount_df: pairCount的Dataframe
        :return: sim_df 相似度Dataframe
        """
        # 计算similarity

        itemCount_df = itemCount_df.select('item', 'itemCount')
        sim_df = pairCount_df.select('item_p', 'item_q', 'pairCount') \
            .join(itemCount_df.toDF('item_p', 'itemCount_p'), 'item_p') \
            .join(itemCount_df.toDF('item_q', 'itemCount_q'), 'item_q')
        # 得到item p and item q 's itemcont and pair count together

        sim_df = sim_df.withColumn('similarity',
                                   sim_df['pairCount'] / ((sim_df['itemCount_p'] * sim_df['itemCount_q']) ** 0.5)) \
            .select('item_p', 'item_q', 'similarity')
        # print('this is sim_df')
        # sim_df.show(5)

        return sim_df

    def topk_similarity(sim_df: DataFrame, k: int):
        """
        calculate the top k similarity of item p
        :param sim_df: item p and item q 's similarity dataframe
        :param k:  top k
        :return: top k sorted similarity of item p
        """

        sim_df = sim_df.select('item_p', 'item_q', 'similarity')

        # sim_df=sim_df.repartition('item_p')
        topk_sim_df = sim_df.withColumn('rank', functions.row_number().over(
            Window.partitionBy('item_p').orderBy(functions.desc('similarity'))))
        # sort the similarity

        topk_sim_df = topk_sim_df.filter(topk_sim_df['rank'] < k + 1)
        # get top k similarity

        # print('this is top k similarity of item p')
        # topk_sim_df.show(5)

        return topk_sim_df

    def rup(user_history_df: DataFrame, topk_sim_df: DataFrame):
        """
        calculate rup and  (user haven't seen)
        :param topk_sim_df: the top k similarity item of item p
        :param N: N item
        :return: rup
        """

        # 1: q haven seen of p's k similarity
        itemp_topk_df = user_history_df.join(
            topk_sim_df.select('item_p', 'item_q', 'similarity').toDF('item_p', 'item', 'similarity'), 'item')
        # userhistory: user item q rating
        # itemp_topk_df.show(5)

        # 2 calculate  equation 2
        rup_df = itemp_topk_df.withColumn('sim*ruq', itemp_topk_df['similarity'] * itemp_topk_df['rating']) \
            .groupBy('user', 'item_p').sum('sim*ruq', 'similarity').toDF('user', 'item_p', 'numerator', 'denominator')
        rup_df = rup_df.withColumn('rup', rup_df['numerator'] / rup_df['denominator']) \
            .select('user', 'item_p', 'rup')
        # rup_df.show(5)

        # 3 filter have seen
        rup_df = rup_df.join(user_history_df.toDF('user', 'item_p', 'rating'), ['user', 'item_p'], 'left_outer')
        rup_df = rup_df.filter(rup_df['rating'].isNull()).select('user', 'item_p', 'rup').toDF('user', 'item', 'rup')
        # rup_df.show(5)

        return rup_df

    def topN_rup(rup_df: DataFrame, N: int):
        """
        top k rup
        :param rup:  rup dataframe
        :return:  tok rup dataframe
        """
        # order by rup
        rup_df = rup_df.withColumn('rank', functions.row_number().over(
            Window.partitionBy('user').orderBy(functions.desc('rup'))))

        # get top N rup
        topN_rup_df = rup_df.filter(rup_df['rank'] < N + 1).select('user', 'item', 'rup', 'rank')
        # print('this is user_rup_topN(not see)')
        # rup_df.show(5)

        return topN_rup_df

    def recommend_N_for_user(user_history_df: DataFrame, topk_sim_df: DataFrame, N: int):
        """
        recommend N item for user (user haven't seen)
        :param topk_sim_df: the top k similarity item of item p
        :param N: N item
        :return:
        """

        # calculate rup (not seen)
        rup_df = ItemCF.rup(user_history_df=user_history_df, topk_sim_df=topk_sim_df)

        # topN rup to recommend
        topN_rup_df = ItemCF.topN_rup(rup_df=rup_df, N=N)

        recommend_df = topN_rup_df

        return recommend_df

    def itemCF(user_history_df: DataFrame, k: int, N: int):
        # get user history
        user_history_df = user_history_df.select('user', 'item', 'rating')

        # calculate itemCount
        itemCount_df = ItemCF.itemCount(user_history_df=user_history_df)

        # calculate pairCount
        pairCount_df = ItemCF.pairCount(user_history_df=user_history_df)

        # calculate sim
        sim_df = ItemCF.similarity(itemCount_df=itemCount_df, pairCount_df=pairCount_df)

        # calculate topk sim
        topk_sim_df = ItemCF.topk_similarity(sim_df=sim_df, k=k)

        # recommend for user
        recommend_df = ItemCF.recommend_N_for_user(user_history_df=user_history_df, topk_sim_df=topk_sim_df, N=N)

        return user_history_df, itemCount_df, pairCount_df, topk_sim_df, recommend_df

    def calculate(self, myMySQLIO: MyMySQLIO, tempFileIO: MyIO):
        '''
        itemcf算法，计算出相似度和topk相似度
                :param spark:sparkSession
                :param k: top k
                :param N: recommend N
                :return:
                '''
        print('starting ItemCF algorithum')
        start_time = time.time()

        # get user history
        user_history_df = myMySQLIO.read(self.recordPath).select('user', 'item', 'rating')

        user_history_df, itemCount_df, pairCount_df, topk_sim_df, recommend_df = ItemCF.itemCF(user_history_df, self.K,
                                                                                               self.N)

        # write four table to MySQL
        user_history_df.persist()
        pairCount_df.persist()
        topk_sim_df.persist()
        recommend_df.persist()

        myMySQLIO.write(df=user_history_df, table='user_history_df')
        myMySQLIO.write(df=itemCount_df, table='itemCount_df')
        myMySQLIO.write(df=pairCount_df, table='pairCount_df')
        myMySQLIO.write(df=topk_sim_df, table='topk_sim_df')
        pairCount_df.unpersist()
        myMySQLIO.write(df=recommend_df, table='recommend_df')
        recommend_df.unpersist()
        topk_sim_df.unpersist()
        user_history_df.unpersist()
        # 后面的会被级联unpersist

        # all time
        end_time = time.time()
        print('ItemCF algorithum 用了')
        print(end_time - start_time)

    def show(self, myMySQLIO: MyMySQLIO, tempFileIO: MyIO):
        '''
                itemcf算法，计算出相似度和topk相似度
                        :param spark:sparkSession
                        :param k: top k
                        :param N: recommend N
                        :return:
                        '''
        print('starting ItemCF algorithum')
        start_time = time.time()

        # get user history
        user_history_df = myMySQLIO.read(self.recordPath).select('user', 'item', 'rating')

        user_history_df, itemCount_df, pairCount_df, topk_sim_df, recommend_df = ItemCF.itemCF(user_history_df, self.K,
                                                                                               self.N)

        # show tables
        print('this is user_history_df')
        user_history_df.show(5)
        print('this is itemCount_df')
        itemCount_df.show(5)
        print('this is pairCount_df')
        pairCount_df.show(5)
        print('this is topk_sim_df')
        topk_sim_df.show(5)
        print('this is recommend_df')
        recommend_df.show(5)

        # all time
        end_time = time.time()
        print('ItemCF algorithum 用了')
        print(end_time - start_time)

    def evaluate(self, myMySQLIO: MyMySQLIO, tempFileIO: MyIO, klist: list, testRatio: float = 0.1):
        M = math.ceil(1 / (testRatio))
        # M为实验次数

        for i in range(M):

            Evaluation.splitRecord(self.spark, myMySQLIO, self.recordPath, test1Retio=testRatio / 2,
                                   test2Retio=testRatio / 2)
            # Random Split Data into train - test

            items_df = PreProcessingUtils.ReadMovies_df(self.spark).select('movieId').toDF('item')
            items_df = myMySQLIO.read('items').select('item')
            # get item's df

            train_df = myMySQLIO.read('train').select('user', 'item', 'rating')

            test1_df = myMySQLIO.read('test1').select('user', 'item', 'rating')
            test2_df = myMySQLIO.read('test2').select('user', 'item', 'rating')
            test_df = test1_df.union(test2_df)

            for k in klist:
                print('starting evaluate')
                start_time = time.time()

                user_history_df, itemCount_df, pairCount_df, topk_sim_df, recommend_df = ItemCF.itemCF(train_df,
                                                                                                       k, self.N)

                recall = Evaluation.Recall(recommend_df=recommend_df, test_df=test_df)
                precision = Evaluation.Precision(recommend_df=recommend_df, test_df=test_df)
                coverage = Evaluation.Coverage(items_df=items_df, recommend_df=recommend_df)
                popularity = Evaluation.Popularity(itemCount_df=itemCount_df, recommend_df=recommend_df)

                Evaluation.resultToMySQL(self.spark, myMySQLIO, k, self.N, recall, precision, coverage, popularity)
                # 将此次结果入库

                end_time = time.time()
                print('once evaluate use time')
                print(end_time - start_time)

        Evaluation.resultStatisticToMySQL(self.spark, myMySQLIO)
        # 统计结果

        return


class StreamingRecommend(ItemCF):
    interval: int = None

    def __init__(self, spark: SparkSession, K, N, recordPath: str, interval: int):
        ItemCF.__init__(self, spark=spark, K=K, N=N, recordPath=recordPath)
        self.interval = interval

    def refresh_accum(accum: Accumulator):
        '''
        用来自动增加累加器accum ，并且返回他的新值str和旧值str
        :param accum:
        :return: str

        '''

        # 记录文件名累加器(user history )
        if int(accum.value) == 100:
            accum.add(-90)
        accum.add(1)
        print(accum.value)

        oldacc_num_str = str((int(accum.value) - 1) % 10)
        # the num name of table where to read
        newacc_num_str = str((accum.value) % 10)
        # the num name of table where to write

        print('oldacc_num_str is ' + oldacc_num_str)
        print('newacc_num_str is ' + newacc_num_str)

        return oldacc_num_str, newacc_num_str

    def GetRatinglogDstreamFromKafka(ssc: StreamingContext, kafkaMetadataBrokerList: str):
        """
        get ratinglog DStream from kafka
        :param ssc: StreamingContext
        :return: ratinglog Dstream
        """
        kafkaStream = KafkaUtils.createDirectStream(ssc, topics=['TencentRec'],
                                                    kafkaParams={'metadata.broker.list': kafkaMetadataBrokerList})
        # paramaters: topic list ,  the metadata.broker.list:broker
        # get DStream

        # kafkaStream.pprint()
        # #说明得到的是一个tuple 只有两位，且第一位是'None'，第二位是数据
        # x[1] is data

        # ratinglog_DStream = kafkaStream.map(lambda x: x[1])  #get first
        # ratinglog_DStream = ratinglog_DStream.map(data_processing_utils.split_streaminglog) # pre process the log

        ratinglog_DStream = kafkaStream.map(lambda x: PreProcessingUtils.PreProcessKafkaStream(x[1]))

        return ratinglog_DStream

    def recommend_N_forActionUser(ratinglog_df: DataFrame, user_history_df: DataFrame, topk_sim_df: DataFrame,
                                  N: int):
        """
        recommend for action user
        :param ratinglog_df:  new tuple of <user , item , rating> dataframe
        :param user_history_df: user history dataframe
        :param topk_sim_df:  topk similarity dataframe
        :param N:  recommend N item
        :return: recommend dataframe
        """

        # all action user
        action_user_df = ratinglog_df.select('user').distinct()

        # all action user with his history
        action_user_history_df = action_user_df.join(user_history_df, 'user')

        # recommend for action user
        recommend_df = StreamingRecommend.recommend_N_for_user(user_history_df=action_user_history_df,
                                                               topk_sim_df=topk_sim_df, N=N)

        return recommend_df

    def streamingRecommend(ratinglog_df: DataFrame, user_history_df: DataFrame, topk_sim_df: DataFrame, N: int):

        ratinglog_df = ratinglog_df.select('user', 'item', 'rating')

        # update user history
        user_history_df = user_history_df.union(ratinglog_df)

        # recommend for user
        recommend_df = StreamingRecommend.recommend_N_forActionUser(ratinglog_df=ratinglog_df,
                                                                    user_history_df=user_history_df,
                                                                    topk_sim_df=topk_sim_df, N=N)

        return user_history_df, recommend_df

    def streamingRecommend_fun(rdd: RDD, spark: SparkSession, accum: Accumulator, N: int, myMySQLIO: MyMySQLIO,
                               tempFileIO: MyIO,
                               runtype: str = 'calculate'):

        if rdd.isEmpty() == False:
            start_time = time.time()

            # 计时器
            # 记录文件名累加器(user history )
            oldacc_num_str, newacc_num_str = StreamingRecommend.refresh_accum(accum=accum)

            # read old table user history
            if int(accum.value) == 1:
                # read from MySQL
                # if first run ,read from MySQL
                user_history_df = myMySQLIO.read('user_history_df')
                topk_sim_df = myMySQLIO.read('topk_sim_df')
                itemCount_df = myMySQLIO.read('itemCount_df')
            else:
                # read from temp saver
                user_history_df = tempFileIO.read('tempuser_history_df' + oldacc_num_str)
                topk_sim_df = tempFileIO.read('temptopk_sim_df')
                itemCount_df = tempFileIO.read('tempitemCount_df')

            ratinglog_df = spark.createDataFrame(rdd)
            ratinglog_df = ratinglog_df.select('UserID', 'MovieID', 'Rating').toDF('user', 'item', 'rating')
            # 后面多用

            user_history_df, recommend_df = StreamingRecommend.streamingRecommend(ratinglog_df, user_history_df,
                                                                                  topk_sim_df, N)

            if runtype == 'calculate' or runtype == 'evaluate':
                ratinglog_df.persist()
                user_history_df.persist()
                recommend_df.persist()
            elif runtype == 'show':
                ratinglog_df.persist()
                user_history_df.persist()
                recommend_df.persist()
                print('this is ratinglog_df')
                ratinglog_df.show(5)
                print('this is user_history_df')
                user_history_df.show(5)
                print('this is recommend_df')
                recommend_df.show(5)

                end_time = time.time()
                print('本次 streaming recommend calculate and read  用了')
                print(end_time - start_time)

                recommend_df.unpersist()
                user_history_df.unpersist()
                ratinglog_df.unpersist()

            end_time = time.time()
            print('本次 streaming recommend calculate and read  用了')
            print(end_time - start_time)

            # 写入文件或者库:
            # 默认写入redis
            if int(accum.value) == 1:
                # 第一次进行，顺便将相似度写入 temp saver
                tempFileIO.write(df=topk_sim_df, table='temptopk_sim_df')
                tempFileIO.write(df=itemCount_df, table='tempitemCount_df')
            tempFileIO.write(df=user_history_df, table='tempuser_history_df' + newacc_num_str)

            # 推荐结果写入MySQL
            myMySQLIO.write(df=recommend_df, table='recommend_result', mode='append')

            # if int(newacc_num_str) == 5:
            #     # 若写入文件10次了，也吧 user history 写入数据库：
            #

            time.sleep(1)
            # wait write terminal

            recommend_df.unpersist()
            user_history_df.unpersist()
            ratinglog_df.unpersist()
            # 级联unpersist 后面的df

            end_time = time.time()
            print('本次 streaming recommend only  用了')
            print(end_time - start_time)

        else:
            print("this batch DStream is Empty ! ")
        return

    def streamingRecommend_showfun(rdd: RDD, spark: SparkSession, accum: Accumulator, N: int, myMySQLIO: MyMySQLIO,
                                   tempFileIO: MyIO):

        if rdd.isEmpty() == False:
            start_time = time.time()

            # 计时器
            # 记录文件名累加器(user history )
            oldacc_num_str, newacc_num_str = StreamingRecommend.refresh_accum(accum=accum)

            # read old table user history
            if int(accum.value) == 1:
                # read from MySQL
                # if first run ,read from MySQL
                user_history_df = myMySQLIO.read('user_history_df')
                topk_sim_df = myMySQLIO.read('topk_sim_df')
                itemCount_df = myMySQLIO.read('itemCount_df')
            else:
                # read from temp saver
                user_history_df = tempFileIO.read('tempuser_history_df' + oldacc_num_str)
                topk_sim_df = tempFileIO.read('temptopk_sim_df')
                itemCount_df = tempFileIO.read('tempitemCount_df')

            ratinglog_df = spark.createDataFrame(rdd)
            ratinglog_df = ratinglog_df.select('UserID', 'MovieID', 'Rating').toDF('user', 'item', 'rating')
            # 后面多用

            user_history_df, recommend_df = StreamingRecommend.streamingRecommend(ratinglog_df, user_history_df,
                                                                                  topk_sim_df, N)

            ratinglog_df.persist()
            user_history_df.persist()
            recommend_df.persist()
            print('this is ratinglog_df')
            ratinglog_df.show(5)
            print('this is user_history_df')
            user_history_df.show(5)
            print('this is recommend_df')
            recommend_df.show(5)

            end_time = time.time()
            print('本次 streaming recommend calculate and read  用了')
            print(end_time - start_time)

            # 写入文件或者库:
            # 默认写入redis
            if int(accum.value) == 1:
                # 第一次进行，顺便将相似度写入 temp saver
                tempFileIO.write(df=topk_sim_df, table='temptopk_sim_df')
                tempFileIO.write(df=itemCount_df, table='tempitemCount_df')
            tempFileIO.write(df=user_history_df, table='tempuser_history_df' + newacc_num_str)

            # # 推荐结果写入MySQL
            # myMySQLIO.write(df=recommend_df, table='recommend_result', mode='append')

            recommend_df.unpersist()
            user_history_df.unpersist()
            ratinglog_df.unpersist()
            # 级联unpersist 后面的df

            end_time = time.time()
            print('本次 streaming recommend only  用了')
            print(end_time - start_time)

        else:
            print("this batch DStream is Empty ! ")
        return

    # TODO 完善
    def streamingRecommend_evaluatefun(rdd: RDD, spark: SparkSession, accum: Accumulator, N: int, myMySQLIO: MyMySQLIO,
                                       tempFileIO: MyIO):
        pass

    def calculate(self, myMySQLIO: MyMySQLIO, tempFileIO: MyIO):
        print('Starting streaming recommend!!')

        sc = self.spark.sparkContext
        accum = sc.accumulator(0)
        # set accumulator to regist what table is to read or write for user history

        ssc = StreamingContext(sc, self.interval)

        ratinglog_DStream = StreamingRecommend.GetRatinglogDstreamFromKafka(ssc, self.recordPath)
        # get DStream from log

        ratinglog_DStream.foreachRDD(
            lambda x: StreamingRecommend.streamingRecommend_fun(rdd=x, spark=self.spark, accum=accum, N=self.N,
                                                                myMySQLIO=myMySQLIO,
                                                                tempFileIO=tempFileIO))
        # for each batch recommend use calculated similarity

        ssc.start()
        ssc.awaitTermination()

    def show(self, myMySQLIO: MyMySQLIO, tempFileIO: MyIO):
        print('Starting streaming recommend!!')

        sc = self.spark.sparkContext
        accum = sc.accumulator(0)
        # set accumulator to regist what table is to read or write for user history

        ssc = StreamingContext(sc, self.interval)

        ratinglog_DStream = StreamingRecommend.GetRatinglogDstreamFromKafka(ssc, self.recordPath)
        # get DStream from log

        ratinglog_DStream.foreachRDD(
            lambda x: StreamingRecommend.streamingRecommend_showfun(rdd=x, spark=self.spark, accum=accum, N=self.N,
                                                                    myMySQLIO=myMySQLIO,
                                                                    tempFileIO=tempFileIO))
        # for each batch recommend use calculated similarity

        ssc.start()
        ssc.awaitTermination()

    # TODO 完善
    def evaluate(self, myMySQLIO: MyMySQLIO, tempFileIO: MyIO, klist: list, testRatio: float = 0.1):
        print('Starting streaming recommend!!')

        sc = self.spark.sparkContext
        accum = sc.accumulator(0)
        # set accumulator to regist what table is to read or write for user history

        ssc = StreamingContext(sc, self.interval)

        ratinglog_DStream = StreamingRecommend.GetRatinglogDstreamFromKafka(ssc, self.recordPath)
        # get DStream from log

        ratinglog_DStream.foreachRDD(
            lambda x: StreamingRecommend.streamingRecommend_evaluatefun(rdd=x, spark=self.spark, accum=accum, N=self.N,
                                                                        myMySQLIO=myMySQLIO,
                                                                        tempFileIO=tempFileIO))
        # for each batch recommend use calculated similarity

        ssc.start()
        ssc.awaitTermination()


class RealTimeUpdatItemCF(StreamingRecommend):

    def __init__(self, spark: SparkSession, K, N, recordPath: str, interval: int):
        StreamingRecommend.__init__(self, spark=spark, K=K, N=N, recordPath=recordPath, interval=interval
                                    )

    def itemCount_update(itemCount_df: DataFrame, ratinglog_df: DataFrame):
        """
        function to update itemCount
        :param itemCount_df:  old itemCount dataframe
        :param ratinglog_df:  new tuple of <user item rating> dataframe
        :return: updated itemCount dataframe
        """
        # calculate delta itemcount
        itemCount_delta_df = ratinglog_df.groupBy('item') \
            .agg({'rating': 'sum', '*': 'count'}) \
            .select('item', 'sum(rating)', 'count(1)') \
            .toDF('item', 'itemCount_delta', 'count_delta')

        # 计算出每个delta item count
        # print('this is item_count_deltadf')
        # item_count_deltadf.show(5)

        # update itemcount
        def update_fun(old, delta):
            if delta == None:
                return old
            elif old == None:
                return delta
            else:
                return old + delta

        update_udf = udf(lambda x, y: update_fun(x, y), FloatType())

        itemCount_df = itemCount_df.join(itemCount_delta_df, 'item', 'full_outer') \
            .withColumn('new_itemCount', update_udf('itemCount', 'itemCount_delta')) \
            .withColumn('new_count', update_udf('count', 'count_delta')) \
            .select('item', 'new_itemCount', 'new_count') \
            .toDF('item', 'itemCount', 'count')
        # #add delta to old itemcount
        # print('this is updated itemCount_df')
        # itemCount_df.show(5)

        return itemCount_df

    def pairCount_update(pairCount_df: DataFrame, user_history_df: DataFrame, ratinglog_df: DataFrame):
        """
        function of update pairCount
        :param pairCount_df: old pairCount dataframe
        :param user_history_df: user history dataframe
        :param ratinglog_df: new tuple of <user item rating> dataframe
        :return: updated pairCount dataframe
        """

        # calculate delta corating
        # 用 udf 来解决 new column
        def co_rating_fun(rating_p, rating_q):
            if rating_p < rating_q:
                return rating_p
            else:
                return rating_q

        co_rating_udf = udf(lambda x, y: co_rating_fun(x, y), FloatType())
        # 定义udf 返回值是 int (spark 里)

        # ratinglog_df=ratinglog_df.repartition('user')
        # 看看能否优化

        co_rating_delta_newold_df = ratinglog_df.toDF('user', 'item_p', 'rating_p') \
            .join(user_history_df.toDF('user', 'item_q', 'rating_q'), 'user') \
            .withColumn('co_rating', co_rating_udf('rating_p', 'rating_q')) \
            .select('user', 'item_p', 'item_q', 'co_rating')
        # # 计算corating(p,q)  (p为新rating的item)
        # print('this is new old  corating')
        # co_rating_delta_newold_df.show(5)

        co_rating_delta_oldnew_df = co_rating_delta_newold_df.toDF('user', 'item_q', 'item_p', 'co_rating') \
            .select('user', 'item_p', 'item_q', 'co_rating')
        # # 计算corating(p,q)  (p为历史的item)
        # # 为了union 的时候对应位置union ,所以要改列位置
        # print('this is old new  corating')
        # co_rating_delta_oldnew_df.show(5)

        co_rating_delta_newnew_df = ratinglog_df.toDF('user', 'item_p', 'rating_p') \
            .join(ratinglog_df.toDF('user', 'item_q', 'rating_q'), 'user') \
            .filter('item_p != item_q') \
            .withColumn('co_rating', co_rating_udf('rating_p', 'rating_q')) \
            .select('user', 'item_p', 'item_q', 'co_rating')
        # # 计算corating(p,q) (p,q都为新rating 的item
        # print('this is new new  corating')
        # co_rating_delta_newnew_df.show(5)

        co_rating_delta = co_rating_delta_newold_df.union(co_rating_delta_oldnew_df) \
            .union(co_rating_delta_newnew_df)

        # # union操作和集合的并集并不等价，因为它不会去除重复数据。
        # # union函数并不是按照列名和并得，而是按照位置合并的。即DataFrame的列名可以不相同，但对应位置的列将合并在一起。
        # print('this is all corating delta')
        # co_rating_delta.show(5)

        # update pairCount
        pairCount_delta_df = co_rating_delta.groupBy('item_p', 'item_q').sum('co_rating') \
            .toDF('item_p', 'item_q', 'pairCount_delta')

        # update pairCount udf
        def update_fun(old, delta):
            if delta == None:
                return old
            elif old == None:
                return delta
            else:
                return old + delta

        update_udf = udf(lambda x, y: update_fun(x, y), FloatType())

        # pairCount_df=pairCount_df.repartition('item_p','item_q')
        # 看优化
        pairCount_df = pairCount_df.join(pairCount_delta_df, ['item_p', 'item_q'], 'full_outer') \
            .withColumn('new_pairCount', update_udf('pairCount', 'pairCount_delta')) \
            .select('item_p', 'item_q', 'new_pairCount') \
            .toDF('item_p', 'item_q', 'pairCount')
        ## add delta to old paircount
        # print('this is pair_count_df')
        # pairCount_df.show(5)

        return pairCount_df

    def realTimeUpdateItemCF(itemCount_df: DataFrame, pairCount_df: DataFrame, user_history_df: DataFrame,
                             ratinglog_df: DataFrame, k: int, N: int):

        ratinglog_df = ratinglog_df.select('user', 'item', 'rating')
        # update itemCount
        itemCount_df = RealTimeUpdatItemCF.itemCount_update(itemCount_df=itemCount_df, ratinglog_df=ratinglog_df)

        # update pairCount
        pairCount_df = RealTimeUpdatItemCF.pairCount_update(pairCount_df=pairCount_df,
                                                            user_history_df=user_history_df,
                                                            ratinglog_df=ratinglog_df)

        # calculate new similarity
        sim_df = RealTimeUpdatItemCF.similarity(itemCount_df=itemCount_df, pairCount_df=pairCount_df)

        # topk similarity
        topk_sim_df = RealTimeUpdatItemCF.topk_similarity(sim_df=sim_df, k=k)

        # update user history
        user_history_df = user_history_df.union(ratinglog_df)

        # recommend N for user (abendon)
        # recommend_df = recommend_N_for_user(user_history_df=user_history_df, topk_sim_df=topk_sim_df, N=N)

        # recommend N for action user
        recommend_df = RealTimeUpdatItemCF.recommend_N_forActionUser(ratinglog_df=ratinglog_df,
                                                                     user_history_df=user_history_df,
                                                                     topk_sim_df=topk_sim_df, N=N)

        return itemCount_df, pairCount_df, user_history_df, recommend_df

    def realTimeRecommend_fun(rdd: RDD, spark: SparkSession, accum: Accumulator, k: int, N: int, myMySQLIO: MyMySQLIO,
                              tempFileIO: MyIO):
        """
        for each batch rdd of Dstream to Incremental Update the similarity
        :param rdd: each batch rdd of Dstream
        :param accum:  accumulator of sc to register the file name num
        :param k: top k simliarity
        :param N: recommend N
        :return:
        """

        if rdd.isEmpty() == False:
            start_time = time.time()
            # 计时器

            # 记录文件名累加器
            oldacc_num_str, newacc_num_str = RealTimeUpdatItemCF.refresh_accum(accum)

            # read old table
            if int(accum.value) == 1:
                # if first run ,read from MySQL
                user_history_df = myMySQLIO.read('user_history_df')
                itemCount_df = myMySQLIO.read('itemCount_df')
                pairCount_df = myMySQLIO.read('pairCount_df')
            else:
                # # not first run ,read from other file such as parquet with old num name file
                user_history_df = tempFileIO.read('tempuser_history_df' + oldacc_num_str)
                itemCount_df = tempFileIO.read('tempitemCount_df' + oldacc_num_str)
                pairCount_df = tempFileIO.read('temppairCount_df' + oldacc_num_str)

            # pre process the dstream rdd
            ratinglog_df = spark.createDataFrame(rdd)
            ratinglog_df = ratinglog_df.select('UserID', 'MovieID', 'Rating').toDF('user', 'item', 'rating')
            # 后面多用

            itemCount_df, pairCount_df, user_history_df, recommend_df = RealTimeUpdatItemCF.realTimeUpdateItemCF(
                itemCount_df, pairCount_df, user_history_df, ratinglog_df, k, N)

            ratinglog_df.persist()
            user_history_df.persist()
            itemCount_df.persist()
            pairCount_df.persist()
            recommend_df.persist()

            # 写入文件或者库:
            # 写入文件/redis
            tempFileIO.write(df=user_history_df, table='tempuser_history_df' + newacc_num_str)
            tempFileIO.write(df=itemCount_df, table='tempitemCount_df' + newacc_num_str)
            tempFileIO.write(df=pairCount_df, table='temppairCount_df' + newacc_num_str)

            # write recommend resutlt to MySQL
            myMySQLIO.write(df=recommend_df, table='recommend_result', mode='append')

            time.sleep(1)
            # wait write terminal

            recommend_df.unpersist()
            pairCount_df.unpersist()
            itemCount_df.unpersist()
            user_history_df.unpersist()
            ratinglog_df.unpersist()
            # 级联unpersist 后面的df

            end_time = time.time()
            print('本次 Incremental Update 用了')
            print(end_time - start_time)

        else:
            print("this batch DStream is Empty ! ")

    def realTimeRecommend_showfun(rdd: RDD, spark: SparkSession, accum: Accumulator, k: int, N: int,
                                  myMySQLIO: MyMySQLIO,
                                  tempFileIO: MyIO):
        """
        for each batch rdd of Dstream to Incremental Update the similarity
        :param rdd: each batch rdd of Dstream
        :param accum:  accumulator of sc to register the file name num
        :param k: top k simliarity
        :param N: recommend N
        :return:
        """

        if rdd.isEmpty() == False:
            start_time = time.time()
            # 计时器

            # 记录文件名累加器
            oldacc_num_str, newacc_num_str = RealTimeUpdatItemCF.refresh_accum(accum)

            # read old table
            if int(accum.value) == 1:
                # if first run ,read from MySQL
                user_history_df = myMySQLIO.read('user_history_df')
                itemCount_df = myMySQLIO.read('itemCount_df')
                pairCount_df = myMySQLIO.read('pairCount_df')
            else:
                # # not first run ,read from other file such as parquet with old num name file
                user_history_df = tempFileIO.read('tempuser_history_df' + oldacc_num_str)
                itemCount_df = tempFileIO.read('tempitemCount_df' + oldacc_num_str)
                pairCount_df = tempFileIO.read('temppairCount_df' + oldacc_num_str)

            # pre process the dstream rdd
            ratinglog_df = spark.createDataFrame(rdd)
            ratinglog_df = ratinglog_df.select('UserID', 'MovieID', 'Rating').toDF('user', 'item', 'rating')
            # 后面多用

            ratinglog_df.persist()
            user_history_df.persist()

            itemCount_df, pairCount_df, user_history_df, recommend_df = RealTimeUpdatItemCF.realTimeUpdateItemCF(
                itemCount_df, pairCount_df, user_history_df, ratinglog_df, k, N)

            recommend_df.unpersist()
            pairCount_df.unpersist()
            itemCount_df.unpersist()
            user_history_df.unpersist()
            ratinglog_df.unpersist()

            print('this is user_history_df')
            user_history_df.show(5)
            print('this is ratinglog_df')
            ratinglog_df.show(5)
            print('this is itemCount_df')
            itemCount_df.show(5)
            print('this is pairCount_df')
            pairCount_df.show(5)
            print('this is recommend_df')
            recommend_df.show(5)

            end_time = time.time()
            print('read and calculate use:')
            print(end_time - start_time)

            # 写入文件或者库:
            # 写入文件/redis

            tempFileIO.write(df=user_history_df, table='tempuser_history_df' + newacc_num_str)
            tempFileIO.write(df=itemCount_df, table='tempitemCount_df' + newacc_num_str)
            tempFileIO.write(df=pairCount_df, table='temppairCount_df' + newacc_num_str)

            # # write recommend resutlt to MySQL
            # myMySQLIO.write(df=recommend_df, table='recommend_result', mode='append')

            time.sleep(1)
            # wait write terminal

            recommend_df.unpersist()
            pairCount_df.unpersist()
            itemCount_df.unpersist()
            user_history_df.unpersist()
            ratinglog_df.unpersist()
            # 级联unpersist 后面的df

            end_time = time.time()
            print('本次 Incremental Update 用了')
            print(end_time - start_time)

        else:
            print("this batch DStream is Empty ! ")

    # TODO 完善
    def realTimeRecommend_evaluatefun(rdd: RDD, spark: SparkSession, accum: Accumulator, k: int, N: int,
                                      myMySQLIO: MyMySQLIO,
                                      tempFileIO: MyIO):
        pass

    def calculate(self, myMySQLIO: MyMySQLIO, tempFileIO: MyIO):
        """
                get log from kafka and update the similarity and recommend
                :param spark: sparkSession
                :return:
                """
        print('Starting streaming Real Time Recommend')

        sc = self.spark.sparkContext
        accum = sc.accumulator(0)
        # set accumulator to regist what table is to read or write

        ssc = StreamingContext(sc, self.interval)
        # 创建一个StreamingContext用于SparkStreaming，划分Batches时间间隔为interval s

        ratinglog_DStream = RealTimeUpdatItemCF.GetRatinglogDstreamFromKafka(ssc, self.recordPath)
        # get DStream from log

        # ratinglog_DStream.foreachRDD(lambda x: incremental_update_fun(rdd=x, accum=accum, k=k, N=N, typestr=typestr))
        ratinglog_DStream.foreachRDD(
            lambda x: RealTimeUpdatItemCF.realTimeRecommend_fun(rdd=x, spark=self.spark, accum=accum, k=self.K,
                                                                N=self.N,
                                                                myMySQLIO=myMySQLIO, tempFileIO=tempFileIO))

        ssc.start()  # Start the computation
        ssc.awaitTermination()  # Wait for the computation to terminate

    def show(self, myMySQLIO: MyMySQLIO, tempFileIO: MyIO):
        """
                        get log from kafka and update the similarity and recommend
                        :param spark: sparkSession
                        :return:
                        """
        print('Starting streaming Real Time Recommend')

        sc = self.spark.sparkContext
        accum = sc.accumulator(0)
        # set accumulator to regist what table is to read or write

        ssc = StreamingContext(sc, self.interval)
        # 创建一个StreamingContext用于SparkStreaming，划分Batches时间间隔为interval s

        ratinglog_DStream = RealTimeUpdatItemCF.GetRatinglogDstreamFromKafka(ssc, self.recordPath)
        # get DStream from log

        # ratinglog_DStream.foreachRDD(lambda x: incremental_update_fun(rdd=x, accum=accum, k=k, N=N, typestr=typestr))
        ratinglog_DStream.foreachRDD(
            lambda x: RealTimeUpdatItemCF.realTimeRecommend_showfun(rdd=x, spark=self.spark, accum=accum, k=self.K,
                                                                    N=self.N,
                                                                    myMySQLIO=myMySQLIO, tempFileIO=tempFileIO))

        ssc.start()  # Start the computation
        ssc.awaitTermination()  # Wait for the computation to terminate

    # TODO 完善
    def evaluate(self, myMySQLIO: MyMySQLIO, tempFileIO: MyIO, klist: list, testRatio: float = 0.1):
        """
                get log from kafka and update the similarity and recommend
            :param spark: sparkSession
                                :return:
                                """
        print('Starting streaming Real Time Recommend')

        sc = self.spark.sparkContext
        accum = sc.accumulator(0)
        # set accumulator to regist what table is to read or write

        ssc = StreamingContext(sc, self.interval)
        # 创建一个StreamingContext用于SparkStreaming，划分Batches时间间隔为interval s

        ratinglog_DStream = RealTimeUpdatItemCF.GetRatinglogDstreamFromKafka(ssc, self.recordPath)
        # get DStream from log

        # ratinglog_DStream.foreachRDD(lambda x: incremental_update_fun(rdd=x, accum=accum, k=k, N=N, typestr=typestr))
        ratinglog_DStream.foreachRDD(
            lambda x: RealTimeUpdatItemCF.realTimeRecommend_evaluatefun(rdd=x, spark=self.spark, accum=accum, k=self.K,
                                                                        N=self.N,
                                                                        myMySQLIO=myMySQLIO, tempFileIO=tempFileIO))

        ssc.start()  # Start the computation
        ssc.awaitTermination()  # Wait for the computation to terminate


class Evaluation:
    spark: SparkSession = None
    K: int = None
    N: int = None
    recordPath: str = None

    def __init__(self, spark: SparkSession, K: int, N: int, recordPath: str):
        self.spark = spark
        self.K = K
        self.N = N
        self.recordPath = recordPath

    def Recall(recommend_df: DataFrame, test_df: DataFrame):
        """
        召回率 Recall 计算，返回值
        :param recommend_df:
        :param test_df:
        :return:
        """
        recommend_df = recommend_df.select('user', 'item')
        test_df = test_df.select('user', 'item')

        RecommendIntersectionTestCount = float(recommend_df.join(test_df, ['user', 'item']).count())
        TestCount = float(test_df.count())

        recall = RecommendIntersectionTestCount / TestCount

        return recall

    def Precision(recommend_df: DataFrame, test_df: DataFrame):
        """
        准确率计算 Precision 返回计算值
        :param recommend_df:
        :param test_df:
        :return:
        """
        recommend_df = recommend_df.select('user', 'item')
        test_df = test_df.select('user', 'item')

        RecommendIntersectionTestCount = float(recommend_df.join(test_df, ['user', 'item']).count())
        RecommendCount = float(recommend_df.count())

        precision = RecommendIntersectionTestCount / RecommendCount

        return precision

    def Coverage(items_df: DataFrame, recommend_df: DataFrame):
        """
        覆盖率 Coverage 返回值
        :param items_df:
        :param recommend_df:
        :return:
        """

        I = float(items_df.count())
        RI = float(recommend_df.select('item').distinct().count())

        coverage = RI / I

        return coverage

    def Popularity(itemCount_df: DataFrame, recommend_df: DataFrame):
        """
        计算推荐的物品的平均 itemCount 来度量流行度

        :return:
        """
        recommend_df = recommend_df.select('user', 'item')
        itemCount_df = itemCount_df.select('item', 'count')

        RI = float(recommend_df.count())
        SumCount = recommend_df.join(itemCount_df, 'item') \
            .agg({'count': 'sum'}) \
            .collect()
        # 是一个Row 组成的list

        # print(recommend_df.count())
        # print(itemCount_df.count())
        # print(itemCount_df.agg({'count': 'sum'}).collect())
        # print(SumCount[0][0])

        SumCount = float(SumCount[0][0])

        popularity = SumCount / RI

        # #>>> df.agg({"age": "max"}).collect()
        #     [Row(max(age)=5)]

        return popularity

    # 以下为计算存储工具

    def resultToMySQL(spark: SparkSession, myMySQLIO: MyMySQLIO, k: int, N: int, recall: float, precision: float,
                      coverage: float, popularity: float):
        print('Recall is :')
        print(recall)
        print("Precision is :")
        print(precision)
        print('Coverage is :')
        print(coverage)
        print('Popularity is :')
        print(popularity)

        rowlist = [Row(recall=recall, precision=precision, coverage=coverage, popularity=popularity, k=k, N=N)]
        row_df = spark.createDataFrame(rowlist)
        myMySQLIO.write(df=row_df, table='resultKN', mode='append')

    def resultStatisticToMySQL(spark: SparkSession, myMySQLIO: MyMySQLIO):
        rows_df = myMySQLIO.read(table='resultKN')
        rowsMean_df = rows_df.groupBy(['k', 'N']).agg(
            {'recall': 'mean', 'precision': 'mean', 'coverage': 'mean', 'popularity': 'mean'})
        myMySQLIO.write(df=rowsMean_df, table='resultStatistic', mode='append')

    def splitRecord(spark: SparkSession, myMySQLIO: MyMySQLIO, recordPath: str, test1Retio: float = 0.05,
                    test2Retio: float = 0.05):
        print('starting split data from recordPath')

        ratings_df = myMySQLIO.read(recordPath)

        [train_df, test1_df, test2_df] = ratings_df.randomSplit([1 - test1Retio - test2Retio, test1Retio, test2Retio])

        myMySQLIO.write(df=train_df, table='train')
        myMySQLIO.write(df=test1_df, table='test1')
        myMySQLIO.write(df=test2_df, table='test2')
        print('starting split data from recordPath Over')


# def PreProcessingDataToFileAndMySQL(spark: SparkSession, myMySQLIO: MyMySQLIO, mode: str = 'nosplit',
#                                     test1Retio: float = 0.05, test2Retio: float = 0.05):
#     print('starting preprocessing of ratings.csv and write to MySQL')
#
#     ratingsfile = '/home/zzh/zzh/Program/Recommend_System/ml-latest-small/ratings.csv'
#
#     trainfile = '/home/zzh/zzh/Program/Recommend_System/ml-latest-small/train.csv'
#     test1file = '/home/zzh/zzh/Program/Recommend_System/ml-latest-small/test1.csv'
#     test2file = '/home/zzh/zzh/Program/Recommend_System/ml-latest-small/test2.csv'
#
#     if mode == 'nosplit':
#         ratings_df = PreProcessingUtils.ReadRatings_df(spark, 'file://' + ratingsfile)
#         myMySQLIO.write(table='ratings', df=ratings_df, mode='overwrite')
#         return
#     else:
#         datalist = PreProcessingUtils.ReadFile(ratingsfile)
#         if mode == 'debug':
#             # 用来调试模式的  划分 训练集，测试集，训练集
#             trainRatio = 0.001
#             test1Retio = 0.001
#             test2Retio = 0.001
#         elif mode == 'debug2':
#             trainRatio = 0.1
#             test1Retio = 0.1
#             test2Retio = 0.1
#
#     trainlist, test1list, test2list = PreProcessingUtils.RandomSplit(datalist, 1 - test1Retio - test2Retio, test1Retio,
#                                                                      test2Retio)
#     PreProcessingUtils.WriteFile(trainlist, trainfile, test1list, test1file, test2list, test2file)
#
#     print(trainfile)
#     train_df = PreProcessingUtils.ReadRatings_df(spark, 'file://' + trainfile)
#     test1_df = PreProcessingUtils.ReadRatings_df(spark, 'file://' + test1file)
#     test2_df = PreProcessingUtils.ReadRatings_df(spark, 'file://' + test2file)
#
#     myMySQLIO.write(df=train_df, table='train')
#     myMySQLIO.write(df=test1_df, table='test1')
#     myMySQLIO.write(df=test2_df, table='test2')
#
#
# def PreWriteMoviesCSVTOMySQL(spark: SparkSession, myMySQLIO: MyMySQLIO):
#     filepath = 'file:///home/zzh/zzh/Program/Recommend_System/ml-latest-small/movies.csv'
#     items_df = PreProcessingUtils.ReadMovies_df(spark, filepath).select('item')
#     myMySQLIO.write(df=items_df, table='items', mode='overwrite')


def Submit(args: sys.argv):
    usage = """
            Usage: RealTimeUpdateItemCF.py <mode> <algorithm> <pathOfRecord> <tempFileSaveMode> <tempFilePath>
                                            <MySQLPath> <MySQLDatabase> <MySQLUser> <MySQLPasswd> <N> [K] [interval] [kafkaMetadataBrokerList]
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

            """
    if len(args) < 12 or len(args) > 13:
        print(usage, file=sys.stderr)

    mode = args[1]
    algorithm = args[2]
    pathOfRecord = args[3]
    tempFIleSaveMode = args[4]
    tempFilePath = args[5]
    MySQLPath = args[6]
    MySQLDatabase = args[7]
    MySQLUser = args[8]
    MySQLPasswd = args[9]

    N = int(args[10])

    K = 20
    interval = 300
    # 默认的K和interval

    if len(args) > 11:
        K = int(args[11])
        if len(args) > 12:
            interval = int(args[12])

    # select tempfile save mode
    # myMySQLIO: MyMySQLIO, tempFileIO
    if tempFIleSaveMode == 'redis':
        spark = SparkSession \
            .builder \
            .appName(mode + algorithm) \
            .config("spark.redis.host", tempFilePath.split(':')[0]) \
            .config("spark.redis.port", tempFilePath.split(':')[1]) \
            .config('spark.sql.crossJoin.enabled', 'true') \
            .getOrCreate()
        tempFileIO = MyRedisIO(spark=spark)
    elif tempFIleSaveMode == 'parquet':
        spark = SparkSession \
            .builder \
            .appName(mode + algorithm) \
            .config('spark.sql.crossJoin.enabled', 'true') \
            .getOrCreate()
        tempFileIO = MyParquetIO(spark=spark, filepath=tempFilePath)
    else:
        print('please choose right parameter : <tempFilePath>')
        print(usage, file=sys.stderr)
        return

    # MySQL url
    url = 'jdbc:mysql://' + MySQLPath + '/' + MySQLDatabase + '?useSSL=false'
    auth_mysql = {'user': MySQLUser, 'password': MySQLPasswd}
    myMySQLIO = MyMySQLIO(spark=spark, url=url, auth_mysql=auth_mysql)
    # url = 'jdbc:mysql://Machine-zzh:3306/TencentRec?useSSL=false'
    # auth_mysql = {"user": "root", "password": "123456"}

    # mode
    # algorithm
    if algorithm == 'ItemCF':
        algo = ItemCF(spark, K=K, N=N, recordPath=pathOfRecord)
    elif algorithm == 'RealTimeUpdateItemCF':
        algo = RealTimeUpdatItemCF(spark=spark, K=K, N=N, recordPath=pathOfRecord, interval=interval)

    elif algorithm == 'StreamingRecommend':
        algo = StreamingRecommend(spark=spark, K=K, N=N, recordPath=pathOfRecord, interval=interval)
    else:
        print('please choose right parameter : <algorithm>')
        print(usage, file=sys.stderr)
        spark.stop()
        return

    if mode == 'normal':
        algo.calculate(myMySQLIO=myMySQLIO, tempFileIO=tempFileIO)
    elif mode == 'show':
        algo.show(myMySQLIO=myMySQLIO, tempFileIO=tempFileIO)
    elif mode == 'evaluate':
        klist = [5, 10, 15, 20, 30, 40, 50, 70, 100]
        if algorithm != 'ItemCF':
            print('暂时未开发ItemCF算法外的evaluate method')
            return
        algo.evaluate(myMySQLIO=myMySQLIO, tempFileIO=tempFileIO, klist=klist, testRatio=0.1)

    else:
        print("please choose right parameter : <mode>")
        print(usage, file=sys.stderr)
        spark.stop()
        return

    spark.stop()


if __name__ == '__main__':
    Submit(sys.argv)

    # url = 'jdbc:mysql://Machine-zzh:3306/TencentRec?useSSL=false'
    # table = ''
    # auth_mysql = {"user": "root", "password": "123456"}
    # spark = SparkSession.builder.master('local[4]') \
    #     .appName('RealTimeUpdateItemCF') \
    #     .config("spark.redis.host", "Machine-zzh") \
    #     .config("spark.redis.port", "6379") \
    #     .config('spark.sql.crossJoin.enabled', 'true') \
    #     .getOrCreate()
    # myMySQLIO = MyMySQLIO(spark, url, auth_mysql)
    #
    # PreWriteMoviesCSVTOMySQL(spark,myMySQLIO)

    # modestr = 'nosplit'
    # PreProcessingDataToFileAndMySQL(spark, myMySQLIO,
    #                                 recordPath='/home/zzh/zzh/Program/Recommend_System/ml-latest-small/ratings.csv',
    #                                 mode=modestr, test1Retio=0.05, test2Retio=0.05)
    # spark.stop()
    # klist=[5,10,15,20,30,40,50,70,100]

    # 为了支持笛卡尔积
    # spark.conf.set("spark.sql.crossJoin.enabled", "true")
    # .enableHiveSupport() \
    # .getOrCreate()
