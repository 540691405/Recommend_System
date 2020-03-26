from pyspark import RDD, Accumulator
from pyspark.sql.window import Window

from pyspark.sql import Row, functions
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import udf
from pyspark.sql.types import BooleanType, IntegerType, NullType, FloatType
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

import time
import math
from ItemCF import data_processing_utils

import data_processing_utils
import random_split

usersfile = 'file:///home/zzh/zzh/Program/Recommend_System/ItemCF/ml-1m/users.dat'
moviesfile = 'file:///home/zzh/zzh/Program/Recommend_System/ItemCF/ml-1m/movies.dat'
ratingsfile = 'file:///home/zzh/zzh/Program/Recommend_System/ItemCF/ml-1m/ratings.dat'

url = 'jdbc:mysql://Machine-zzh:3306/TencentRec?useSSL=false'
table = ''
auth_mysql = {"user": "root", "password": "123456"}


def get_matrix_of_similarity(spark: SparkSession, ratings_df: DataFrame):
    """
    calculate the similarity matrix
    :param spark: SparkSession
    :param ratings_df: ratings dataframe(record)
    :return: the dataframe of similarity matrix of item
    """

    # ratings_df.show(1)
    ratings_df = ratings_df.select('UserID', 'MovieID', 'Rating').toDF('user', 'item', 'rating')
    ratings_df.persist()

    # TODO 计算user behavior 的记录
    # 暂时用 ratings_df来替代
    user_history_df = ratings_df

    # TODO 计算item count
    item_count_df = ratings_df.groupBy('item').sum('rating').toDF('item', 'item_count')
    # 计算出每个 item count
    item_count_df.persist()

    # 以后要用，所以持久化
    # print('this is item_count_df')
    # item_count_df.show()

    # TODO 计算pari count
    # 用 udf 来解决 new column
    def co_rating_fun(rating_p, rating_q):
        if rating_p < rating_q:
            return rating_p
        else:
            return rating_q

    co_rating_udf = udf(lambda x, y: co_rating_fun(x, y), IntegerType())
    # 定义udf 返回值是 int (spark 里)

    co_rating_df = ratings_df.toDF('user', 'item_p', 'rating_p') \
        .join(ratings_df.toDF('user', 'item_q', 'rating_q'), 'user') \
        .filter('item_p != item_q') \
        .withColumn('co_rating', co_rating_udf('rating_p', 'rating_q')).select('user', 'item_p', 'item_q', 'co_rating')
    # print('this is co_rating_df')
    # co_rating_df.show()

    pair_count_df = co_rating_df.groupBy('item_p', 'item_q') \
        .agg({'co_rating': 'sum', '*': 'count'}) \
        .toDF('item_p', 'item_q', 'nij', 'pair_count')
    # 给每个pair count 来agg count，用来记录n来realtime purning
    pair_count_df.persist()
    # # 由于后面有用，所以持久化
    print('this is pair_count_df')
    pair_count_df.show(5)
    # pair_count_df = co_rating_df.groupBy('item_p', 'item_q').sum('co_rating').toDF('item_p', 'item_q', 'pair_count')
    # # pair count of item p
    # pair_count_df.persist()
    # # # 由于后面有用，所以持久化
    # # print('this is pair_count_df')
    # # pair_count_df.show()

    # TODO 计算sim(p,q)

    sim_df = pair_count_df.select('item_p', 'item_q', 'pair_count') \
        .join(item_count_df.toDF('item_p', 'item_count_p'), 'item_p') \
        .join(item_count_df.toDF('item_q', 'item_count_q'), 'item_q')
    # 得到item p and item q 's itemcont and pair count together

    sim_df = sim_df.withColumn('similarity',
                               sim_df['pair_count'] / ((sim_df['item_count_p'] * sim_df['item_count_q']) ** 0.5)) \
        .select('item_p', 'item_q', 'similarity')
    # 计算得 similarity  (由于TencentRec公式，已经范围至[0,1])
    sim_df.persist()
    # print('this is similarity_df')
    # sim_df.show()

    return user_history_df, item_count_df, pair_count_df, sim_df


def get_topk_similarity(sim_df: DataFrame, k: int):
    """
    calculate the top k similarity of item p
    :param sim_df: item p and item q 's similarity dataframe
    :param k:  top k
    :return: top k sorted similarity of item p
    """

    #################??
    sim_df = sim_df.select('item_p', 'item_q', 'similarity')

    topk_sim_df = sim_df.withColumn('rank', functions.row_number().over(
        Window.partitionBy('item_p').orderBy(functions.desc('similarity'))))
    # sort the similarity

    topk_sim_df = topk_sim_df.filter(topk_sim_df['rank'] < k + 1)
    # get top k similarity

    print('this is top k similarity of item p')
    topk_sim_df.persist()
    topk_sim_df.show()

    return topk_sim_df


def recommend_N_for_user(user_history_df: DataFrame, topk_sim_df: DataFrame, N: int):
    """
    recommend N item for user (user haven't seen)
    :param topk_sim_df: the top k similarity item of item p
    :param N: N item
    :return:
    """

    # TODO: 修改成正确公式，并先求子集

    # 1 : subtract:
    # 2: q haven seen of p's k similarity
    itemp_topk_df = user_history_df.join(
        topk_sim_df.select('item_p', 'item_q', 'similarity').toDF('item_p', 'item', 'similarity'), 'item')
    # userhistory: user item q rating
    # itemp_topk_df.show()

    # 3 calculate  equation 2
    rup_df = itemp_topk_df.withColumn('sim*ruq', itemp_topk_df['similarity'] * itemp_topk_df['rating']) \
        .groupBy('user', 'item_p').sum('sim*ruq', 'similarity').toDF('user', 'item_p', 'numerator', 'denominator')
    rup_df = rup_df.withColumn('rup', rup_df['numerator'] / rup_df['denominator']) \
        .select('user', 'item_p', 'rup')
    # rup_df.show()

    # filter have seen
    rup_df = rup_df.join(user_history_df.toDF('user', 'item_p', 'rating'), ['user', 'item_p'], 'left_outer')
    # rup_df.show()
    rup_df = rup_df.filter(rup_df['rating'].isNull()).select('user', 'item_p', 'rup').toDF('user', 'item', 'rup')
    # rup_df.show()

    # order by rup
    rup_df = rup_df.withColumn('rank', functions.row_number().over(
        Window.partitionBy('user').orderBy(functions.desc('rup'))))
    # get top N rup
    rup_df = rup_df.filter(rup_df['rank'] < N + 1)
    print('this is user_rup_topN(not see)')
    rup_df.persist()
    # rup_df.show()

    return rup_df


# def recommend_N_for_user(user_history_df: DataFrame, topk_sim_df: DataFrame, N: int):
#     """
#     recommend N item for user (user haven't seen)
#     :param topk_sim_df: the top k similarity item of item p
#     :param N: N item
#     :return:
#     """
#
#     user_interest_df = user_history_df.join(
#         topk_sim_df.select('item_p', 'item_q', 'similarity').toDF('item', 'item_q', 'similarity'), 'item')
#     # join get user's item top k similarity
#
#     user_interest_df = user_interest_df.groupBy('user', 'item_q').sum('similarity').toDF('user', 'item', 'interest')
#     # calculate the interest
#     user_interest_df.persist()
#
#     # TODO filter had seen by user (成功！！)
#     not_see_item_df = user_interest_df.select('user', 'item').subtract(user_history_df.select('user', 'item'))
#     # find what item not seen
#     user_interest_df = not_see_item_df.join(user_interest_df, ['user', 'item'])  # 注意此时join 多条件是传入一个List
#     # filter out not seen's item and its interest
#
#     user_interest_df = user_interest_df.withColumn('rank', functions.row_number().over(
#         Window.partitionBy('user').orderBy(functions.desc('interest'))))
#     # sort interest
#     user_interest_df = user_interest_df.filter(user_interest_df['rank'] < N + 1)
#     # get top N command
#
#     print('this is user_interest_topN(not see)')
#     user_interest_df.show()
#
#     return user_interest_df

def read_from_MySQL(spark: SparkSession, table: str):
    df = spark.read.format('jdbc') \
        .option('url', url) \
        .option('dbtable', table) \
        .option('user', auth_mysql['user']) \
        .option('password', auth_mysql['password']) \
        .load()
    # print("Read table :"+table+' from MySQL FAILED!!')
    return df


def write_to_MySQL(spark: SparkSession, df: DataFrame, table: str):
    df.write.jdbc(url=url, table=table, mode='overwrite', properties=auth_mysql)


def read_from_parquet(spark: SparkSession, table: str):
    df = spark.read.parquet(
        'file:///home/zzh/zzh/Program/Recommend_System/temp_tables/' + table + '.parquet')
    return df


def write_to_parquet(spark: SparkSession, df: DataFrame, table: str):
    df.write.mode('overwrite').save('file:///home/zzh/zzh/Program/Recommend_System/temp_tables/' + table + '.parquet')


def write_to_redis(spark: SparkSession, df: DataFrame, table: str):
    df.write.mode("overwrite").format("org.apache.spark.sql.redis").option("table", table).save()


def read_from_redis(spark: SparkSession, table: str):
    df = spark.read.format("org.apache.spark.sql.redis").option("table", table).load()
    return df


def test_redis(spark: SparkSession):
    df = data_processing_utils.get_users_df(spark)
    df.show(5)
    write_to_redis(spark, df.select("UserID", "Age"), 'users_df')

    redis_df = read_from_redis(spark, 'users_df')
    redis_df.show(5)


##########################################################################

# it is all old
def test(spark):
    ssc = StreamingContext(spark.sparkContext, 10)
    # 创建一个StreamingContext用于SparkStreaming，划分Batches时间间隔为10s

    kafkaStream = KafkaUtils.createDirectStream(ssc, topics=['TencentRec'],
                                                kafkaParams={'metadata.broker.list': 'Machine-zzh:9092'})
    # paramaters: topic list ,  the metadata.broker.list:broker
    # get DStream

    # kafkaStream.pprint()
    # #说明得到的是一个tuple 只有两位，且第一位是'None'，第二位是数据

    ratinglog_DStream = kafkaStream.map(lambda x: x[1])
    ratinglog_DStream = ratinglog_DStream.map(data_processing_utils.split_streaminglog)


# TODO 实现实时更新
def update_from_streaming(spark: SparkSession):
    """
    get log from kafka and update the similarity
    :param spark: sparkSession
    :return:
    """

    # accum = sc.accumulator(0)
    # >>> accum
    # Accumulator<id=0, value=0>
    #
    # >>> sc.parallelize([1, 2, 3, 4]).foreach(lambda x: accum.add(x))
    # ...
    # 10/09/29 18:41:08 INFO SparkContext: Tasks finished in 0.317106 s
    #
    # >>> accum.value
    # 10

    sc = spark.sparkContext
    accum = sc.accumulator(0)

    ssc = StreamingContext(sc, 100)
    # 创建一个StreamingContext用于SparkStreaming，划分Batches时间间隔为10s

    kafkaStream = KafkaUtils.createDirectStream(ssc, topics=['TencentRec'],
                                                kafkaParams={'metadata.broker.list': 'Machine-zzh:9092'})
    # paramaters: topic list ,  the metadata.broker.list:broker
    # get DStream

    # kafkaStream.pprint()
    # #说明得到的是一个tuple 只有两位，且第一位是'None'，第二位是数据

    ratinglog_DStream = kafkaStream.map(lambda x: x[1])
    ratinglog_DStream = ratinglog_DStream.map(data_processing_utils.split_streaminglog)

    # TODO 解决写的时候已经开始读，数据不一致问题
    # TODO 备用解决办法：sparkstreaming用来更新temp userhistory,再用来update ,最后写回 userhistory
    # TODO 最好解决办法：一次streaming 就能计算完成一次similarity
    # TODO 备用方法2：用rdd 方法解决：updatestatebykey
    # TODO 由于lazy性质所以旧table不能直接写，要写到新table里去
    # TODO 解决新入数据是旧的评过分的数据的冲突问题

    def DstreamToDF(rdd):

        if rdd.isEmpty() == False:
            start_time = time.time()

            # 记录文件名累加器
            accum.add(1)
            print(accum.value)
            oldacc_num_str = str(int(accum.value) - 1)
            newacc_num_str = str(accum.value)

            # TODO read from MySQL
            # print("reading from MySQL############################################################################")
            # user_history_df = read_from_MySQL(spark, 'user_history_df')
            # item_count_df = read_from_MySQL(spark, 'item_count_df')
            # pair_count_df = read_from_MySQL(spark, 'pair_count_df')
            # sim_df = read_from_MySQL(spark, 'sim_df')
            # print(
            #     "reading from MySQL over ############################################################################")

            # print("reading from MySQL############################################################################")
            # user_history_df = read_from_MySQL(spark, 'tempuser_history_df')
            # item_count_df = read_from_MySQL(spark, 'tempitem_count_df')
            # pair_count_df = read_from_MySQL(spark, 'temppair_count_df')
            # sim_df = read_from_MySQL(spark, 'tempsim_df')
            # print(
            #     "reading from MySQL over ############################################################################")

            if int(newacc_num_str) == 1:
                print(
                    "reading from parquet############################################################################")
                user_history_df = read_from_parquet(spark, 'tempuser_history_df')
                item_count_df = read_from_parquet(spark, 'tempitem_count_df')
                pair_count_df = read_from_parquet(spark, 'temppair_count_df')
                sim_df = read_from_parquet(spark, 'tempsim_df')
                print(
                    "reading from parquet over ############################################################################")

            else:
                print(
                    "reading from parquet############################################################################")
                user_history_df = read_from_parquet(spark, 'tempuser_history_df' + oldacc_num_str)
                item_count_df = read_from_parquet(spark, 'tempitem_count_df' + oldacc_num_str)
                pair_count_df = read_from_parquet(spark, 'temppair_count_df' + oldacc_num_str)
                sim_df = read_from_parquet(spark, 'tempsim_df' + oldacc_num_str)
                print(
                    "reading from parquet over ############################################################################")

            ratinglog_df = spark.createDataFrame(rdd)
            ratinglog_df = ratinglog_df.select('UserID', 'MovieID', 'Rating').toDF('user', 'item', 'rating')

            # TODO 计算delta itemcount
            item_count_deltadf = ratinglog_df.groupBy('item').sum('rating').toDF('item', 'item_count')
            # 计算出每个 item count
            # item_count_deltadf.persist()
            # 以后要用，所以持久化
            print('this is item_count_deltadf')
            item_count_deltadf.show(5)

            # TODO 计算delta corating for pari count
            # 用 udf 来解决 new column
            def co_rating_fun(rating_p, rating_q):
                # TODO null
                # if rating_p == None:
                #     return rating_q
                # elif rating_q == None:
                #     return rating_p
                if rating_p < rating_q:
                    return rating_p
                else:
                    return rating_q

            co_rating_udf = udf(lambda x, y: co_rating_fun(x, y), IntegerType())
            # 定义udf 返回值是 int (spark 里)

            co_rating_delta_newold_df = ratinglog_df.toDF('user', 'item_p', 'rating_p') \
                .join(user_history_df.toDF('user', 'item_q', 'rating_q'), 'user') \
                .withColumn('co_rating', co_rating_udf('rating_p', 'rating_q')).select('user', 'item_p', 'item_q',
                                                                                       'co_rating')
            # # 计算corating(p,q)  (p为新rating的item)
            # #用 new 的leftouter join 不然会失去新的值
            print('this is new old  corating')
            co_rating_delta_newold_df.show(5)

            co_rating_delta_oldnew_df = co_rating_delta_newold_df.toDF('user', 'item_q', 'item_p', 'co_rating').select(
                'user', 'item_p', 'item_q', 'co_rating')
            # # 计算corating(p,q)  (p为历史的item)
            # # # 为了union 的时候对应位置union ,所以要改列位置
            print('this is old new  corating')
            co_rating_delta_oldnew_df.show(5)

            co_rating_delta_newnew_df = ratinglog_df.toDF('user', 'item_p', 'rating_p') \
                .join(ratinglog_df.toDF('user', 'item_q', 'rating_q'), 'user') \
                .filter('item_p != item_q') \
                .withColumn('co_rating', co_rating_udf('rating_p', 'rating_q')).select('user', 'item_p', 'item_q',
                                                                                       'co_rating')
            # # 计算corating(p,q) (p,q都为新rating 的item
            print('this is new new  corating')
            co_rating_delta_newnew_df.show(5)
            #
            co_rating_delta = co_rating_delta_newold_df.union(co_rating_delta_oldnew_df).union(
                co_rating_delta_newnew_df)
            # # union操作和集合的并集并不等价，因为它不会去除重复数据。
            # # union函数并不是按照列名和并得，而是按照位置合并的。即DataFrame的列名可以不相同，但对应位置的列将合并在一起。
            print('this is all corating delta')
            co_rating_delta.show(5)

            # print(co_rating_delta.count())

            # TODO : update itemcount
            def update_fun(old, delta):
                if delta == None:
                    return old
                elif old == None:
                    return delta
                else:
                    return old + delta

            update_udf = udf(lambda x, y: update_fun(x, y), IntegerType())

            item_count_df = item_count_df.join(item_count_deltadf.toDF('item', 'delta'), 'item', 'full_outer') \
                .withColumn('new_itemcount', update_udf('item_count', 'delta')).select('item', 'new_itemcount').toDF(
                'item', 'item_count')
            # #add delta to old itemcount
            print('this is updated item_count_df')
            item_count_df.show(5)
            item_count_df.persist()

            # TODO : update pair count

            deltapair_count_df = co_rating_delta.groupBy('item_p', 'item_q').sum('co_rating').toDF('item_p', 'item_q',
                                                                                                   'delta')
            pair_count_df = pair_count_df.join(deltapair_count_df, ['item_p', 'item_q'], 'full_outer') \
                .withColumn('new_paircount', update_udf('pair_count', 'delta')).select('item_p', 'item_q',
                                                                                       'new_paircount').toDF('item_p',
                                                                                                             'item_q',
                                                                                                             'pair_count')
            ## add delta to old paircount
            # temppair_count_df.filter(temppair_count_df['item_p']=='1193').show(100)
            # pair_count_df.filter(pair_count_df['item_p']=='1193').show(100)
            print('this is pair_count_df')
            pair_count_df.show(5)
            pair_count_df.persist()

            # TODO 计算new sim(p,q)
            sim_df = pair_count_df.join(item_count_df.toDF('item_p', 'item_count_p'), 'item_p') \
                .join(item_count_df.toDF('item_q', 'item_count_q'), 'item_q')
            # 得到item p and item q 's itemcont and pair count together

            sim_df = sim_df.withColumn('similarity',
                                       sim_df['pair_count'] / (
                                               (sim_df['item_count_p'] * sim_df['item_count_q']) ** 0.5)) \
                .select('item_p', 'item_q', 'similarity')
            # 计算得 similarity  (由于TencentRec公式，已经范围至[0,1]
            sim_df.persist()
            print('this is similarity_df')
            sim_df.show(5)

            # TODO 更新user history
            user_history_df = user_history_df.union(ratinglog_df)
            user_history_df.persist()

            # #TODO 写入MySql
            # user_history_df.show(1)
            # item_count_df.show(1)
            # pair_count_df.show(1)
            # sim_df.show(1)

            # print("writing To MySQL ############################################################################")
            # write_to_MySQL(spark, df=user_history_df, table='user_history_df')
            # write_to_MySQL(spark, df=item_count_df, table='item_count_df')
            #             # write_to_MySQL(spark, df=pair_count_df, table='pair_count_df')
            #             # write_to_MySQL(spark, df=sim_df, table='sim_df')
            # print("writing To MySQL over ############################################################################")

            # print("writing To MySQL ############################################################################")
            # write_to_MySQL(spark, df=user_history_df, table='tempuser_history_df')
            # write_to_MySQL(spark, df=item_count_df, table='tempitem_count_df')
            # write_to_MySQL(spark, df=pair_count_df, table='temppair_count_df')
            # write_to_MySQL(spark, df=sim_df, table='tempsim_df')
            # print("writing To MySQL over ############################################################################")

            print("writing To parquet ############################################################################")
            write_to_parquet(spark, df=user_history_df, table='tempuser_history_df' + newacc_num_str)
            write_to_parquet(spark, df=item_count_df, table='tempitem_count_df' + newacc_num_str)
            write_to_parquet(spark, df=pair_count_df, table='temppair_count_df' + newacc_num_str)
            write_to_parquet(spark, df=sim_df, table='tempsim_df' + newacc_num_str)
            print(
                "writing To parquet over ############################################################################")

            end_time = time.time()
            print('用了')
            print(end_time - start_time)

            # time.sleep(20)

        else:
            pass

    # # 将Dstream 的每个小批次的rdd变成dataframe
    # def test(rdd):
    #     if rdd.isEmpty() == False:
    #         accum.add(1)
    #         print(accum.value)

    # ratinglog_DStream.foreachRDD(test)

    ratinglog_DStream.foreachRDD(DstreamToDF)
    ssc.start()  # Start the computation
    ssc.awaitTermination()  # Wait for the computation to terminate


def old(spark: SparkSession):
    # get three tables as spark's dataframe
    users_df = data_processing_utils.get_users_df(spark)
    movies_df = data_processing_utils.get_movies_df(spark)
    # ratings_df=data_processing_utils.get_ratings_df(spark)  #all rating's record
    ratings_df = data_processing_utils.get_small_ratings_df(spark)  # get small record

    user_history_df, item_count_df, pair_count_df, sim_df = get_matrix_of_similarity(spark, ratings_df)

    print("writing To MySQL ############################################################################")
    write_to_MySQL(spark, df=user_history_df, table='user_history_df')
    write_to_MySQL(spark, df=item_count_df, table='item_count_df')
    write_to_MySQL(spark, df=pair_count_df, table='pair_count_df')
    write_to_MySQL(spark, df=sim_df, table='sim_df')
    print("writing To MySQL over ############################################################################")

    print("reading from MySQL############################################################################")
    user_history_df = read_from_MySQL(spark, 'user_history_df')
    item_count_df = read_from_MySQL(spark, 'item_count_df')
    pair_count_df = read_from_MySQL(spark, 'pair_count_df')
    sim_df = read_from_MySQL(spark, 'sim_df')
    print("reading from MySQL over ############################################################################")

    topk_sim_df = get_topk_similarity(sim_df, k=20)
    user_interest_df = recommend_N_for_user(user_history_df, topk_sim_df, 10)

    print("writing To MySQL ############################################################################")
    write_to_MySQL(spark, df=user_history_df, table='tempuser_history_df')
    write_to_MySQL(spark, df=item_count_df, table='tempitem_count_df')
    write_to_MySQL(spark, df=pair_count_df, table='temppair_count_df')
    write_to_MySQL(spark, df=sim_df, table='tempsim_df')
    print("writing To MySQL over ############################################################################")

    print("writing To parquet ############################################################################")
    write_to_parquet(spark, df=user_history_df, table='tempuser_history_df')
    write_to_parquet(spark, df=item_count_df, table='tempitem_count_df')
    write_to_parquet(spark, df=pair_count_df, table='temppair_count_df')
    write_to_parquet(spark, df=sim_df, table='tempsim_df')
    print("writing To parquet over ############################################################################")

    print("reading from parquet############################################################################")
    user_history_df = read_from_parquet(spark, 'tempuser_history_df')
    # item_count_df = read_from_parquet(spark, 'tempitem_count_df')
    item_count_df = read_from_parquet(spark, 'tempitem_count_df')
    pair_count_df = read_from_parquet(spark, 'temppair_count_df')
    sim_df = read_from_parquet(spark, 'tempsim_df')
    print("reading from parquet over ############################################################################")

    user_history_df.show(5)
    item_count_df.show(5)
    pair_count_df.show(5)
    sim_df.show(5)

    update_from_streaming(spark)
    test(spark)


#######################################################################################

def split_data(spark: SparkSession, mode: str = 'debug2'):
    print('随机切分rating的method ')
    filename = '/home/zzh/zzh/Program/Recommend_System/ItemCF/ml-1m/ratings.dat'
    trainfile = '/home/zzh/zzh/Program/Recommend_System/ItemCF/ml-1m/train_ratings.dat'
    testfile = '/home/zzh/zzh/Program/Recommend_System/ItemCF/ml-1m/test_ratings.dat'

    datalist = random_split.readfile(filename)

    if mode == 'debug':
        # 用来调试模式的  划分 训练集，测试集，训练集
        trainlist, testlist = random_split.random_split(datalist, 0.001, 0.001)
    elif mode == 'debug2':
        trainlist, testlist = random_split.random_split(datalist, 0.1, 0.1)
    else:
        trainlist, testlist = random_split.random_split(datalist, 0.8, 0.2)

    random_split.writefile(trainlist, trainfile, testlist, testfile)
    ratings_df = data_processing_utils.get_ratings_df(spark, 'file://' + trainfile)

    # if mode == 'debug':
    #     # 用来调试模式的  划分 训练集，测试集，训练集
    #     [train_ratings_df, test_ratings_df, test2_ratings_df] = ratings_df.randomSplit([0.001, 0.001, 0.998])
    #     # write four table to MySQL
    # elif mode == 'debug2':
    #     [train_ratings_df, test_ratings_df, test2_ratings_df] = ratings_df.randomSplit([0.1, 0.001, 0.899])
    #     # write four table to MySQL
    # else:
    #     [train_ratings_df, test_ratings_df, test2_ratings_df] = ratings_df.randomSplit([0.8, 0.1, 0.1])
    #     # 划分 训练集，测试集，训练集

    print("writing To MySQL ############################################################################")
    write_to_MySQL(spark, df=ratings_df, table='ratings_df')
    print("writing To MySQL over #######################################################################")


def item_cf(spark: SparkSession, k: int):
    '''
    itemcf算法，计算出相似度和topk相似度
    :param spark:sparkSession
    :param k: top k
    :return:
    '''
    print('starting itemcf algorithum')
    start_time = time.time()

    ## get three tables as spark's dataframe
    # users_df = data_processing_utils.get_users_df(spark)
    # movies_df = data_processing_utils.get_movies_df(spark)
    # # ratings_df=data_processing_utils.get_ratings_df(spark)  #all rating's record
    # ratings_df = data_processing_utils.get_small_ratings_df(spark)  # get small record
    ratings_df = read_from_MySQL(spark, 'ratings_df')

    user_history_df, item_count_df, pair_count_df, sim_df = get_matrix_of_similarity(spark, ratings_df)
    # get user history dataframe ,  item count dataframe , pair count dataframe , similarity dataframe
    topk_sim_df = get_topk_similarity(sim_df=sim_df, k=k)

    user_history_df.show(5)
    item_count_df.show(5)
    pair_count_df.show(5)
    sim_df.show(5)
    topk_sim_df.show(5)

    end_time = time.time()
    print('itemcf algorithum clculate  用了')
    print(end_time - start_time)

    # write four table to MySQL
    print("writing To MySQL ############################################################################")
    write_to_MySQL(spark, df=user_history_df, table='user_history_df')
    write_to_MySQL(spark, df=item_count_df, table='item_count_df')
    write_to_MySQL(spark, df=pair_count_df, table='pair_count_df')
    write_to_MySQL(spark, df=sim_df, table='sim_df')
    write_to_MySQL(spark, df=topk_sim_df, table='topk_sim_df')
    print("writing To MySQL over #######################################################################")

    user_history_df.unpersist()
    # 后面的会被级联unpersist

    end_time = time.time()
    print('itemcf algorithum 用了')
    print(end_time - start_time)


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


def get_ratinglogDstream_from_Kafka(ssc: StreamingContext):
    """
    get ratinglog DStream from kafka
    :param ssc: StreamingContext
    :return: ratinglog Dstream
    """
    kafkaStream = KafkaUtils.createDirectStream(ssc, topics=['TencentRec'],
                                                kafkaParams={'metadata.broker.list': 'Machine-zzh:9092'})
    # paramaters: topic list ,  the metadata.broker.list:broker
    # get DStream

    # kafkaStream.pprint()
    # #说明得到的是一个tuple 只有两位，且第一位是'None'，第二位是数据
    # x[1] is data

    # ratinglog_DStream = kafkaStream.map(lambda x: x[1])  #get first
    # ratinglog_DStream = ratinglog_DStream.map(data_processing_utils.split_streaminglog) # pre process the log

    ratinglog_DStream = kafkaStream.map(lambda x: data_processing_utils.split_streaminglog(x[1]))

    return ratinglog_DStream


def incremental_update_fun(rdd: RDD, accum: Accumulator, k: int, N: int, typestr: str):
    """
    for each batch rdd of Dstream to incremental update the similarity
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
        oldacc_num_str, newacc_num_str = refresh_accum(accum)

        # read old table
        if int(accum.value) == 1:
            # if first run ,read from MySQL
            print("reading from MySQL############################################################################")
            user_history_df = read_from_MySQL(spark, 'user_history_df')
            item_count_df = read_from_MySQL(spark, 'item_count_df')
            pair_count_df = read_from_MySQL(spark, 'pair_count_df')
            # sim_df = read_from_MySQL(spark, 'sim_df')
            print(
                "reading from MySQL over ############################################################################")
        else:
            # # not first run ,read from other file such as parquet with old num name file
            if typestr == 'redis':
                print(
                    "reading from Redis############################################################################")
                user_history_df = read_from_redis(spark, 'tempuser_history_df' + oldacc_num_str)
                item_count_df = read_from_redis(spark, 'tempitem_count_df' + oldacc_num_str)
                pair_count_df = read_from_redis(spark, 'temppair_count_df' + oldacc_num_str)
                # sim_df = read_from_redis(spark, 'tempsim_df' + oldacc_num_str)
                print(
                    "reading from Redis over ############################################################################")
            elif typestr == 'parquet':
                print(
                    "reading from parquet############################################################################")
                user_history_df = read_from_parquet(spark, 'tempuser_history_df' + oldacc_num_str)
                item_count_df = read_from_parquet(spark, 'tempitem_count_df' + oldacc_num_str)
                pair_count_df = read_from_parquet(spark, 'temppair_count_df' + oldacc_num_str)
                # sim_df = read_from_parquet(spark, 'tempsim_df' + oldacc_num_str)
                print(
                    "reading from parquet over ############################################################################")
                ##not first run ,read from other file such as redis table with old num name file

        ratinglog_df = spark.createDataFrame(rdd)
        ratinglog_df = ratinglog_df.select('UserID', 'MovieID', 'Rating').toDF('user', 'item', 'rating')
        ratinglog_df.persist()
        # 后面多用

        # TODO 计算delta itemcount
        item_count_deltadf = ratinglog_df.groupBy('item').sum('rating').toDF('item', 'item_count')

        # 计算出每个 item count
        # item_count_deltadf.persist()
        # 中间结果不需要persist (后面只用了一次)
        # print('this is item_count_deltadf')
        # item_count_deltadf.show(5)

        # TODO 计算delta corating for pari count
        # 用 udf 来解决 new column
        def co_rating_fun(rating_p, rating_q):
            # TODO null
            # if rating_p == None:
            #     return rating_q
            # elif rating_q == None:
            #     return rating_p
            if rating_p < rating_q:
                return rating_p
            else:
                return rating_q

        co_rating_udf = udf(lambda x, y: co_rating_fun(x, y), IntegerType())
        # 定义udf 返回值是 int (spark 里)

        co_rating_delta_newold_df = ratinglog_df.toDF('user', 'item_p', 'rating_p') \
            .join(user_history_df.toDF('user', 'item_q', 'rating_q'), 'user') \
            .withColumn('co_rating', co_rating_udf('rating_p', 'rating_q')) \
            .select('user', 'item_p', 'item_q', 'co_rating')
        # # 计算corating(p,q)  (p为新rating的item)
        # #用 new 的leftouter join 不然会失去新的值
        # print('this is new old  corating')
        # co_rating_delta_newold_df.show(5)

        co_rating_delta_oldnew_df = co_rating_delta_newold_df.toDF('user', 'item_q', 'item_p', 'co_rating') \
            .select('user', 'item_p', 'item_q', 'co_rating')
        # # 计算corating(p,q)  (p为历史的item)
        # # # 为了union 的时候对应位置union ,所以要改列位置
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
        #
        co_rating_delta = co_rating_delta_newold_df.union(co_rating_delta_oldnew_df) \
            .union(co_rating_delta_newnew_df)

        # # union操作和集合的并集并不等价，因为它不会去除重复数据。
        # # union函数并不是按照列名和并得，而是按照位置合并的。即DataFrame的列名可以不相同，但对应位置的列将合并在一起。
        # print('this is all corating delta')
        # co_rating_delta.show(5)
        # 中间结果不用persist（只后面用了一次)

        # print(co_rating_delta.count())

        # TODO : update itemcount
        def update_fun(old, delta):
            if delta == None:
                return old
            elif old == None:
                return delta
            else:
                return old + delta

        update_udf = udf(lambda x, y: update_fun(x, y), IntegerType())

        item_count_df = item_count_df.join(item_count_deltadf.toDF('item', 'delta'), 'item', 'full_outer') \
            .withColumn('new_itemcount', update_udf('item_count', 'delta')) \
            .select('item', 'new_itemcount') \
            .toDF('item', 'item_count')
        # #add delta to old itemcount
        print('this is updated item_count_df')
        item_count_df.show(5)
        item_count_df.persist()
        # need to write need persist

        # TODO : update pair count

        deltapair_count_df = co_rating_delta.groupBy('item_p', 'item_q').sum('co_rating') \
            .toDF('item_p', 'item_q', 'delta')

        pair_count_df = pair_count_df.join(deltapair_count_df, ['item_p', 'item_q'], 'full_outer') \
            .withColumn('new_paircount', update_udf('pair_count', 'delta')) \
            .select('item_p', 'item_q', 'new_paircount') \
            .toDF('item_p', 'item_q', 'pair_count')
        ## add delta to old paircount
        # temppair_count_df.filter(temppair_count_df['item_p']=='1193').show(100)
        # pair_count_df.filter(pair_count_df['item_p']=='1193').show(100)
        print('this is pair_count_df')
        pair_count_df.show(5)
        pair_count_df.persist()

        # TODO 计算new sim(p,q)
        sim_df = pair_count_df.join(item_count_df.toDF('item_p', 'item_count_p'), 'item_p') \
            .join(item_count_df.toDF('item_q', 'item_count_q'), 'item_q')
        # 得到item p and item q 's itemcont and pair count together

        sim_df = sim_df.withColumn('similarity',
                                   sim_df['pair_count'] / (
                                           (sim_df['item_count_p'] * sim_df['item_count_q']) ** 0.5)) \
            .select('item_p', 'item_q', 'similarity')
        # 计算得 similarity  (由于TencentRec公式，已经范围至[0,1]
        sim_df.persist()
        print('this is similarity_df')
        sim_df.show(5)

        # TODO 更新user history
        user_history_df = user_history_df.union(ratinglog_df)
        user_history_df.show(5)
        user_history_df.persist()

        # # TODO recommend n for user use top k
        topk_sim_df = get_topk_similarity(sim_df, k=k)
        user_interest_df = recommend_N_for_user(user_history_df, topk_sim_df, N)
        # user_interest_df = recommend_N_for_user(ratinglog_df, topk_sim_df, N)
        user_interest_df.persist()

        endforcal = time.time()
        print('calculate use:')
        print(endforcal - start_time)

        # #TODO 写入文件或者库:
        # # 写入文件/redis
        if typestr == 'redis':
            print("writing To Redis ############################################################################")
            write_to_redis(spark, df=user_history_df, table='tempuser_history_df' + newacc_num_str)
            write_to_redis(spark, df=item_count_df, table='tempitem_count_df' + newacc_num_str)
            write_to_redis(spark, df=pair_count_df, table='temppair_count_df' + newacc_num_str)
            # write_to_redis(spark, df=sim_df, table='tempsim_df' + newacc_num_str)
            write_to_redis(spark, df=user_interest_df, table='tempuser_interest_df' + newacc_num_str)
            print(
                "writing To Redis over ############################################################################")
        elif typestr == 'parquet':
            print("writing To parquet ############################################################################")
            write_to_parquet(spark, df=user_history_df, table='tempuser_history_df' + newacc_num_str)
            write_to_parquet(spark, df=item_count_df, table='tempitem_count_df' + newacc_num_str)
            write_to_parquet(spark, df=pair_count_df, table='temppair_count_df' + newacc_num_str)
            # write_to_parquet(spark, df=sim_df, table='tempsim_df' + newacc_num_str)
            write_to_parquet(spark, df=user_history_df, table='tempuser_interest_df' + newacc_num_str)
            print(
                "writing To parquet over ############################################################################")

        # 默认写入redis

        if int(newacc_num_str) == 5:
            # 若写入文件10次了，也写入数据库：
            print("writing To MySQL ############################################################################")
            write_to_MySQL(spark, df=user_history_df, table='tempuser_history_df')
            write_to_MySQL(spark, df=item_count_df, table='tempitem_count_df')
            write_to_MySQL(spark, df=pair_count_df, table='temppair_count_df')
            # write_to_MySQL(spark, df=sim_df, table='tempsim_df')
            write_to_MySQL(spark, df=user_interest_df, table='tempuser_interest_df')
            print("writing To MySQL over ############################################################################")

        time.sleep(1)
        # wait write terminal

        ratinglog_df.unpersist()
        # 级联unpersist 后面的df

        end_time = time.time()
        print('本次 Incremental Update 用了')
        print(end_time - start_time)

    else:
        pass


def incremental_update_fun_withpurning(rdd: RDD, accum: Accumulator, k: int, N: int, delta: float, typestr: str):
    """
    for each batch rdd of Dstream to incremental update the similarity
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
        oldacc_num_str, newacc_num_str = refresh_accum(accum)

        # read old table
        if int(accum.value) == 1:
            # if first run ,read from MySQL
            print("reading from MySQL############################################################################")
            user_history_df = read_from_MySQL(spark, 'user_history_df')
            item_count_df = read_from_MySQL(spark, 'item_count_df')
            pair_count_df = read_from_MySQL(spark, 'pair_count_df')
            # sim_df = read_from_MySQL(spark, 'sim_df')
            try:
                Li_df = read_from_MySQL(spark, 'tempLi_df')
            except:
                Li_df = pair_count_df.select('item_p', 'item_q').filter(pair_count_df['item_p'].isNull())

            print(
                "reading from MySQL over ############################################################################")
        else:
            # # not first run ,read from other file such as parquet with old num name file
            if typestr == 'redis':
                print(
                    "reading from Redis############################################################################")
                user_history_df = read_from_redis(spark, 'tempuser_history_df' + oldacc_num_str)
                item_count_df = read_from_redis(spark, 'tempitem_count_df' + oldacc_num_str)
                pair_count_df = read_from_redis(spark, 'temppair_count_df' + oldacc_num_str)
                # sim_df = read_from_redis(spark, 'tempsim_df' + oldacc_num_str)
                Li_df = read_from_redis(spark, 'tempLi_df' + oldacc_num_str)
                print(
                    "reading from Redis over ############################################################################")
            elif typestr == 'parquet':
                print(
                    "reading from parquet############################################################################")
                user_history_df = read_from_parquet(spark, 'tempuser_history_df' + oldacc_num_str)
                item_count_df = read_from_parquet(spark, 'tempitem_count_df' + oldacc_num_str)
                pair_count_df = read_from_parquet(spark, 'temppair_count_df' + oldacc_num_str)
                # sim_df = read_from_parquet(spark, 'tempsim_df' + oldacc_num_str)
                Li_df = read_from_parquet(spark, 'tempLi_df' + oldacc_num_str)
                print(
                    "reading from parquet over ############################################################################")
                ##not first run ,read from other file such as redis table with old num name file

        ratinglog_df = spark.createDataFrame(rdd)
        ratinglog_df = ratinglog_df.select('UserID', 'MovieID', 'Rating').toDF('user', 'item', 'rating')
        ratinglog_df.persist()
        # 后面多用

        # TODO 更新user history
        user_history_df = user_history_df.union(ratinglog_df)
        user_history_df.show(5)
        user_history_df.persist()

        # TODO 计算delta itemcount
        item_count_deltadf = ratinglog_df.groupBy('item').sum('rating').toDF('item', 'item_count')

        # 计算出每个 item count
        # item_count_deltadf.persist()
        # 中间结果不需要persist (后面只用了一次)
        # print('this is item_count_deltadf')
        # item_count_deltadf.show(5)

        # TODO : update itemcount
        def update_fun(old, delta):
            if delta == None:
                return old
            elif old == None:
                return delta
            else:
                return old + delta

        update_udf = udf(lambda x, y: update_fun(x, y), IntegerType())

        item_count_df = item_count_df.join(item_count_deltadf.toDF('item', 'delta'), 'item', 'full_outer') \
            .withColumn('new_itemcount', update_udf('item_count', 'delta')) \
            .select('item', 'new_itemcount') \
            .toDF('item', 'item_count')
        # #add delta to old itemcount
        print('this is updated item_count_df')
        item_count_df.show(5)
        item_count_df.persist()
        # need to write need persist

        # # TODO realtime purning

        # Li_df: item_p(i) item_q(j)

        Li_now = ratinglog_df.select('item').distinct() \
            .toDF('item_p') \
            .join(Li_df, 'item_p')
        # 得到所有new rating item i's Li :   rating p is new

        jratedbyuser = ratinglog_df.toDF('user', 'item_p', 'rating_p') \
            .join(user_history_df.toDF('user', 'item_q', 'rating_q'), 'user') \
            .filter('item_p!=item_q') \
            .select('user', 'item_p', 'item_q', 'rating_p', 'rating_q')
        jratedbyuser.show(5)
        # 得到每个 j rated by user u

        Li = Li_now.toDF('i', 'j')
        jnotinLi = jratedbyuser.join(Li, [jratedbyuser['item_p'] == Li['i'], jratedbyuser['item_q'] == Li['j']],
                                     'left_outer') \
            .filter(Li['j'].isNull()) \
            .select('user', 'item_p', 'item_q', 'rating_p', 'rating_q')

        # 用 udf 来解决 new column
        def co_rating_fun(rating_p, rating_q):
            if rating_p < rating_q:
                return rating_p
            else:
                return rating_q

        co_rating_udf = udf(lambda x, y: co_rating_fun(x, y), IntegerType())
        # 定义udf 返回值是 int (spark 里)

        co_rating_delta_ij_df = jnotinLi.withColumn('co_rating', co_rating_udf('rating_p', 'rating_q')) \
            .select('user', 'item_p', 'item_q', 'co_rating')
        # print('this is new old  corating')
        # co_rating_delta_newold_df.show(5)

        co_rating_delta_ji_df = co_rating_delta_ij_df.toDF('user', 'item_q', 'item_p', 'co_rating') \
            .select('user', 'item_p', 'item_q', 'co_rating')

        co_rating_delta = co_rating_delta_ij_df.union(co_rating_delta_ji_df)

        # TODO : update pair count

        deltapair_count_df = co_rating_delta.groupBy('item_p', 'item_q') \
            .agg({'co_rating': 'sum', '*': 'count'}) \
            .toDF('item_p', 'item_q', 'delta_nij', 'delta')

        pair_count_df = pair_count_df.join(deltapair_count_df, ['item_p', 'item_q'], 'full_outer') \
            .withColumn('new_paircount', update_udf('pair_count', 'delta')) \
            .withColumn('new_nij', update_udf('nij', 'delta_nij')) \
            .select('item_p', 'item_q', 'new_nij', 'new_paircount') \
            .toDF('item_p', 'item_q', 'nij', 'pair_count')

        ## add delta to old paircount and increment nij
        print('this is pair_count_df')
        pair_count_df.show(5)
        pair_count_df.persist()

        # TODO 计算new sim(p,q)
        sim_df = pair_count_df.join(item_count_df.toDF('item_p', 'item_count_p'), 'item_p') \
            .join(item_count_df.toDF('item_q', 'item_count_q'), 'item_q')
        # 得到item p and item q 's itemcont and pair count together

        sim_df = sim_df.withColumn('similarity',
                                   sim_df['pair_count'] / (
                                           (sim_df['item_count_p'] * sim_df['item_count_q']) ** 0.5)) \
            .select('item_p', 'item_q', 'similarity', 'nij')
        # 计算得 similarity  (由于TencentRec公式，已经范围至[0,1]
        sim_df.persist()
        print('this is similarity_df')
        sim_df.show(5)

        topk_sim_df = get_topk_similarity(sim_df, k=k)

        # TODO 判断是否purning

        # 1 get threshold t1 and t2

        t1t2threshold = deltapair_count_df.join(topk_sim_df.filter(topk_sim_df['rank'] == k), 'item_p') \
            .select('item_p', 'similarity').distinct()
        print('this is t1t2threshold')
        t1t2threshold.show()

        def threshold(t1, t2):
            if t1 < t2:
                return t1
            else:
                return t2

        threshold_udf = udf(lambda x, y: threshold(x, y), FloatType())

        # 定义udf 返回值是 int (spark 里)

        def epsilon_fun(n):
            return math.sqrt(math.log(1 / delta) / (2 * n))

        epsilon_udf = udf(lambda x: epsilon_fun(x), FloatType())

        epsilon_df = sim_df.join(t1t2threshold.toDF('item_p', 't1'), 'item_p') \
            .join(t1t2threshold.toDF('item_q', 't2'), 'item_q') \
            .withColumn('threshold', threshold_udf('t1', 't2')) \
            .withColumn('epsilon', epsilon_udf('nij')) \
            .select('item_p','item_q','threshold','epsilon','similarity')
        epsilon_df.show(100)

        purning_df = epsilon_df.filter(epsilon_df['epsilon'] < (epsilon_df['threshold'] - epsilon_df['similarity'])) \
            .select('item_p', 'item_q')
        purning_df.show(5)


        Li_df = Li_df.union(purning_df).union(purning_df.toDF('item_q', 'item_p').select('item_p', 'item_q')).distinct()
        Li_df.persist()
        Li_df.show()

        # # TODO recommend n for user use top k
        # 得到当前行为的用户
        action_user_df = ratinglog_df.select('user').distinct()
        # 过滤得到当前action 的用户的历史数据
        action_user_df = action_user_df.join(user_history_df, 'user', 'left_outer').filter(
            user_history_df['item'].isNotNull())
        # 过滤出可以推荐的用户
        # action_user_df.show()

        # TODO 对新用户推荐
        # user_interest_df = recommend_N_for_user(user_history_df, topk_sim_df, N)
        user_interest_df = recommend_N_for_user(action_user_df, topk_sim_df, N)
        user_interest_df.persist()

        endforcal = time.time()
        print('calculate use:')
        print(endforcal - start_time)

        # #TODO 写入文件或者库:
        # # 写入文件/redis
        if typestr == 'redis':
            print("writing To Redis ############################################################################")
            write_to_redis(spark, df=user_history_df, table='tempuser_history_df' + newacc_num_str)
            write_to_redis(spark, df=item_count_df, table='tempitem_count_df' + newacc_num_str)
            write_to_redis(spark, df=pair_count_df, table='temppair_count_df' + newacc_num_str)
            # write_to_redis(spark, df=sim_df, table='tempsim_df' + newacc_num_str)
            write_to_redis(spark, df=user_interest_df, table='tempuser_interest_df' + newacc_num_str)
            write_to_redis(spark, df=Li_df, table='tempLi_df' + newacc_num_str)
            print(
                "writing To Redis over ############################################################################")


        elif typestr == 'parquet':
            print("writing To parquet ############################################################################")
            write_to_parquet(spark, df=user_history_df, table='tempuser_history_df' + newacc_num_str)
            write_to_parquet(spark, df=item_count_df, table='tempitem_count_df' + newacc_num_str)
            write_to_parquet(spark, df=pair_count_df, table='temppair_count_df' + newacc_num_str)
            # write_to_parquet(spark, df=sim_df, table='tempsim_df' + newacc_num_str)
            write_to_parquet(spark, df=user_history_df, table='tempuser_interest_df' + newacc_num_str)
            write_to_parquet(spark, df=Li_df, table='tempLi_df' + newacc_num_str)
            print(
                "writing To parquet over ############################################################################")

        # 默认写入redis

        if int(newacc_num_str) == 5:
            # 若写入文件10次了，也写入数据库：
            print("writing To MySQL ############################################################################")
            write_to_MySQL(spark, df=user_history_df, table='tempuser_history_df')
            write_to_MySQL(spark, df=item_count_df, table='tempitem_count_df')
            write_to_MySQL(spark, df=pair_count_df, table='temppair_count_df')
            # write_to_MySQL(spark, df=sim_df, table='tempsim_df')
            write_to_MySQL(spark, df=user_interest_df, table='tempuser_interest_df')
            write_to_MySQL(spark, df=Li_df, table='tempLi_df')
            print("writing To MySQL over ############################################################################")

        time.sleep(1)
        # wait write terminal

        ratinglog_df.unpersist()
        # 级联unpersist 后面的df

        end_time = time.time()
        print('本次 Incremental Update 用了')
        print(end_time - start_time)

    else:
        pass


def incremental_Update_ItemCF(spark: SparkSession, interval: int, k: int, N: int, typestr: str):
    """
    get log from kafka and update the similarity
    :param spark: sparkSession
    :return:
    """
    print('Starting streaming incremental update itemcf')

    sc = spark.sparkContext
    accum = sc.accumulator(0)
    # set accumulator to regist what table is to read or write

    ssc = StreamingContext(sc, interval)
    # 创建一个StreamingContext用于SparkStreaming，划分Batches时间间隔为interval s

    ratinglog_DStream = get_ratinglogDstream_from_Kafka(ssc)
    # get DStream from log

    # ratinglog_DStream.foreachRDD(lambda x: incremental_update_fun(rdd=x, accum=accum, k=k, N=N, typestr=typestr))
    ratinglog_DStream.foreachRDD(
        lambda x: incremental_update_fun_withpurning(rdd=x, accum=accum, k=k, N=N, delta=0.05, typestr=typestr))

    ssc.start()  # Start the computation
    ssc.awaitTermination()  # Wait for the computation to terminate


def streaming_recommend_fun(rdd: RDD, accum: Accumulator, N: int, typestr: str):
    if rdd.isEmpty() == False:
        start_time = time.time()

        # 计时器
        # 记录文件名累加器(user history )
        oldacc_num_str, newacc_num_str = refresh_accum(accum=accum)

        # read old table user history
        if int(accum.value) == 1:
            # read from MySQL
            # if first run ,read from MySQL
            print("reading from MySQL ############################################################################")
            user_history_df = read_from_MySQL(spark, 'user_history_df')
            # sim_df = read_from_MySQL(spark, 'sim_df')
            topk_sim_df = read_from_MySQL(spark, 'topk_sim_df')
            print(
                "reading from MySQL over ############################################################################")
        else:
            if typestr == 'redis':
                # read from redis
                print(
                    "reading from Redis ############################################################################")
                user_history_df = read_from_redis(spark, 'tempuser_history_df' + oldacc_num_str)
                # sim_df = read_from_redis(spark, 'tempsim_df')
                topk_sim_df = read_from_redis(spark, 'temptopk_sim_df')
                print(
                    "reading from Redis over ############################################################################")

            elif typestr == 'parquet':
                # read from redis
                print(
                    "reading from parquet ############################################################################")
                user_history_df = read_from_parquet(spark, 'tempuser_history_df' + oldacc_num_str)
                # sim_df = read_from_redis(spark, 'tempsim_df')
                topk_sim_df = read_from_parquet(spark, 'temptopk_sim_df')
                print(
                    "reading from parquet over ###################################################################")

        ratinglog_df = spark.createDataFrame(rdd)
        ratinglog_df = ratinglog_df.select('UserID', 'MovieID', 'Rating').toDF('user', 'item', 'rating')
        # ratinglog_df.persist()
        # 后面多用

        # TODO 更新user history
        user_history_df = user_history_df.union(ratinglog_df)
        user_history_df.persist()

        # TODO 得到当前行为的用户
        action_user_df = ratinglog_df.select('user').distinct()

        # TODO 过滤得到当前action 的用户的历史数据
        action_user_df = action_user_df.join(user_history_df, 'user', 'left_outer').filter(
            user_history_df['item'].isNotNull())
        # 过滤出可以推荐的用户
        # action_user_df.show()
        # TODO 对新用户推荐

        # TODO recommend n for user use top k use ready-made similarity
        # topk_sim_df = get_topk_similarity(sim_df, k=k)
        # user_interest_df = recommend_N_for_user(user_history_df, topk_sim_df, N)
        user_interest_df = recommend_N_for_user(action_user_df, topk_sim_df, N)
        user_interest_df.persist()

        user_history_df.show(5)
        user_interest_df.show(5)

        end_time = time.time()
        print('本次 streaming recommend only calculate  用了')
        print(end_time - start_time)

        # #TODO 写入文件或者库:
        # 默认写入redis
        if typestr == 'redis':
            print("writing To Redis ############################################################################")
            if int(accum.value) == 1:
                # write_to_redis(spark, df=sim_df, table='tempsim_df')
                write_to_redis(spark, df=topk_sim_df, table='temptopk_sim_df')
                # 第一次进行，顺便将相似度写入redis
            write_to_redis(spark, df=user_history_df, table='tempuser_history_df' + newacc_num_str)
            print(
                "writing To Redis over ############################################################################")

        elif typestr == 'parquet':
            print("writing To parquet ############################################################################")
            if int(accum.value) == 1:
                # write_to_redis(spark, df=sim_df, table='tempsim_df')
                write_to_parquet(spark, df=topk_sim_df, table='temptopk_sim_df')
                # 第一次进行，顺便将相似度写入redis
            write_to_parquet(spark, df=user_history_df, table='tempuser_history_df' + newacc_num_str)
            print(
                "writing To parquet over ############################################################################")

        print("writing To MySQL ############################################################################")
        write_to_MySQL(spark, df=user_interest_df, table='tempuser_interest_df' + newacc_num_str)
        # 推荐结果写入MySQL
        if int(newacc_num_str) == 5:
            # 若写入文件10次了，也吧 user history 写入数据库：
            print("writing To MySQL ############################################################################")
            write_to_MySQL(spark, df=user_history_df, table='tempuser_history_df')
        print("writing To MySQL over ############################################################################")

        time.sleep(1)
        # wait write terminal

        ratinglog_df.unpersist()
        # 级联unpersist 后面的df

        end_time = time.time()
        print('本次 streaming recommend only  用了')
        print(end_time - start_time)

    else:
        pass


def streaming_recommend_only(spark: SparkSession, interval: int, N: int, typestr: str):
    print('Starting streaming recommend only!!')

    sc = spark.sparkContext
    accum = sc.accumulator(0)
    # set accumulator to regist what table is to read or write for user history

    ssc = StreamingContext(sc, interval)

    ratinglog_DStream = get_ratinglogDstream_from_Kafka(ssc)
    # get DStream from log

    ratinglog_DStream.foreachRDD(lambda x: streaming_recommend_fun(rdd=x, accum=accum, N=N, typestr=typestr))
    # for each batch recommend use calculated similarity

    ssc.start()
    ssc.awaitTermination()


def streaming_itemcf(rdd: RDD, accum: Accumulator, k: int, N: int):
    if rdd.isEmpty() == False:
        start_time = time.time()

        # 计时器
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

        # read old table user history
        if int(accum.value) == 1:
            # read from MySQL
            # if first run ,read from MySQL
            print("reading from MySQL ############################################################################")
            user_history_df = read_from_MySQL(spark, 'user_history_df')
            print(
                "reading from MySQL over ############################################################################")

        else:
            # read from redis
            print(
                "reading from Redis ############################################################################")
            user_history_df = read_from_redis(spark, 'tempuser_history_df' + oldacc_num_str)
            # sim_df = read_from_redis(spark, 'tempsim_df')
            print(
                "reading from Redis over ############################################################################")

        ratinglog_df = spark.createDataFrame(rdd)
        ratinglog_df = ratinglog_df.select('UserID', 'MovieID', 'Rating').toDF('user', 'item', 'rating')
        # ratinglog_df.persist()
        # 后面多用

        # TODO 更新user history
        ratings_df = user_history_df.union(ratinglog_df)
        ratings_df.persist()
        user_history_df = ratings_df

        # TODO 计算item count
        item_count_df = ratings_df.groupBy('item').sum('rating').toDF('item', 'item_count')
        # 计算出每个 item count
        item_count_df.persist()

        # 以后要用，所以持久化
        # print('this is item_count_df')
        # item_count_df.show()

        # TODO 计算pari count
        # 用 udf 来解决 new column
        def co_rating_fun(rating_p, rating_q):
            if rating_p < rating_q:
                return rating_p
            else:
                return rating_q

        co_rating_udf = udf(lambda x, y: co_rating_fun(x, y), IntegerType())
        # 定义udf 返回值是 int (spark 里)

        co_rating_df = ratings_df.toDF('user', 'item_p', 'rating_p') \
            .join(ratings_df.toDF('user', 'item_q', 'rating_q'), 'user') \
            .filter('item_p != item_q') \
            .withColumn('co_rating', co_rating_udf('rating_p', 'rating_q')).select('user', 'item_p', 'item_q',
                                                                                   'co_rating')
        # co_rating_df=co_rating_df.withColumn('co_rating',co_rating_udf('rating_p','rating_q')).select('user','item_p','item_q','co_rating')
        # print('this is co_rating_df')
        # co_rating_df.show()

        pair_count_df = co_rating_df.groupBy('item_p', 'item_q').sum('co_rating').toDF('item_p', 'item_q', 'pair_count')
        # pair count of item p
        pair_count_df.persist()
        # # 由于后面有用，所以持久化
        # print('this is pair_count_df')
        # pair_count_df.show()

        # TODO 计算sim(p,q)
        sim_df = pair_count_df.join(item_count_df.toDF('item_p', 'item_count_p'), 'item_p') \
            .join(item_count_df.toDF('item_q', 'item_count_q'), 'item_q')
        # 得到item p and item q 's itemcont and pair count together

        sim_df = sim_df.withColumn('similarity',
                                   sim_df['pair_count'] / ((sim_df['item_count_p'] * sim_df['item_count_q']) ** 0.5)) \
            .select('item_p', 'item_q', 'similarity')
        # 计算得 similarity  (由于TencentRec公式，已经范围至[0,1])
        sim_df.persist()
        # print('this is similarity_df')
        sim_df.show()

        # # TODO recommend n for user use top k use ready-made similarity
        # topk_sim_df = get_topk_similarity(sim_df, k=k)
        # user_interest_df = recommend_N_for_user(user_history_df, topk_sim_df, N)
        # user_interest_df.persist()

        # #TODO 写入文件或者库:
        # 默认写入redis
        print("writing To Redis ############################################################################")
        if int(accum.value) == 1:
            write_to_redis(spark, df=sim_df, table='tempsim_df')
            # 第一次进行，顺便将相似度写入redis
        write_to_redis(spark, df=user_history_df, table='tempuser_history_df' + newacc_num_str)
        print(
            "writing To Redis over ############################################################################")

        # write_to_MySQL(spark, df=user_interest_df, table='tempuser_interest_df' + newacc_num_str)
        # 推荐结果写入MySQL
        if int(newacc_num_str) == 5:
            # 若写入文件10次了，也吧 user history 写入数据库：
            print("writing To MySQL ############################################################################")
            write_to_MySQL(spark, df=user_history_df, table='tempuser_history_df')
            print("writing To MySQL over ############################################################################")

        time.sleep(1)
        # wait write terminal

        ratinglog_df.unpersist()
        # 级联unpersist 后面的df

        end_time = time.time()
        print('本次 streaming itemcf only  用了')
        print(end_time - start_time)

    else:
        pass
    pass


def streaming_calculate_similarity(spark: SparkSession, interval: int, k: int, N: int):
    print('Starting streaming calculate similarity !!')

    sc = spark.sparkContext
    accum = sc.accumulator(0)
    # set accumulator to regist what table is to read or write for user history

    ssc = StreamingContext(sc, interval)

    ratinglog_DStream = get_ratinglogDstream_from_Kafka(ssc)
    # get DStream from log

    ratinglog_DStream.foreachRDD(lambda x: streaming_itemcf(rdd=x, accum=accum, k=k, N=N))
    # for each batch recommend use calculated similarity

    ssc.start()
    ssc.awaitTermination()


def realtime_pruning():
    pass


def recommend_for_user():
    # # get three tables as spark's dataframe
    # users_df = data_processing_utils.get_users_df(spark)
    # movies_df = data_processing_utils.get_movies_df(spark)
    # # ratings_df=data_processing_utils.get_ratings_df(spark)  #all rating's record
    # ratings_df = data_processing_utils.get_small_ratings_df(spark)  # get small record
    #
    # user_history_df, item_count_df, pair_count_df, sim_df = get_matrix_of_similarity(spark, ratings_df)
    pass


if __name__ == '__main__':
    spark = SparkSession.builder.master('local[*]').appName('TencentRec') \
        .config("spark.redis.host", "Machine-zzh") \
        .config("spark.redis.port", "6379") \
        .getOrCreate()

    # .enableHiveSupport() \
    # .getOrCreate()

    # split_data(spark, 'debug2')

    # item_cf(spark, k=20)
    # streaming_recommend_only(spark, interval=100, N=10, typestr='parquet')

    # r=Row(item_p=,item_q=)
    # Li_df=
    # Li_df = write_to_parquet(spark,df=Li_df,'tempLi_df')
    incremental_Update_ItemCF(spark, interval=20, k=20, N=10, typestr='parquet')

    # streaming_calculate_similarity(spark,interval=80,k=20,N=10)

    # print("reading from MySQL############################################################################")
    # user_history_df = read_from_MySQL(spark, 'user_history_df')
    # # item_count_df = read_from_MySQL(spark, 'item_count_df')
    # # pair_count_df = read_from_MySQL(spark, 'pair_count_df')
    # sim_df = read_from_MySQL(spark, 'sim_df')
    # print("reading from MySQL over ############################################################################")
    #
    # topk_sim_df = get_topk_similarity(sim_df, k=20)
    # user_interest_df = recommend_N_for_user(user_history_df, topk_sim_df, 10)

    spark.stop()
