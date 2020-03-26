from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import Row,functions
from pyspark.ml.linalg import Vectors
from pyspark.sql.window import Window

import time
import numpy as np
import matplotlib.pyplot as plt
from collections import Counter

import data_processing_utils

def get_matrix_of_usersXitems(spark:SparkSession,ratings_df):
    """

    :param spark: SparkSession
    :param ratings_df: ratings dataframe
    :return: the dataframe contain the similarity of itema itemb
    """

    #ratings_df.show()

    # First: get user_item  and item_user

    user_item_rdd=ratings_df.select(ratings_df['UserID'],ratings_df['MovieID']).rdd.map(lambda x:(x[0],x[1]))
    # #read from rating to a rdd ,item in rdd is tuple (UserID,MovieID)
    # print(user_item_rdd.collect())  #too much   print out is tuple
    user_item_df=spark.createDataFrame(user_item_rdd.map(lambda x:Row(UserID=x[0],Movie_ID=x[1])))
    #transfer to dataframe to show
    #user_item_df.show()
    # print(user_item_df.rdd.collect())   #print out is Row(UseId,MovieID)


    user_item_rdd=user_item_rdd.join(user_item_rdd).filter(lambda x:x[1][0]!=x[1][1])
    # first step : join
    # second step : filter same itemid  such as Uid , item a ,item a
    #UserID type is str
    item_item_user_df=spark.createDataFrame(user_item_rdd.map(lambda x:Row(UserID=x[0],Movie_ID_1=x[1][0],Movie_ID_2=x[1][1])))
    item_item_user_df.show()
    #这个是写明那些 item 与 item 属于哪个user 的df

    item_item_intersection_df=item_item_user_df.groupBy('Movie_ID_1','Movie_ID_2').count().toDF('itemi','itemj','N(i)_intersection_N(j)')
    item_item_intersection_df.show()

    item_count_df=item_item_user_df.groupBy('Movie_ID_1').count().toDF('itemi','|N(i)|')
    item_count_df.show()

    item_item_interCount_unionCount_df=item_item_intersection_df.join(item_count_df,'itemi')
    item_item_interCount_unionCount_df.show()

    item_count_df=item_count_df.toDF('itemj','|N(j)|')
    #item_item_interCount_unionCount_df=item_item_interCount_unionCount_df.join(item_count_df,item_item_interCount_unionCount_df.itemj==item_count_df.itemj)
    item_item_interCount_unionCount_df = item_item_interCount_unionCount_df.join(item_count_df,'itemj')
    item_item_interCount_unionCount_df.show()

    #计算相似度
    item_item_similarity_df=item_item_interCount_unionCount_df.withColumn('similarity',item_item_interCount_unionCount_df['N(i)_intersection_N(j)']/((item_item_interCount_unionCount_df['|N(i)|']*item_item_interCount_unionCount_df['|N(j)|'])**0.5))\
        .select('itemi','itemj','similarity')
    item_item_similarity_df.show()
    item_item_similarity_df.persist()
    #持久化，后面多用


    #对item j 的similarity 归一化
    itemj_max_similarity_df=item_item_similarity_df.groupby('itemj').max()
    item_item_similarity_df=item_item_similarity_df.join(itemj_max_similarity_df,'itemj')
    item_item_similarity_df=item_item_similarity_df.select('itemi','itemj',item_item_similarity_df['similarity']/item_item_similarity_df['max(similarity)']).toDF('itemi','itemj','similarity')


    #对item i 的相似度排序
    #用 窗口函数  WIndow
    item_item_similarity_df=item_item_similarity_df.withColumn('rank',functions.row_number().over(Window.partitionBy("itemi").orderBy(functions.desc('similarity'))))
    #item_item_similarity_df = item_item_similarity_df.withColumn('rank', functions.rank().over(Window.partitionBy("itemi").orderBy(functions.desc('similarity'))))
    #用row number 则是不重复rank ,rank（）则有重复rank
    item_item_similarity_df.show()

    return  item_item_similarity_df




def get_topk_similarity(item_item_similarity_df:DataFrame,k:int):

    topk_similarity_df=item_item_similarity_df.filter(item_item_similarity_df['rank']<k+1)
    #topk_similarity_df.show(50)

    return topk_similarity_df


def recommend_N_for_user(ratings_df:DataFrame,topk_similarity_df:DataFrame,N:int):


    #TODO filter had seen by user(not successd yet )
    user_item_rating_df=ratings_df.select('UserID','MovieID',ratings_df['Rating']/5).toDF('user','itemi','rating')
    #rating normalization

    user_item_interest_df=user_item_rating_df.join(topk_similarity_df,'itemi')
    #join get user's item top k similarity

    user_item_interest_df=user_item_interest_df.withColumn('interest', user_item_interest_df['rating'] * user_item_interest_df['similarity'])\
        .groupBy('user','itemj').sum('interest').select('user', 'itemj', 'sum(interest)').toDF('user','itemj','interest')
    #calculate interest

    user_item_interest_df=user_item_interest_df.withColumn('rank', functions.row_number().over(Window.partitionBy('user').orderBy(functions.desc('interest'))))
    #get interest and order

    user_interest_topk_df=user_item_interest_df.filter(user_item_interest_df['rank'] < N + 1)
    #calculate topN command

    user_interest_topk_df.show(100)

    return None



def trytry(spark:SparkSession):
    # sc=spark.sparkContext
    # list=[1,2,3,4,5]
    # list_rdd=sc.parallelize(list)
    # print(list_rdd.collect())

    matrix_data = [(0, Vectors.dense([1, 2, 3]),),
                   (1, Vectors.dense([4, 5, 6]),),
                   (2, Vectors.dense([7, 8, 9]),)]

    matrix_dataframe = spark.createDataFrame(matrix_data, ["id", "features"])
    # print(matrix_dataframe.collect())
    new_df = matrix_dataframe.filter(matrix_dataframe['id'] == 0).select('features')
    # new_df.show()

    new_df.rdd.map(lambda x: print(x))

    # data2=[(0,Vectors.sparse(10,[1,2,7],[5,5,5]),),
    #        (1,Vectors.sparse(10,[4,6,8],[5,7,8]),),
    #        (2,Vectors.sparse(10,[3,5,7],[1345,5,74]),)]
    # df=spark.createDataFrame(data2,['id','features'])
    # df.show()

    ###############################################
    # #形式为item _item 的rdd，也就是一个Item相似item b list
    # item_item_rdd=user_item_rdd.map(lambda x:(x[1][0],[x[1][1]])).reduceByKey(lambda a,b:a+b)
    # # get itema <itemb,item c, > as a W matrix 's row
    #
    # temp_df=spark.createDataFrame(item_item_rdd.map(lambda x:Row(item1=x[0],item2=x[1])))
    # #print(tempdf.filter(tempdf['item1']==3408).select('item2').rdd.map(lambda x:(len(x[0]))).collect())
    # #print(tempdf.filter(tempdf['item1']==3408).select('item2').rdd.map(lambda x:(len(set(x[0])))).collect())
    #
    #
    # # print(tempdf.rdd.map(lambda x:(x[0],len(x[1]))).take(20))
    # # print(tempdf.rdd.map(lambda x:(x[0],len(set(x[1])))).take(20))
    # # #看看有没有某些物品比较相似
    # # print(tempdf.filter(tempdf['item1']==2321).select('item2').rdd.map(lambda x:dict(Counter(x[0]))).take(20))
    # # #说明item item 有重复的item a item b 所以是现实的数据
    ####################################################

    # calculate the similarity
    # print(item_item_rdd.groupByKey().take(20))

    # item_user_rdd=user_item_rdd.map(lambda user_item_tuple:(user_item_tuple[1],user_item_tuple[0]))
    # #reverse to (item user)

    ####################################################################################
    # Second : for each user in last transfer item_user_rdd (item,[user a, user b,]),
    # use user_item_rdd (user , [item a ,item b,]) to add Vector.sparse  to build matrix W




    # user_item_rdd=user_item_rdd.groupByKey().map(lambda user_item_tuple:(user_item_tuple[0],list(user_item_tuple[1])))
    # #transfer to  (user, [item a ,item b,])
    # # print(user_item_rdd.collect())
    # user_item_df=spark.createDataFrame(user_item_rdd.map(lambda x:Row(UserID=x[0],Movie_ID=x[1])))
    # #transfer to dataframe to show
    # user_item_df.show()















    # def get_feature_list_from_partition(iterator):
    #     result = []
    #     for arr in iterator:
    #         arr_tmp = list(set(arr[1][1]))
    #         arr_tmp.sort()
    #         result.append((arr[0], Vectors.sparse(arr[1][0], arr_tmp, [1] * len(arr_tmp))))
    #     return result
    #
    # unionRDD = train.union(test)
    # userItemRDD = unionRDD.map(lambda x: (x[0], x[1]))
    # # 计算物品相似度
    # # 基于LSH的操作
    # max_user_id = userItemRDD.map(lambda x: int(x[0])).distinct().max()
    # itemUserRDD = userItemRDD.map(lambda x: (x[1], [x[0]])).reduceByKey(lambda x, y: x + y)
    # itemUserRDD = itemUserRDD.map(lambda x: (x[0], [max_user_id + 1, x[1]]))
    # item_vec_rdd = itemUserRDD.mapPartitions(get_feature_list_from_partition)
    # item_vec_df = item_vec_rdd.toDF(["item", "features"])
    # item_vec_df.show()
    # mh = MinHashLSH(inputCol="features", outputCol="hashes", numHashTables=5)
    # model = mh.fit(item_vec_df)

    #
    # def afun(spark:SparkSession,ratings_df:DataFrame):
    #
    #     print()
    #     ratings_df.rdd.map

if __name__ == '__main__':
    spark=SparkSession.builder.appName('ItemCF').enableHiveSupport().getOrCreate()



    starttime=time.time()

    #get data
    #get three table as spark's dataframe
    users_df=data_processing_utils.get_users_df(spark)
    movies_df=data_processing_utils.get_movies_df(spark)
    #ratings_df=data_processing_utils.get_ratings_df(spark)
    ratings_df=data_processing_utils.get_small_ratings_df(spark)

    #get similarity df
    item_item_similarity_df=get_matrix_of_usersXitems(spark,ratings_df)

    topk_similarity_df=get_topk_similarity(item_item_similarity_df,k=20)

    recommend_N_for_user(ratings_df,topk_similarity_df,N=30)


    endtime=time.time()
    print('用了')
    print(endtime-starttime)


    # max_user_id=users_df.select('UserID').rdd\
    #     .map(lambda x:int(x[0])).distinct().max()
    #     #x is a Row  (Dataframe of spark is contain of row)
    # print('max_user_id is :')
    # print(max_user_id)


    spark.stop()


