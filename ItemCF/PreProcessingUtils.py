from pyspark.sql import SparkSession
from pyspark.sql import Row
import random



#1m
# usersfile = 'file:///home/zzh/zzh/Program/Recommend_System/ItemCF/ml-1m/users.dat'
# moviesfile = 'file:///home/zzh/zzh/Program/Recommend_System/ItemCF/ml-1m/movies.dat'
# ratingsfile = 'file:///home/zzh/zzh/Program/Recommend_System/ItemCF/ml-1m/ratings.dat'

def get_users_df(spark: SparkSession, file:str):
    # 读取user表
    # 用reflact 法得到columns

    sc = spark.sparkContext

    # Load a text file and convert each line to a Row
    lines = sc.textFile(file)
    parts = lines.map(lambda l: l.split("::"))
    user = parts.map(lambda p: Row(UserID=p[0], Gender=p[1], Age=int(p[2]), Occupation=p[3], Zip_code=p[4]))

    # Infer the schema
    users_df = spark.createDataFrame(user)
    # users_df.show()
    # users_df.printSchema()

    return users_df


def get_movies_df(spark: SparkSession, file:str):
    # 读取movie表
    # 用reflact 法得到columns

    sc = spark.sparkContext

    # Load a text file and convert each line to a Row
    lines = sc.textFile(file)
    parts = lines.map(lambda l: l.split("::"))
    movie = parts.map(lambda p: Row(MovieID=p[0], Title=p[1], Genres=p[2]))

    # Infer the schema
    movies_df = spark.createDataFrame(movie)
    # movies_df.show()
    # movies_df.printSchema()

    return movies_df


def get_ratings_df(spark: SparkSession, file:str):
    # 读取ratings表
    # 用reflact 法得到columns

    sc = spark.sparkContext

    # Load a text file and convert each line to a Row
    lines = sc.textFile(file)
    parts = lines.map(lambda l: l.split("::"))
    rating = parts.map(lambda p: Row(UserID=p[0], MovieID=p[1], Rating=int(p[2]), Timestamp=p[3]))

    # Infer the schema
    ratings_df = spark.createDataFrame(rating)
    # ratings_df.show()
    # ratings_df.printSchema()

    return ratings_df


def get_small_ratings_df(spark: SparkSession):
    # 读取ratings表
    # 用reflact 法得到columns

    sc = spark.sparkContext

    # Load a text file and convert each line to a Row
    lines = sc.textFile("file:///home/zzh/zzh/Program/Recommend_System/ItemCF/ml-1m/train_ratings.dat")
    parts = lines.map(lambda l: l.split("::"))
    rating = parts.map(lambda p: Row(UserID=p[0], MovieID=p[1], Rating=int(p[2]), Timestamp=p[3]))

    # Infer the schema
    ratings_df = spark.createDataFrame(rating)
    # ratings_df.show()
    # ratings_df.printSchema()

    return ratings_df


def split_streaminglog(line: str):
    p = line.split("::")
    # split as part

    return Row(UserID=p[0], MovieID=p[1], Rating=int(p[2]), Timestamp=p[3])


#  For 100k small

ratingsfile = 'file:///home/zzh/zzh/Program/Recommend_System/ml-latest-small/ratings.csv'
moviesfile = 'file:///home/zzh/zzh/Program/Recommend_System/ml-latest-small/movies.csv'
tagsfile = 'file:///home/zzh/zzh/Program/Recommend_System/ml-latest-small/tags.csv'

def ReadRatings_df(spark:SparkSession,file:str):
    ratings_df = spark.read.csv(file, header=True, inferSchema=True)
    # ratings_df.show(5)
    # ratings_df.printSchema()
    ratings_df=ratings_df.toDF('user','item','rating','timestamp')

    return ratings_df


def ReadMovies_df(spark: SparkSession, file: str):
    movies_df = spark.read.csv(file, header=True, inferSchema=True)
    # movies_df.show(5)
    # movies_df.printSchema()
    movies_df = movies_df.toDF('item', 'title', 'genres')

    return movies_df


def ReadTags_df(spark: SparkSession, file: str):
    tags_df = spark.read.csv(file, header=True, inferSchema=True)
    # tags_df.show(5)
    # tags_df.printSchema()
    tags_df = tags_df.toDF('user', 'item', 'tag', 'timestamp')

    return tags_df


#read and split and write
def ReadFile(filename):
    datalist = []
    with open(filename, 'r') as file:
        for line in file.readlines():
            datalist.append(line)
    # print(datalist[1])
    # print(type(datalist[1]))
    return datalist


def RandomSplit(datalist: list, train_Ratio, test1_Ratio, test2_Ratio):
    # if train_Ratio + test_Ratio + test2_Ratio != 1:
    #     print('wrong Ratio')
    #     return 0

    random.shuffle(datalist)
    # 打乱list
    length = len(datalist)
    train_num = int(length * train_Ratio)
    test1_num = int(length * test1_Ratio)
    test2_num = int(length * test2_Ratio)

    if length == 0 or train_num < 1 or test1_num < 1:
        return [], []
    trainlist = datalist[:train_num]
    test1list = datalist[train_num:train_num + test1_num]

    test2list = datalist[train_num + test1_num:train_num + test1_num + test2_num]

    return trainlist, test1list, test2list


def WriteFile(trainlist, trainfile, test1list, test1file, test2list, test2file):
    with open(trainfile, 'w') as f:
        for item in trainlist:
            f.write(item)

    with open(test1file, 'w') as f:
        for item in test1list:
            f.write(item)

    with open(test2file, 'w') as f:
        for item in test2list:
            f.write(item)

def PreProcessKafkaStream(line: str):
    p = line.split(",")
    # split as part

    return Row(user=p[0], item=p[1], rating=int(p[2]), timestamp=p[3])



if __name__ == '__main__':
    spark = SparkSession.builder.appName('GetData').getOrCreate()
        # .enableHiveSupport().getOrCreate()

    # users_df=get_users_df(spark)
    # movies_df=get_movies_df(spark)
    # ratings_df=get_ratings_df(spark)
    # get_small_ratings_df(spark)

    # split_data(spark)


    # print('这是一个随机切分rating的程序')
    # ratingsfile = '/home/zzh/zzh/Program/Recommend_System/ml-latest-small/ratings.csv'
    # trainfile = '/home/zzh/zzh/Program/Recommend_System/ml-latest-small/train.csv'
    # testfile = '/home/zzh/zzh/Program/Recommend_System/ml-latest-small/test1.csv'
    # testfile2 = '/home/zzh/zzh/Program/Recommend_System/ml-latest-small/test2.csv'
    #
    # datalist = ReadFile(ratingsfile)
    # trainlist, testlist, testlist2 = RandomSplit(datalist, 0.8, 0.1, 0.1)
    # WriteFile(trainlist, trainfile, testlist, testfile, testlist2, testfile2)

    spark.stop()
