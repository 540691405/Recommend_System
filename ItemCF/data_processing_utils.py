from pyspark.sql import SparkSession
from pyspark.sql import Row

usersfile = 'file:///home/zzh/zzh/Program/Recommend_System/ItemCF/ml-1m/users.dat'
moviesfile = 'file:///home/zzh/zzh/Program/Recommend_System/ItemCF/ml-1m/movies.dat'
ratingsfile = 'file:///home/zzh/zzh/Program/Recommend_System/ItemCF/ml-1m/ratings.dat'


def get_users_df(spark: SparkSession, file=usersfile):
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


def get_movies_df(spark: SparkSession, file=moviesfile):
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


def get_ratings_df(spark: SparkSession, file=ratingsfile):
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


if __name__ == '__main__':
    spark = SparkSession.builder.appName('ItemCF').enableHiveSupport().getOrCreate()

    # users_df=get_users_df(spark)
    # movies_df=get_movies_df(spark)
    # ratings_df=get_ratings_df(spark)
    # get_small_ratings_df(spark)

    # split_data(spark)

    spark.stop()
