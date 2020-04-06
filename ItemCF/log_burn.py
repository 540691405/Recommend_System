import time
import random


def generate_log(filename, logfile):
    datalist = []
    dataset = set()

    with open(filename, 'r') as file:
        for line in file.readlines():
            datalist.append(line)
    print(datalist[1])
    print(type(datalist[1]))

    while (True):
        # 持续生成日志条目
        while (True):
            # 持续随机生成，并且不重复
            randnum = random.randint(1, len(datalist))
            if randnum not in dataset:
                dataset.add(randnum)
                break

        with open(logfilename, 'a') as f:
            f.write(datalist[randnum])

        time.sleep(5)
        # 每几秒才有一个数据

def all_log(filename, logfile):
    datalist = []

    with open(filename, 'r') as file:
        for line in file.readlines():
            datalist.append(line)

    with open(logfilename,'a') as f:
        for i in datalist:
            f.write(i)



if __name__ == '__main__':
    print('这是一个日志生成器')
    filename = '/home/zzh/zzh/Program/Recommend_System/ml-1m/test_ratings.dat'
    logfilename = '/home/zzh/zzh/Program/Recommend_System/logger/data.log'

    generate_log(filename, logfilename)
    # all_log(filename,logfilename)
    # all_log('/home/zzh/zzh/Program/Recommend_System/ml-1m/test_ratings2.dat',logfilename)