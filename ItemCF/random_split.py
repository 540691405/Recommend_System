import random


def readfile(filename):
    datalist = []
    with open(filename, 'r') as file:
        for line in file.readlines():
            datalist.append(line)
    print(datalist[1])
    print(type(datalist[1]))
    return datalist


def random_split(datalist: list, train_Ratio, test_Ratio):
    random.shuffle(datalist)
    # 打乱list
    length = len(datalist)
    train_num = int(length * train_Ratio)
    test_num = int(length * test_Ratio)
    if length == 0 or train_num < 1 or test_num < 1:
        return [], []
    trainlist = datalist[:train_num]
    testlist = datalist[train_num:train_num + test_num]
    return trainlist, testlist


def writefile(trainlist, trainfile, testlist, testfile):
    with open(trainfile, 'w') as f:
        for item in trainlist:
            f.write(item)

    with open(testfile, 'w') as f:
        for item in testlist:
            f.write(item)


if __name__ == '__main__':
    print('这是一个随机切分rating的程序')
    filename = '/home/zzh/zzh/Program/Recommend_System/ItemCF/ml-1m/ratings.dat'
    trainfile = '/home/zzh/zzh/Program/Recommend_System/ItemCF/ml-1m/train_ratings.dat'
    testfile = '/home/zzh/zzh/Program/Recommend_System/ItemCF/ml-1m/test_ratings.dat'

    datalist = readfile(filename)
    trainlist, testlist = random_split(datalist, 0.1, 0.1)
    writefile(trainlist, trainfile, testlist, testfile)
