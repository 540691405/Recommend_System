import random


def readfile(filename):
    datalist = []
    with open(filename, 'r') as file:
        for line in file.readlines():
            datalist.append(line)
    print(datalist[1])
    print(type(datalist[1]))
    return datalist


def random_split(datalist: list, train_Ratio, test_Ratio, test2_Ratio):
    if train_Ratio + test_Ratio + test2_Ratio != 1:
        print('wrong Ratio')
        return 0

    random.shuffle(datalist)
    # 打乱list
    length = len(datalist)
    train_num = int(length * train_Ratio)
    test_num = int(length * test_Ratio)
    if length == 0 or train_num < 1 or test_num < 1:
        return [], []
    trainlist = datalist[:train_num]
    testlist = datalist[train_num:train_num + test_num]

    testlist2 = datalist[train_num + test_num:]

    return trainlist, testlist, testlist2


def writefile(trainlist, trainfile, testlist, testfile, testlist2, testfile2):
    with open(trainfile, 'w') as f:
        for item in trainlist:
            f.write(item)

    with open(testfile, 'w') as f:
        for item in testlist:
            f.write(item)

    with open(testfile2, 'w') as f:
        for item in testlist2:
            f.write(item)


if __name__ == '__main__':
    print('这是一个随机切分rating的程序')
    filename = '/home/zzh/zzh/Program/Recommend_System/ItemCF/ml-1m/ratings.dat'

    trainfile = '/home/zzh/zzh/Program/Recommend_System/ItemCF/ml-1m/train_ratings.dat'
    testfile = '/home/zzh/zzh/Program/Recommend_System/ItemCF/ml-1m/test_ratings.dat'
    testfile2 = '/home/zzh/zzh/Program/Recommend_System/ItemCF/ml-1m/test_ratings2.dat'

    datalist = readfile(filename)
    trainlist, testlist, testlist2 = random_split(datalist, 0.8, 0.1, 0.1)
    writefile(trainlist, trainfile, testlist, testfile, testlist2, testfile2)
