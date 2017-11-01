#这一版的不同在于：将每个电池的图分开画，单独保存
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
# from scipy import interpolate
from scipy.interpolate import spline
import os
import time
#plt.rcParams['font.sas-serig']=['SimHei'] #用来正常显示中文标签
#plt.rcParams['axes.unicode_minus']=False #用来正常显示负号
#对于预处理好的文件，删除其中电流与电压等于0的行（是否可以删除电流大于0的行，即放电的行）
def delInvalidRow(df):
    df=df[df['soc']!=0]
    df=df[df['current']!=0]
    return df[['id','time','current','soc','voltage']]

#对每一个电池的数据按照发送时间进行排序
#首先将时间字符串转换为秒
def datestr2secs(datestr):
    tmlist = []
    array = datestr.strip().split(' ')
    array1 = array[0].split('-')
    array2 = array[1].split(':')
    for v in array1:
        tmlist.append(int(v))
    for v in array2:
        tmlist.append(int(v))
    tmlist.append(0)
    tmlist.append(0)
    tmlist.append(0)
    #print(tmlist)
    if len(tmlist) != 9:
        return 0
    return int(time.mktime(tuple(tmlist)))

#找到相邻两个SOC（不同数值的），分别找到其最后一次出现的index
def GraphIndex(df):
    i=0
    indexList=[]
    while i<len(df)-1:
        if df['soc'][i+1]==df['soc'][i]:
            i=i+1
        else:
            indexList.append(i)
            i=i+1
    return indexList

filePath = 'D:\csv-LK'
picturePath = 'D:\picture'
ReprocessFilePath = 'D:\ReprocessCsv'
files = os.listdir(filePath)
undoFiles = []
for file in files:
    print("开始处理："+file)
    #data = pd.read_csv('{0}\{1}'.format(filePath, file), error_bad_lines=False, header=None, encoding='gbk', iterator=True, chunksize=20000000,low_memory=False)
    #对于大文件，设置读写参数
    mydata = []
    for chunk in pd.read_csv('{0}\{1}'.format(filePath, file), error_bad_lines=False, header=None, encoding='gbk', iterator=True, chunksize=20000000,low_memory=False):
        mydata.append(chunk)
    data = pd.concat(mydata, axis=0)
    del mydata

    '''with open('{0}\{1}'.format(filePath, file),'r') as bigfile:
        for line in bigfile:
            arr=line.split(',')
            arr.remove(arr[0])
            arr.remove(len(arr)-1)
    '''

    # 删除最后一列，因为是空值
    data.drop(len(data.columns) - 1, axis=1)
    # 如果单电池的电压存在Nan，则填充为0
    data = data.fillna(0)
    #print(data.head())
    # 从第38列开始是单电池的电压
    data['voltage'] = [0 for i in range(len(data))]
    for i in range(len(data)):
        data['voltage'][i] = format(sum(data[data.columns[38:]].ix[i, :]), '.3f')
    data = data[[1, 2, 4, 5, 6, 'voltage']]
    data.columns = ['id', 'time', 'current', 'soc', 'OldVoltage', 'voltage']
    # 判断是否单电压之和等于0，不是则进行后续处理
    if any(data['voltage'] == format(0, '.3f')):
        undoFiles.append(file)
        print("不处理文件：" + file)
        continue  # 跳出此次循环
    # 判断是否电压和与给定的总电压之差是否大于100，是则不进行后续处理，否则进行后续处理
    if any(data['voltage'].astype('float') - data['OldVoltage'].astype('float') > 100):
        undoFiles.append(file)
        print("不处理文件：" + file)
        continue  # 跳出此次循环
    df = delInvalidRow(data)
    df.index = range(len(df))
    df['time_id'] = [0 for i in range(len(df))]
    for i in range(len(df)):
        df['time_id'][i] = datestr2secs(df['time'][i])
    df = df.sort(columns='time_id')
    df.index = range(len(df))
    # 求SOC与voltage相邻电压之差
    indexList = GraphIndex(df)
    # indexList的长度小于10，跳过
    if len(indexList) < 10:
        undoFiles.append(file)
        print("不处理文件：" + file)
        continue  # 跳出此次循环
    cleanDf = df[df.index.isin(indexList)]
    length = len(cleanDf)
    cleanDf.index = range(length)
    cleanDf['deltaSoc'] = [0 for i in range(length)]
    cleanDf['deltaVoltage'] = [0 for i in range(length)]
    for i in range(length - 1):
        cleanDf['deltaSoc'][i + 1] = cleanDf['soc'][i + 1] - cleanDf['soc'][i]
        cleanDf['deltaVoltage'][i + 1] = format(float(cleanDf['voltage'][i + 1]) - float(cleanDf['voltage'][i]), '.3f')
    cleanDf['x'] = cleanDf['voltage']
    cleanDf['y'] = [0 for i in range(len(cleanDf))]
    for i in range(1, len(cleanDf)):
        #当deltaVoltage=0时，进行特殊处理
        if cleanDf['deltaVoltage'][i] == format(0, '.3f'):
            cleanDf['y'][i] = format(0, '.3f')
        else:
            cleanDf['y'][i] = format(float(cleanDf['deltaSoc'][i]) / float(cleanDf['deltaVoltage'][i]), '.3f')
    # 将在此阶段得到的数据保存
    cleanDf.to_csv('{0}\{1}.{2}'.format(ReprocessFilePath, cleanDf['id'][0], 'csv'), sep=',')

    cleanDf['InCharge'] = [0 for i in range(len(cleanDf))]
    i = 1
    while i < len(cleanDf):
        # 首先判断电流current第一个值是否是大于0，如果是则表示放电，对InCharge值赋予-1，否则赋予1
        if cleanDf['current'][i] > 0:
            cleanDf['InCharge'][i] = -1
        else:
            cleanDf['InCharge'][i] = 1
        i = i + 1

    # 找出每段充电（放电）的起始index并表示出属于充电还是放电
    beginIndex = []
    endIndex = []
    stage = []
    j = 2
    while j < len(cleanDf):
        if j == 2:
            beginIndex.append(j - 1)
        if cleanDf['InCharge'][j] == cleanDf['InCharge'][j - 1]:
            j = j + 1
        else:
            endIndex.append(j - 1)
            beginIndex.append(j)
            stage.append(cleanDf['InCharge'][j - 1])
            j = j + 1
    endIndex.append(len(cleanDf) - 1)
    stage.append(cleanDf['InCharge'][len(cleanDf) - 1])

    # 作图时只画出样本量大于10的阶段
    Index = []
    for i in range(len(beginIndex)):
        if endIndex[i] - beginIndex[i] + 1 >= 10:
            Index.append(i)

    #plt.figure(figsize=(10, 50))
    os.makedirs('{0}\{1}\{2}'.format(picturePath, cleanDf['id'][0], 'charge'))
    os.makedirs('{0}\{1}\{2}'.format(picturePath, cleanDf['id'][0], 'discharge'))
    NumOfGraph = len(Index)
    #plt.subplots_adjust(left=0.08, right=0.95, wspace=0.25, hspace=0.95)
    #plt.grid(True)
    for i in range(1, NumOfGraph+1):
        #plt.subplot(NumOfGraph, 1, i)
        x = list(cleanDf['x'][beginIndex[Index[i-1]]:endIndex[Index[i-1]]])
        y = list(cleanDf['y'][beginIndex[Index[i-1]]:endIndex[Index[i-1]]])
        # 插值函数：是通过已知的离散数据求未知数据的方法。
        # 与拟合不同的是，它要求曲线通过所有的已知数据。SciPy的interpolate模块提供了许多对数据进行插值运算的函数。
        # f = interpolate.interp1d(x, y, kind='cubic')
        # newy=f(x)
        # xnew=np.linspace(float(min(x)),float(max(x)),300)#300 represents number of points to make between x.min and x.max
        # y_smooth=spline(x,y,xnew)
        #plt.plot(x, y)
        if stage[i-1] == 1:
            plt.figure()
            plt.title('Charge')
            xnew = np.linspace(float(min(x)), float(max(x)), 300)
            y = list(map(lambda x: float(x), y))
            ynew = spline(x, y, xnew)
            # 设置横坐标与纵坐标的范围
            #plt.xlim(float(cleanDf['x'].min()) * 1.1, float(cleanDf['x'].max()) * 1.1)
            #plt.ylim((cleanDf['y'].astype(float).min()) * 1.1, (cleanDf['y'].astype(float).max()) * 1.1)
            plt.plot(xnew, ynew, color="red", linewidth=1.5, linestyle="-")
            plt.savefig('{0}\{1}\{2}\{3}-{4}.{5}'.format(picturePath, cleanDf['id'][0], 'charge', cleanDf['id'][0], str(i), 'png'))
        elif stage[i-1] == -1:
            plt.figure()
            plt.title('DisCharge')
            xnew = np.linspace(float(min(x)), float(max(x)), 300)
            y = list(map(lambda x: float(x), y))
            ynew = spline(x, y, xnew)
            # 设置横坐标与纵坐标的范围
            #plt.xlim(float(cleanDf['x'].min()) * 1.1, float(cleanDf['x'].max()) * 1.1)
            #plt.ylim((cleanDf['y'].astype(float).min()) * 1.1, (cleanDf['y'].astype(float).max()) * 1.1)
            plt.plot(xnew, ynew, color="blue", linewidth=1.5, linestyle="-")
            plt.savefig('{0}\{1}\{2}\{3}-{4}.{5}'.format(picturePath, cleanDf['id'][0], 'discharge', cleanDf['id'][0], str(i), 'png'))
    print(file+"处理结束")
undoFiles.to_csv('{0}\{1}.{2}'.format(filePath, 'undoFiles', 'csv'), encoding='gbk')