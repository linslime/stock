"""
针对某一随机测略的多次回测结果，求得最终收益的概率密度分布
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt


def load_sample_data(sample_data_path):
    """
    函数功能 : 加载样本数据，返回一个有序列表

    入参 :
    sample_data_path:样本数据地址
    """
    data = pd.read_csv(filepath_or_buffer=sample_data_path, header=None)
    data = data.values
    data = list(map(list, zip(*data)))[0]
    data.sort()
    return data


def analyse(data, start, step):
    """
    函数功能 : 分析样本数据，得到最终受益的概率密度函数

    入参介绍 :
    data : 样本数据
    start : 概率分布统计起始位置
    step : 样本数据统计间隔

    出参介绍 :
    x : 利润
    y : 概率密度
    """
    temp = []
    x = []
    y = []
    i = 1
    j = 0
    while j < len(data):
        if start + i * step >= data[j]:
            temp.append(data[j])
            j += 1
        else :
            y.append(len(temp)/ len(data) / step)
            if len(temp) == 0:
                x.append(start + (i - 0.5) * step)
            else:
                x.append(np.mean(temp))
            i += 1
            temp = []
    y.append(len(temp)/ len(data) / step)
    x.append(np.mean(temp))
    return x, y


if __name__ == "__main__":

    start = 0
    step = 0.1
    file_path = "./data/sample1.csv"
    data = load_sample_data(file_path)
    x, y = analyse(data, start, step)

    plt.rcParams['font.sans-serif'] = ['SimHei']
    plt.rcParams['axes.unicode_minus'] = False
    plt.plot(x[:int(len(x)/18)], y[:int(len(y)/18)], color="red" ,linewidth=1.0, linestyle="-")  # 将散点连在一起
    plt.xlabel('利润')
    plt.ylabel('密度')
    plt.show()
