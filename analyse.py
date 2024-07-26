"""
针对某一随机测略的多次回测结果，求得最终收益的概率密度分布
"""
import multiprocessing
import os
import pandas as pd
import numpy as np
import re


def get_sample_data_path(sample_data_path):
    """
    函数功能 : 得到所有待处理数据的目录

    入参 :
    sample_data_path : 待处理数据文件目录

    出参 :
    stock_path_list : 所有待处理数据的目录
    """
    stock_code_list = os.listdir(sample_data_path)
    sample_data_path_list = [sample_data_path + stock_code for stock_code in stock_code_list]
    return sample_data_path_list


def load_sample_data(sample_data_path):
    """
    函数功能 : 加载样本数据，返回一个有序列表

    入参 :
    sample_data_path:样本数据地址
    """
    data = pd.read_csv(filepath_or_buffer=sample_data_path, index_col=0, encoding='GBK')
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
    data = sorted(data['value'].tolist())
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


def complete_one_task(sample_data_path):
    """
    函数功能 ： 完成一个样本数据的计算结果

    入参 :
    sample_data_path : 样本数据的目录

    出参 :
    stock_code : 股票代码
    mean : 多测回测结果的平均值
    std : 多次回测结果的标准差
    """
    sample_data = load_sample_data(sample_data_path)
    mean = sample_data['value'].values.mean()
    std = sample_data['value'].values.std()
    stock_code = re.split(pattern='[\./]', string=sample_data_path)[-2]
    return stock_code, mean, std


def run(sample_data_path_queue, result_queue, lock):
    """
    函数功能 ： 多进程处理样本数据
    :param sample_data_path_queue: 样本数据目录队列
    :param result_queue: 结果队列，数据处理完后放入
    :param lock: 锁，用于互斥访问样本数据目录队列
    :return: 无
    """
    while True:
        lock.acquire()
        if not sample_data_path_queue.empty():
            sample_data_path = sample_data_path_queue.get()
            lock.release()
            result = complete_one_task(sample_data_path)
            result_queue.put(result)
        else:
            lock.release()
            break


if __name__ == "__main__":
    # 样本数据目录
    sample_data_path = "./data/random_buy_and_sell/"
    # 获取所有样本数据文件目录
    sample_data_path_list = get_sample_data_path(sample_data_path)

    # 管理共享资源
    manager = multiprocessing.Manager()

    # 创建样本数据目录共享队列
    sample_data_path_queue = manager.Queue()
    for sample_data_path in sample_data_path_list:
        sample_data_path_queue.put(sample_data_path)

    # 创建处理结果共享队列
    result_queue = manager.Queue()
    # 锁, 用于同步样本数据共享队列
    lock = manager.Lock()

    # 多进程完成任务
    processes = []
    for i in range(8):
        process = multiprocessing.Process(target=run, args=(sample_data_path_queue, result_queue, lock))
        process.start()
        processes.append(process)
    for process in processes:
        process.join()

    # 股票代码列表
    stock_code_list = []
    # 平均值
    mean_list = []
    # 标准差
    std_list = []

    # 将队列中的数据放置到列表中
    while result_queue.qsize() > 0:
        result = result_queue.get()
        stock_code_list.append(result[0])
        mean_list.append(result[1])
        std_list.append(result[2])
    # 构建pandas
    results = pd.DataFrame(data={'stock_code': stock_code_list, 'mean': mean_list, 'std': std_list})
    # 保存数据
    results.to_csv("./data/random_buy_and_sell_result.csv")
    print(results)