"""
特征工程，给每只股票添加特征
"""
import multiprocessing
import random
import re
from file_path import *
import pandas as pd
import os
import math


def get_stock_path_list(stock_daily_data_file_path):
    """
    函数功能 : 根据股票目录寻找所有股票文件目录

    入参 :
    file_path : 表示任务文件路径

    出参 :
    stock_path_list : 股票文件目录列表
    """
    # 将全部任务减去已完成的任务，得到待完成任务
    stock_code_list = os.listdir(stock_daily_data_file_path)
    stock_path_list = [stock_daily_data_file_path + stock_code for stock_code in stock_code_list]
    return stock_path_list


def get_one_data(data_path):
    """
    函数功能 : 根据数据文件目录读取数据文件

    入参 :
    data_path : 数据文件目录

    出参 :
    one_stock_daily_data : 数据文件的pandas
    """
    data = pd.read_csv(data_path, index_col=0, encoding='GBK',dtype={'':str})
    return data


def get_value_mean(stock_data: pd.DataFrame) -> float:
    """
    函数功能：将一只股票的收盘价当作当天的股价，并计算平均股价
    :param stock_data: 该股票的每日数据
    :return: 收盘价的平均值
    """
    value_mean = stock_data['收盘'].mean()
    return value_mean


def get_volume_mean(stock_data: pd.DataFrame) -> float:
    """
    函数功能：计算一只股票的成交量的平均值
    :param stock_data: 该股票的每日数据
    :return: 成交量的平均值
    """
    volume_mean = stock_data['成交量'].mean()
    return volume_mean


def get_turnover_mean(stock_data: pd.DataFrame) -> float:
    """
    函数功能：计算成交额的平均值
    :param stock_data: 该股票数据
    :return: 成交额平均值
    """
    turnover_mean = stock_data['成交额'].mean()
    return turnover_mean


def normalize_min_max(stock_feature: pd.DataFrame) -> pd.DataFrame:
    """
    函数功能：将值进行最大最小归一化
    :param stock_feature: 值
    :return: 归一化后的结果
    """
    return (stock_feature - stock_feature.min())/(stock_feature.max() - stock_feature.min())


def get_discretized_feature(stock_feature: pd.DataFrame) -> pd.DataFrame:
    """
    函数功能：将特征值离散化，先归一化到0--1，然后取对数
    :param stock_feature:股票特征值
    :return:离散化特征值
    """
    discretized_feature = pd.DataFrame(index=stock_feature.index, columns=stock_feature.columns)
    normalized_features = normalize_min_max(stock_feature)
    for column in discretized_feature.columns:
        for index in discretized_feature.index:
            if normalized_features.loc[index, column] != 0:
                discretized_feature.loc[index, column] = int(abs(math.log(normalized_features.loc[index, column], 10)))
            else:
                discretized_feature.loc[index, column] = int(abs(math.log(normalized_features[column].nsmallest(2).iloc[-1], 10)))

    discretized_feature.to_csv(path_or_buf=FilePath.discretized_feature_path, encoding='GBK')
    return discretized_feature


def check_discretized_feature(discretized_feature: pd.DataFrame, normalized_features: pd.DataFrame) -> None:
    """
    函数功能:检查离散标签是否正确
    :param discretized_feature:离散化标签
    :param normalized_features:
    :return:
    """

    for index in discretized_feature.index.values:
        for column in discretized_feature.columns.values:
            normalize = normalized_features[column][index]
            discretize = discretized_feature[column][index]

            if not (normalize != 0.0 and normalize <= math.pow(10, -1 * discretize) and normalize > math.pow(10, -1 * (discretize + 1))) and normalize != 0.0:
                print(index, column)
                print(normalize)
                print(discretize)


def run(stock_path_queue, lock, result_queue):
    """
    函数功能：用于多进程完成特征值提取
    :param stock_path_queue: 共享队列，股票数据目录
    :param lock: 锁，用户互斥访问共享队列
    :param result_queue: 共享队列，将结果放入其中
    :return:
    """
    while True:
        lock.acquire()
        if not stock_path_queue.empty():
            stock_path = stock_path_queue.get()
            lock.release()
            stock_code = re.split(pattern='[\./]', string=stock_path)[-2]
            stock_data = get_one_data(stock_path)

            value_mean = get_value_mean(stock_data)
            volume_mean = get_volume_mean(stock_data)
            turnover_mean = get_turnover_mean(stock_data)

            result = pd.Series(data=(value_mean, volume_mean, turnover_mean), index=['价格', '成交量', '成交额'], name=stock_code)
            result_queue.put(result)
        else:
            lock.release()
            break


def get_features():
    """
    多进程完成特征值计算，并保存结果
    :return: 无
    """
    stock_path_list = get_stock_path_list(FilePath.stock_daily_data_path)

    stock_path_queue = multiprocessing.Manager().Queue()
    for stock_path in stock_path_list:
        stock_path_queue.put(stock_path)

    lock = multiprocessing.Manager().Lock()

    result_queue = multiprocessing.Manager().Queue()

    processes = []
    for i in range(8):
        process = multiprocessing.Process(target=run, args=(stock_path_queue, lock, result_queue))
        process.start()
        processes.append(process)
    for process in processes:
        process.join()

    results = []
    while not result_queue.empty():
        result = result_queue.get()
        results.append(result)
    features = pd.DataFrame(results)
    features.sort_index(inplace=True)
    features.to_csv(path_or_buf=FilePath.feature_data, encoding='GBK', index_label='stock_code')


def get_classification():
    """
    函数功能，分类，取元素数量最大的一类
    :return:
    """
    discretized_feature = pd.read_csv(FilePath.discretized_feature_path, encoding='GBK', index_col=0, dtype={'stock_code': str})
    classification = {}
    for index in discretized_feature.index.values:
        key = str(discretized_feature.loc[index, '价格']) + '_' + str(discretized_feature.loc[index, '成交量']) + '_' + str(discretized_feature.loc[index, '成交额'])
        if key not in classification:
            classification[key] = []
        classification[key].append(index)
    keys = [key for key in classification.keys()]
    value = [len(classification[key]) for key in keys]
    max_index = value.index(max(value))
    return classification[keys[max_index]]


if __name__ == '__main__':
    # get_features()
    # features = pd.read_csv(FilePath.feature_data, encoding='GBK', index_col=0, dtype={'stock_code': str})
    # discretized_feature = get_discretized_feature(features)
    # check_discretized_feature(discretized_feature, normalize_min_max(features))
    normal_classification = get_classification()
    stock_code_list = random.sample(normal_classification, 100)
    stock_code_list = pd.DataFrame(data=stock_code_list, columns=['stock_code'])
    stock_code_list.to_csv(FilePath.part_stock_code, index_label='index')
    print(stock_code_list)
