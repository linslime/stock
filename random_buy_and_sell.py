"""
策略：随机买入卖出股票
"""

import random
import pandas as pd
import multiprocessing
import os
import re


def get_stock_path_list(stock_daily_data_file_path, result_data_file_path):
    """
    函数功能 : 根据股票目录寻找所有股票文件目录

    入参 :
    file_path : 表示任务文件路径

    出参 :
    stock_path_list : 股票文件目录列表
    """
    # 将全部任务减去已完成的任务，得到待完成任务
    stock_code_list = set(os.listdir(stock_daily_data_file_path)) - set(os.listdir(result_data_file_path))
    stock_path_list = [stock_daily_data_file_path + stock_code for stock_code in stock_code_list]
    return stock_path_list


def get_one_stock_daily_data(data_path):
    """
    函数功能 : 根据股票文件目录读取股票文件

    入参 :
    data_path : 股票文件目录

    出参 :
    one_stock_daily_data : 股票文件
    """
    one_stock_daily_data = pd.read_csv(data_path, index_col=0, encoding='GBK')
    return one_stock_daily_data


def get_buy_and_sell_time(data_number, rate):
    """
    函数功能 : 随机获取股票买入卖出时间

    入参 :
    data_number : 股票数据个数
    rate : 买入卖出时机个数

    出参 :
    'buy_and_sell_time' : 买入卖出时间
    """
    buy_and_sell_number = round(data_number * rate)
    if buy_and_sell_number % 2 == 1:
        buy_and_sell_number += 1
    buy_and_sell_time = sorted(random.sample(range(1, data_number), buy_and_sell_number))
    return buy_and_sell_time


def get_final_value(buy_and_sell_time, stock_data):
    """
    函数功能 : 通过买入和卖出数据和股票数据得到最终值

    入参 :
    buy_and_sell_time : 买入和卖出时间
    stock_data : 股票数据

    出参 :
    final_value : 股票交易后的最终数据
    """
    final_value = stock_data[0]
    for time in range(0, len(buy_and_sell_time), 2):
        final_value = final_value * stock_data[buy_and_sell_time[time + 1]] / stock_data[buy_and_sell_time[time]]
    return final_value


def save(results, stock_code):
    """
    函数功能 : 将多次重复回测的结果保存

    入参 :
    results : 多次回测结果
    stock_code : 股票代码
    """
    results = pd.DataFrame(results, columns=['value'], dtype=float)
    results.to_csv(path_or_buf='./data/random_buy_and_sell/' + stock_code + '.csv', encoding='GBK')


def complete_one_task(stock_path):
    """
    函数功能 : 读取股票数据，进行回测

    入参 : 股票数据目录
    """
    # 获取股票数据
    one_stock_daily_data = get_one_stock_daily_data(stock_path)
    # 列名，将收盘价当作当天股票的价格
    columns_name = '收盘'
    # 判断这一股票数据的是否有columns_name这一列名，若没有则跳过
    if columns_name not in one_stock_daily_data.columns:
        print(stock_path)
        return
    # 把股票的每日收盘价当作股票的当天价格
    stock_value = one_stock_daily_data[columns_name]
    # 结果列表
    results = []
    for i in range(10000):
        # 获取买入和卖出的时间
        buy_and_sell_time = get_buy_and_sell_time(len(stock_value), 0.1)
        # 获取股票买卖的最终价值
        final_value = get_final_value(buy_and_sell_time, stock_value)
        # 将结果放入results中
        results.append(final_value)
    # 用正则表达式获取股票代码
    stock_code = re.split(pattern='[\./]', string=stock_path)[-2]
    # 保存结果
    save(results, stock_code)


def run(stock_path_list):
    """
    函数功能 :

    入参 :
    stock_path_list: 共享队列，内容为股票文件路径
    """
    while not stock_path_list.empty():
        stock_path = stock_path_list.get()
        complete_one_task(stock_path)


if __name__ == "__main__":

    # 股票数据目录
    stock_daily_data_file_path = './data/stock_daily_data/'
    # 回测结果数据目录
    result_data_file_path = './data/random_buy_and_sell/'

    # 获取股票文件目录
    stock_path_list = get_stock_path_list(stock_daily_data_file_path, result_data_file_path)

    # 将股票文件目录放入队列
    queue = multiprocessing.Queue()
    for stock_path in stock_path_list:
        queue.put(stock_path)

    # 多进程处理
    processes = []
    for i in range(10):
        process = multiprocessing.Process(target=run, args=(queue,))
        process.start()
        processes.append(process)

    for process in processes:
        process.join()



