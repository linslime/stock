"""
进行数据预处理
"""
import multiprocessing
import pandas as pd
import os
import re
import numpy as np
from file_path import FilePath


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


def get_one_stock_daily_data(stock_path):
    """
    函数功能 : 根据股票文件目录读取股票文件

    入参 :
    data_path : 股票文件目录

    出参 :
    one_stock_daily_data : 股票文件
    """
    one_stock_daily_data = pd.read_csv(stock_path, index_col=0, encoding='GBK')
    return one_stock_daily_data


def remove_empty_data():
    """
    函数功能 ： 删除空白的股票数据
    :param stock_path_list: 每日股票数据目录列表
    :return:
    """
    stock_path_list = get_stock_path_list(FilePath.stock_daily_data_path)
    for stock_path in stock_path_list:
        one_stock_daily_data = get_one_stock_daily_data(stock_path)
        if one_stock_daily_data.empty:
            os.remove(stock_path)
            print(stock_path)


def get_stock_daily_data(stock_path_list):
    """
    函数功能 ： 读取所有股票的每日数据
    :param stock_path_list: 所有数据文件的目录
    :return: 所有股票数据
    """
    all_stock_daily_data = {}
    for stock_path in stock_path_list:
        stock = get_one_stock_daily_data(stock_path)
        stock_code = re.split(pattern='[\./]', string=stock_path)[-2]
        all_stock_daily_data[stock_code] = stock
    return all_stock_daily_data


def check_nan_number():
    """
    函数功能 ： 检查每个股票的数据是否有缺省值，如果有，就返回股票代码和缺省值数量
    :return: 返回有缺省值的股票代码和相应的缺省值数量
    """
    stock_path_list = get_stock_path_list(FilePath.stock_daily_data_path)
    nan_dir = {}
    for stock_path in stock_path_list:
        stock_code = re.split(pattern='[\./]', string=stock_path)[-2]
        stock_daily_data = get_one_stock_daily_data(stock_path)
        nan_number = stock_daily_data.isnull().sum().sum()
        if nan_number > 0:
            print(stock_code, nan_number)
            nan_dir[stock_code] = nan_number
    return nan_dir


def set_data_index(all_stock_daily_data):
    """
    函数功能 ： 将所有股票数据的索引改为日期
    :param all_stock_daily_data:  修改前的股票数据
    :return: all_stock_daily_data: 修改后、索引已经是日期的股票数据
    """
    for stock_code in all_stock_daily_data:
        all_stock_daily_data[stock_code].set_index('日期', inplace=True)
    return all_stock_daily_data


def complete_one_task(stock_path):
    """
    函数功能 ： 输入股票数据文件目录，读取股票数据，判断该股票的交易日有哪些
    :param stock_path: 股票数据文件目录
    :return: 一个pandas，索引为所有的交易日，值全部为1
    """
    stock_daily_data = get_one_stock_daily_data(stock_path)
    index = stock_daily_data['日期'].values
    result = pd.Series(data=np.ones(len(index), dtype=np.int8), index=index)
    return result


def run(stock_path_queue, result_queue, lock):
    """
    函数功能 ： 多进程完成各个股票的交易日，并保存结果
    :param stock_path_queue: 股票数据文件目录队列
    :param result_queue: 结果队列
    :param lock: 锁，互斥访问股票数据文件目录队列
    :return:
    """
    while True:
        lock.acquire()
        if not stock_path_queue.empty():
            stock_path = stock_path_queue.get()
            lock.release()
            result = complete_one_task(stock_path)
            stock_code = re.split(pattern='[\./]', string=stock_path)[-2]
            result_queue.put({stock_code: result})
        else:
            lock.release()
            break


def get_date_to_stock_data():
    """
    函数功能 ：多进程完成各个股票的交易日统计，并保存结果
    :return:
    """
    # 股票数据文件目录
    stock_path_list = get_stock_path_list(FilePath.stock_daily_data_path)

    # 用于待完成股票数据队列
    stock_path_queue = multiprocessing.Manager().Queue()
    for stock_path in stock_path_list:
        stock_path_queue.put(stock_path)
    # 用于放置结果数据
    result_queue = multiprocessing.Manager().Queue()

    # 用于互斥访问股票数据队列
    lock = multiprocessing.Lock()

    # 多进程完成任务
    processes = []
    for i in range(8):
        process = multiprocessing.Process(target=run, args=(stock_path_queue, result_queue, lock))
        process.start()
        processes.append(process)
    for process in processes:
        process.join()

    # 所有结果都是一个字典，将所有结果合并到一个字典里
    results = {}
    while not result_queue.empty():
        stock_daily_data = result_queue.get()
        results.update(stock_daily_data)

    # 将字典转化为pandas
    results = pd.DataFrame(data=results, dtype='int8')
    # 将结果中的nan转化为0
    results.fillna(value=0, inplace=True)
    # 将数据类型转化为整数
    results = results.astype('int8')
    # 保存结果
    results.to_csv(path_or_buf=FilePath.trade_date, encoding='GBK', index_label='date')
    return results


def check_exist_csv():
    """
    函数功能 ：判断exist.csv文件是否有问题
    :return:
    """
    # 读取exist.csv文件
    exist_csv = pd.read_csv(FilePath.trade_date, index_col=0, encoding='GBK')

    # 股票数据文件目录
    stock_path_list = get_stock_path_list(FilePath.stock_daily_data_path)

    for stock_path in stock_path_list:
        # 用正则表达式得到股票代码
        stock_code = re.split(pattern='[\./]', string=stock_path)[-2]
        # 从exist的pandas中读取该股票的交易日
        exist_csv_set = set(exist_csv[exist_csv[stock_code] == 1].index)
        # 从原始数据中获得该股票的交易日
        stock_data_set = set(get_one_stock_daily_data(stock_path)['日期'])
        # 对比这两个集合，如果不同则打印出该股票代码
        if exist_csv_set != stock_data_set:
            print(stock_code)


if __name__ == '__main__':

    # 移除所有空数据文件
    # remove_empty_data()

    # 读取全部股票数据文件
    # all_stock_daily_data = get_stock_daily_data(stock_path_list)

    # 检查每一个文件中是否有缺省值
    # nan_dir = check_nan_number()

    # 将每一只股票的日期作为索引
    # set_data_index(all_stock_daily_data)

    # 多进程完成各个股票的交易日统计，并保存结果
    get_date_to_stock_data()

    # 检查exist.csv文件是否正确
    # check_exist_csv()
    # print(all_stock_daily_data)