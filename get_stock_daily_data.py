'''
下载沪深京A股股票的每日数据
'''

import akshare as ak
import multiprocessing
import os
from file_path import FilePath

def get_stock_daily(stock_code):
    """
    函数功能 :根据股票代码下载股票每日数据

    入参 :
    stock_code : 股票代码
    """

    # 下载数据从上市到2024年06月30日
    end_date = '20240630'
    # 获取上市时间
    start_date = ak.stock_individual_info_em(symbol=stock_code)['value'][7]
    # 获取数据
    stock_daily_data = ak.stock_zh_a_hist(symbol=stock_code, period="daily", start_date=start_date, end_date=end_date)
    # 保存数据到本地，GBK为windows系统的默认编码方式
    stock_daily_data.to_csv(FilePath.stock_daily_data_path + stock_code + '.csv', encoding='GBK')


def run(stock_code_list, lock):
    """
    函数方法 : 多进程下载股票数据

    入参 ：
    stock_code_list : 共享队列，内容为股票代码
    """
    while True:
        lock.acquire()
        if not stock_code_list.empty():
            stock_code = stock_code_list.get()
            lock.release()
            get_stock_daily(stock_code)
        else:
            lock.release()
            break


def download_data():
    """
    函数功能：下载所有股票数据
    :return:
    """
    # 获取所有沪深京A股股票代码
    stock_zh_a_spot_em_df = ak.stock_zh_a_spot_em()
    stock_code_list = stock_zh_a_spot_em_df['代码'].tolist()

    # 获取已完成的股票代码
    completed_task_list = os.listdir(FilePath.stock_daily_data_path)
    completed_task_list = [i.split('.')[0] for i in completed_task_list]

    # 获取未完成的所有股票代码
    stock_code_list = list(set(stock_code_list) - set(completed_task_list))

    # 创建共享队列
    queue = multiprocessing.Manager().Queue()
    for stock_code in stock_code_list:
        queue.put(stock_code)

    # 创建锁，用于互斥访问队列
    lock = multiprocessing.Manager().Lock()

    # 多进程
    processes = []
    for i in range(50):
        process = multiprocessing.Process(target=run, args=(queue, lock))
        process.start()
        processes.append(process)
    for process in processes:
        process.join()


if __name__ == "__main__":
    download_data()
