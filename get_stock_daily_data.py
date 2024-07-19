'''
下载沪深京A股股票的每日数据
'''

import akshare as ak
from multiprocessing import Process, Lock, Value
import os

'''
stock_code_list : 表示任务列表，其中包含各个股票的代码
index : 表示任务列表的下标，是一个共享变量
lock : 锁，用来同步控制多个进程访问任务列表下标
'''
def get_stock_daily(stock_code_list, index, lock):

    end_date = '20240630'
    while True:
        lock.acquire()
        if index.value == len(stock_code_list):
            lock.release()
            break
        stock_code = stock_code_list[index.value]
        index.value += 1
        lock.release()
        
        start_date = ak.stock_individual_info_em(symbol=stock_code)['value'][7]
        stock_daily_data = ak.stock_zh_a_hist(symbol=stock_code, period="daily", start_date=start_date, end_date=end_date)

        #GBK为windows系统的默认编码方式
        stock_daily_data.to_csv('./data/stock_daily_data/' + stock_code + '.csv', encoding='GBK')


if __name__ == "__main__":
    #获取所有沪深京A股股票代码
    stock_zh_a_spot_em_df = ak.stock_zh_a_spot_em()
    stock_code_list = stock_zh_a_spot_em_df['代码'].tolist()

    #获取已完成的股票代码
    completed_task_list = os.listdir('./data/stock_daily_data/')
    completed_task_list = [i.split('.')[0] for i in completed_task_list]

    #获取未完成的所有股票代码
    stock_code_list = list(set(stock_code_list) - set(completed_task_list))

    #股票代码索引，是一个共享变量
    index = Value('i', 0)
    #控制同步互斥地访问股票代码索引
    lock = Lock()
    results = []

    for i in range(20):
        result = Process(target=get_stock_daily, args=(stock_code_list, index, lock, ))
        result.start()
        result.join()
        results.append(result)




