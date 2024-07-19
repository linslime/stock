import akshare as ak
from multiprocessing import Process, Lock, Value

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
        stock_daily_data.to_csv('./data/stock_daily_data/' + stock_code + '.csv', encoding='utf_8')

if __name__ == "__main__":
    stock_zh_a_spot_em_df = ak.stock_zh_a_spot_em()
    stock_code_list = stock_zh_a_spot_em_df['代码'].tolist()

    index = Value('i', 0)  # 定义一个共享整数
    lock = Lock()
    results = []

    for i in range(20):
        result = Process(target=get_stock_daily, args=(stock_code_list, index, lock, ))
        result.start()
        result.join()
        results.append(result)




