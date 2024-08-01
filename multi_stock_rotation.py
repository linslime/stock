"""
策略: 多个股票轮动
"""
import multiprocessing
import pandas as pd
from file_path import *
import matplotlib.pyplot as plt

def choice_stock(date, trade_date, stock_data, period):
    """
    函数功能：计算date这一天，应当买入的股票
    :param date: 日期
    :param trade_date:表示当天股票有没有交易 ，
    :param stock_data: 股票每日交易数据，
    :return: 返回当天应当买入的股票
    """
    trade_code = trade_date.loc[date][trade_date.loc[date] == 1]
    rise_fall = []
    rise_fall_stock = []
    for stock_code in trade_code.index:
        current_date = stock_data[stock_code]['日期'].values.tolist().index(date)
        if current_date >= period:
            period_data = stock_data[stock_code].iloc[current_date - period:current_date]
            rise_fall_stock.append(stock_code)
        elif current_date == 0:
            continue
        else:
            period_data = stock_data[stock_code].iloc[:current_date]
            rise_fall_stock.append(stock_code)
        start_value = period_data['最低'].iloc[0]
        end_value = period_data['最高'].iloc[-1]
        rise_fall.append(end_value / start_value)
    max_stock_index = rise_fall.index(max(rise_fall))
    max_stock_code = rise_fall_stock[max_stock_index]
    return max_stock_code


def run(date_queue, trade_date, stock_data, result_queue, period, lock):
    """
    多进程实现多个股票轮动
    :param date_queue:表示时间队列，包括要完成的所有时间
    :param trade_date: 交易日期
    :param stock_data: 股票交易数据
    :param result_queue: 结果队列，将结果放入其中
    :param period: 查看过去period交易日的涨幅
    :param lock: 锁，用于互斥访问队列
    :return:
    """
    while True:
        lock.acquire()
        if not date_queue.empty():
            date = date_queue.get()
            lock.release()
            stock_code = choice_stock(date, trade_date, stock_data, period)
            result_queue.put((date, stock_code))
        else:
            lock.release()
            break


def multi_stock_rotation(stock_code_list, period, start_date, end_date):
    """
    函数功能 ： 多股票轮动，在end_date和start_date时间范围内，在stock_code_list这些股票列表中，在最近的period的交易日里，谁的涨幅快就满仓谁
    :param stock_code_list: 参与轮动的股票列表
    :param period: 考虑涨幅的最近的时间天数
    :param start_date: 数据开始的时间
    :param end_date: 数据结束的时间
    :return:
    """
    trade_date = pd.read_csv(filepath_or_buffer=FilePath.trade_date, encoding='GBK', usecols=stock_code_list + ['date'], index_col='date')[start_date:end_date]
    stock_data = {}
    for stock_code in stock_code_list:
        stock_data[stock_code] = pd.read_csv(filepath_or_buffer=FilePath.stock_daily_data_path + stock_code + '.csv', encoding='GBK', index_col=0)
    # 将考虑范围以内的交易日放入队列
    date_queue = multiprocessing.Manager().Queue()
    for date in trade_date.index:
        date_queue.put(date)
    # 结果队列
    result_queue = multiprocessing.Manager().Queue()
    # 用与互斥访问队列
    lock = multiprocessing.Manager().Lock()
    # 进程
    processes = []
    for i in range(8):
        process = multiprocessing.Process(target=run, args=(date_queue, trade_date, stock_data, result_queue, period, lock))
        process.start()
        processes.append(process)

    for process in processes:
        process.join()

    results = []
    while not result_queue.empty():
        results.append(result_queue.get())
    buy_in = pd.DataFrame(results)
    buy_in.columns = ['date', 'stock_code']
    buy_in.sort_values(by='date', inplace=True)
    # 表示初始资金量
    start_value = 10000
    # 添加市值列
    buy_in['value'] = start_value
    # 计算每日市值
    for i in range(1, len(buy_in)):
        current_stock = buy_in['stock_code'].iloc[i]
        previous_stock = buy_in['stock_code'].iloc[i - 1]
        if current_stock == previous_stock:
            stock = stock_data[current_stock]
            current_value = stock.loc[stock['日期'] == buy_in['date'].iloc[i], '收盘'].values[0]
            previous_value = stock.loc[stock['日期'] == buy_in['date'].iloc[i - 1], '开盘'].values[0]
            buy_in['value'].iloc[i] = current_value / previous_value * buy_in['value'].iloc[i - 1]
        else:
            buy_in['value'].iloc[i] = buy_in['value'].iloc[i - 1]
    # 回撤的初始值为0
    start_drawdown = 0
    # 创建回撤这一列
    buy_in['drawdown'] = start_drawdown
    # 计算每日回撤
    for i in range(1, len(buy_in)):
        max_value = buy_in['value'].iloc[:i + 1].max()
        current_value = buy_in['value'].iloc[i]
        buy_in['drawdown'].iloc[i] = (max_value - current_value) / max_value
    buy_in.to_csv(path_or_buf=FilePath.multi_stock_rotation, encoding='GBK', sep='\t')
    print(buy_in)

    plt.plot(buy_in['date'].values.tolist(), buy_in['value'], color="red", linewidth=1.0, linestyle="-")
    # plt.plot(range(len(y)), y_, color="blue", linewidth=1.0, linestyle="-")
    plt.xlabel('date')
    plt.ylabel('value')
    plt.show()

    plt.plot(buy_in['date'].values.tolist(), buy_in['drawdown'], color="red", linewidth=1.0, linestyle="-")
    # plt.plot(range(len(y)), y_, color="blue", linewidth=1.0, linestyle="-")
    plt.xlabel('date')
    plt.ylabel('drawdoen')
    plt.show()


if __name__ == '__main__':
    stock_code_list = pd.read_csv(filepath_or_buffer=FilePath.part_stock_code, encoding='GBK', dtype={'stock_code': str})['stock_code'].tolist()
    period = 20
    start_date = '2017-01-01'
    end_date = '2023-12-31'
    multi_stock_rotation(stock_code_list, period, start_date, end_date)
    print(stock_code_list)