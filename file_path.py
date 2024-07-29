"""
作为全局变量，存放所有文件的目录
"""


class FilePath:
    # 股票数据目录
    stock_daily_data_path = './data/stock_daily_data/'
    # 每只股票的交易日
    trade_date = './data/trade_date.csv'
    # 特征文件目录
    feature_data = './data/feature_data.csv'
    # 离散化标签的地址
    discretized_feature_path = './data/discretized_feature.csv'