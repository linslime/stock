import pandas as pd
import numpy as np
import math
import matplotlib.pyplot as plt

#读数据
def read():
    df = pd.read_excel(r'D:\Desktop\one.xlsx')
    data = df.values.flatten()
    return data

def getDate(data,n,m):
    y = data[n + m - 1:]
    x = np.zeros((len(data) - n - m + 1, n), dtype=float)
    t = np.ones(len(data) - n - m + 1)
    for i in range(0,len(data) - n - m + 1):
        x[i] = data[i:i + n]
    x = np.insert(x,n,t,axis=1)
    return x,y

#主函数
if __name__ == "__main__":
    data = read()
    x,y = getDate(data,5,5)

    w = np.linalg.inv(x.T@x)@x.T@y
    y_ = x@w

    d = (y_ - y)
    e = d.T@d
    print(1/len(y) * math.pow(e,0.5))
    d = (y_ - y)/y



    plt.rcParams['font.sans-serif'] = ['SimHei']
    plt.rcParams['axes.unicode_minus'] = False
    plt.plot(range(len(y)), y, color="red" ,linewidth=1.0, linestyle="-")  # 将散点连在一起
    plt.plot(range(len(y)), y_, color="blue", linewidth=1.0, linestyle="-")
    plt.xlabel('时间')
    plt.ylabel('股价')
    plt.show()