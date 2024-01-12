import pandas as pd
import numpy as np
import math
import matplotlib.pyplot as plt
import torch
import torch.nn as nn
import torch.nn.functional as F
from torch.autograd import Variable

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
    # print(x)
    # x = np.insert(x,n,t,axis=1)
    return x,y

class Net(nn.Module):
    def __init__(self,n_input,n_hidden,n_output):
        super(Net,self).__init__()
        self.hidden1 = nn.Linear(n_input,n_hidden)
        self.hidden2 = nn.Linear(n_hidden,n_hidden)
        self.hidden3 = nn.Linear(n_hidden, n_hidden)
        self.predict = nn.Linear(n_hidden,n_output)
    def forward(self,input):
        out = self.hidden1(input)
        out = F.tanh(out)
        out = self.hidden2(out)
        out = F.tanh(out)
        out = self.hidden3(out)
        out = F.tanh(out)
        out =self.predict(out)

        return out

#主函数
if __name__ == "__main__":
    data = read()
    x,y = getDate(data,5,1)
    y = y.reshape(-1,1)
    y = torch.tensor(y).float()
    # print(y)
    x = torch.tensor(x).float()
    # print(x)
    net = Net(5, 50, 1)

    print(x)
    print(y)

    optimizer = torch.optim.SGD(net.parameters(), lr=0.1)
    loss_func = torch.nn.L1Loss()
    prediction = net(x)
    for t in range(10000):
        prediction = net(x)
        loss = loss_func(prediction, y)
        optimizer.zero_grad()
        loss.backward()
        optimizer.step()
        print(loss)
    torch.save(net.state_dict(),"./model")
    # d = (y_ - y)
    # e = d.T@d
    # print(1/len(y) * math.pow(e,0.5))
    # d = (y_ - y)/y


    print(y)
    print(prediction)
    plt.rcParams['font.sans-serif'] = ['SimHei']
    plt.rcParams['axes.unicode_minus'] = False
    plt.plot(range(len(y)), y, color="red" ,linewidth=1.0, linestyle="-")  # 将散点连在一起
    # plt.plot(range(len(prediction)), prediction, color="blue", linewidth=1.0, linestyle="-")
    # plt.plot(range(len(y)), d, color="red", linewidth=1.0, linestyle="-")  # 将散点连在一起
    plt.xlabel('时间/s')
    plt.ylabel('位移/m')
    plt.show()