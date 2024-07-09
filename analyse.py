import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

class analyse():
    def __init__(self):
        self.data = self.load()
        self.step = 0.1
        self.start = 0
        self.y = []
        self.x = []

    def load(self):
        data = pd.read_csv("sample3.csv")
        data = data.values
        data = list(map(list, zip(*data)))[0]
        data.sort()
        return data

    def statistics(self):
        temp = []
        i = 1
        j = 0
        while j < len(self.data):
            if self.start + i * self.step >= self.data[j]:
                temp.append(self.data[j])
                j += 1
            else :
                self.y.append(len(temp)/ len(self.data) / self.step)
                if len(temp) == 0:
                    self.x.append(self.start + (i - 0.5) * self.step)
                else:
                    self.x.append(np.mean(temp))
                i += 1
                temp = []
        self.y.append(len(temp)/ len(self.data) / self.step)
        self.x.append(np.mean(temp))

if __name__ == "__main__":
    a = analyse()
    a.statistics()

    plt.rcParams['font.sans-serif'] = ['SimHei']
    plt.rcParams['axes.unicode_minus'] = False
    plt.plot(a.x[:int(len(a.x)/18)], a.y[:int(len(a.y)/18)], color="red" ,linewidth=1.0, linestyle="-")  # 将散点连在一起
    plt.xlabel('利润')
    plt.ylabel('密度')
    plt.show()

    print(max(a.y))
    print(a.x[a.y.index(max(a.y))])
    print(len([i for i in a.data if i >= 30]))
