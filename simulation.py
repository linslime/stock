import random
import pandas as pd
import numpy as np
import csv

class simulation():
    def __init__(self):
        self.value = 1
        self.buyValue = 0
        self.sellValue = 0
        self.curve = self.read()

        self.answer = []
        self.max = 0
        self.min = 0
        self.expectation = 0
        self.standardDeviation = 0

    def read(self):
        df = pd.read_excel(r'D:\Desktop\one.xlsx')
        data = df.values.flatten()
        return data

    def getTime(self):
        n = round(len(self.curve) * self.rate)
        if n % 2 == 1:
            n += 1
        return sorted(random.sample(range(len(self.curve)),n))

    def getFinalValue(self):
        time = self.getTime()
        tempValue = self.value
        for i in range(0,len(time),2):
            tempValue = tempValue * self.curve[time[i + 1]] / self.curve[time[i]]
        return tempValue

    def simulation(self,rate,number):
        self.number = number
        self.rate = rate
        self.answer = []

        for i in range(self.number):
            self.answer.append(self.getFinalValue())

        self.max = max(self.answer)
        self.min = min(self.answer)
        self.expectation = np.mean(self.answer)
        self.standardDeviation = np.std(self.answer)

    def save(self):
        # 将结果输出
        with open("data/sample3.csv", "w", newline="") as csvfile:
            writer = csv.writer(csvfile)
            writer.writerows(list(map(list, zip(*[self.answer]))))

if __name__ == "__main__":
    s = simulation()
    s.simulation(0.14,1000000)
    s.save()
    print(s.max,s.min,s.expectation,s.standardDeviation)



