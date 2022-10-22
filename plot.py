
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from pylab import *
rcParams['axes.unicode_minus'] = False
rcParams['font.sans-serif'] = ['Simhei']

data_1 = [1/130, 1/140, 1/240, 1/100,1/130]
data_2 = [1/30, 1/40, 1/70, 1/100,1/30]
data_3 = [1/3600, 1/2000, 1/1000, 1/700, 1/4800]
data_4 = [1/1360, 1/1400, 1/1500, 1/700, 1/1480]
data = pd.DataFrame({"2 Node,3%": data_1, "2 Node,30%": data_2, "6 Node,3%": data_3, "6 Node,30%": data_4})
df = pd.DataFrame(data)
df.plot.box()
plt.xlabel("",fontsize=16)
plt.ylabel("False Positive Rate ",fontsize=16)
# plt.grid(linestyle="--", alpha=0.8)
print(df.describe())
plt.show()
