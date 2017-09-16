import sys
import os


par_path=os.path.dirname(os.path.dirname(__file__))

sys.path.append(par_path+"/holoclean/models")
sys.path.append(par_path+"/holoclean")

import pandas as pd
import learning_framework as lf
import dataEngine as de

data = [
    [2, 3, 'k'],
    [1, 3, 'k'],
    [1, 3, 'k'],
    [2, 2, 'l'],
    [2, 4, 'l'],
    [2, 4, 'l'],
    [3, 2, 'f']
]
df = pd.DataFrame(data, columns=['a', 'b', 'c'])
dcCode=['a,0,b,3' , 'c,0,a,3']
noisy_cells=[(0,'a'),(3,'b')]


q=de.HolocleanData(df, noisy_cells, dcCode)
v=q.holoclean_ready_data()
l=lf.Learning(v)
print(l.learn())

