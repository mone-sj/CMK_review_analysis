import pandas as pd
from db import *
#import emp_class
from keys.key import *
from gp import *

anal00_df = db.anal00_G()
print(anal00_df)
key_df =db.TB_join_G(anal00_df)
print(key_df)
#key_df.to_csv('220302_anal00_df.csv',encoding='utf-8-sig')
anal03=total(key_df)
#anal02=emo(not_anal_df)
anal03.to_csv('./etc/result/220302_GLOWPICK_anal03.csv',encoding='utf-8-sig')
