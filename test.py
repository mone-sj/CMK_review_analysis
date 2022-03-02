import pandas as pd
from db import *
#import emp_class
from keys.key import *
from gp import *

# anal00_df = db.anal00_G()
# print(anal00_df)
# key_df =db.TB_join_G(anal00_df)
# print(key_df)
# #key_df.to_csv('220302_anal00_df.csv',encoding='utf-8-sig')
# #anal03=total(key_df)
# anal02=emo(key_df)
# #anal03.to_csv('./etc/result/220302_1244_GLOWPICK_anal03.csv',encoding='utf-8-sig', index=False)
# anal02.to_csv('./etc/result/220302_1321_GLOWPICK_anal02.csv',encoding='utf-8-sig', index=False)

anal02=pd.read_csv('./etc/result/20220302/220302_1329_G_anal02_result.csv')
TB_anal02_insert(anal02)
anal03=pd.read_csv('./etc/result/20220302/220302_1254_G_anal03_result.csv')
TB_anal03_insert(anal03)