import time
import traceback
from datetime import datetime

class TestError(Exception):
    def __init__(self, e:str):
        now = datetime.now()
        current_time = time.strftime("%Y.%m.%d/%H:%M:%S", time.localtime(time.time()))
        now = now.strftime("%Y%m%d-%H-%M")
        self.value=traceback.format_exc()
        self.error_content=e
        with open(f"./etc/log/{now} Log.txt", "a") as f: # 경로설정
            f.write(f"[{current_time}] - {self.error_content}\n{self.value}\n")
        print("실행됨")
    
    def __str__(self):
        return self.value

def exe_time(func_name,start_time):
    total=(time.time()-start_time)/60
    with open(f"./etc/log/time_log.txt", "a") as f:
        f.write(f"{func_name} 실행시간: {total}분\n")