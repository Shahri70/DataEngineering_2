import schedule
import time
import subprocess

def run_target_file_min():
    subprocess.run(["python", "Data_Updating.py"])
    subprocess.run(["python", "prediction.py"])
def run_target_file_day():
    subprocess.run(["python", "Model_Update_2d.py"])
schedule.every(1).minutes.do(run_target_file_min)
schedule.every(2).days.do(run_target_file_day)
while True:
    schedule.run_pending()
    time.sleep(1)
