import os
import subprocess
import time

SILVER_TRIGGER = "data/_run_silver"
GOLD_TRIGGER = "data/_run_gold"

SILVER_JOB = "spark/jobs/bronze_to_silver.py"
GOLD_JOB = "spark/jobs/silver_to_gold.py"

def run(cmd):
    print("Running:", " ".join(cmd))
    subprocess.run(cmd, check=True)

print("Spark runner started. Waiting for Airflow triggers...")

while True:
    if os.path.exists(SILVER_TRIGGER):
        print("Silver trigger detected")
        run(["spark-submit", SILVER_JOB])
        os.remove(SILVER_TRIGGER)
        print("Silver trigger consumed (deleted)")

    if os.path.exists(GOLD_TRIGGER):
        print("Gold trigger detected")
        run(["spark-submit", GOLD_JOB])
        os.remove(GOLD_TRIGGER)
        print("Gold trigger consumed (deleted)")


    time.sleep(5)
