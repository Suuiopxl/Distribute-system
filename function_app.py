import os
import random
import datetime
import pyodbc
import azure.functions as func
import logging
import time

app = func.FunctionApp()

def get_conn():
    conn_str = os.environ["SqlConnectionString"]
    return pyodbc.connect(conn_str)

def generate_snapshot(num_sensors=20, batches=1):
    rows = []
    for _ in range(batches):
        for sensor_id in range(1, num_sensors + 1):
            rows.append((
                sensor_id,
                round(random.uniform(5.0, 30.0), 2),     # temperature
                round(random.uniform(0.0, 40.0), 2),     # wind mph
                random.randint(20, 90),                 # rel humidity
                random.randint(350, 2000),              # co2 ppm
                datetime.datetime.utcnow()              # timestamp
            ))
    return rows

@app.timer_trigger(schedule="*/10 * * * * *", arg_name="myTimer")
def store_data(myTimer: func.TimerRequest):
    num_sensors = 20 
    batches = 50   

    logging.info(f"[RUN] Generating data: sensors={num_sensors}, batches={batches}")
    start = time.time()

    data = generate_snapshot(num_sensors, batches)

    conn = get_conn()
    cursor = conn.cursor()
    cursor.fast_executemany = True
    cursor.executemany("""
        INSERT INTO dbo.SensorReadings
            (sensor_id, temperature, wind_mph, rel_humidity, co2_ppm, ts_utc)
        VALUES (?, ?, ?, ?, ?, ?)
    """, data)
    conn.commit()
    conn.close()

    elapsed = time.time() - start
    logging.info(f"[PERF] sensors={num_sensors}, batches={batches}, rows={len(data)}, time={elapsed:.3f}s")