import azure.functions as func
import json
import random
import logging
from blueprint import blueprint


app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)
app.register_functions(blueprint)

@app.function_name(name="generate_sensor_data")
@app.route(route="generate_sensor_data")
def generate_sensor_data(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Generating simulated IoT sensor data...")

    sensors = []
    for sensor_id in range(1, 21):
        data = {
            "sensor_id": sensor_id,
            "temperature": round(random.uniform(5, 18), 1),
            "wind_speed": round(random.uniform(12, 24), 1),
            "humidity": round(random.uniform(30, 60), 1),
            "co2": random.randint(400, 1600)
        }
        sensors.append(data)

    result_json = json.dumps(sensors, indent=2)

    return func.HttpResponse(
        result_json,
        status_code=200,
        mimetype="application/json"
    )
