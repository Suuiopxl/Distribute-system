import azure.functions as func
import pyodbc
import os
import json
import logging

blueprint = func.Blueprint()

@blueprint.route(route="calculate_statistics", auth_level=func.AuthLevel.ANONYMOUS)
def calculate_statistics(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Calculating IoT sensor statistics from Azure SQL Database...")

    conn_str = os.environ["SqlConnectionString"]

    try:
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()

        cursor.execute("""
            SELECT
                sensor_id,
                MIN(temperature) AS min_temp,
                MAX(temperature) AS max_temp,
                AVG(temperature) AS avg_temp,
                MIN(wind_mph) AS min_wind,
                MAX(wind_mph) AS max_wind,
                AVG(wind_mph) AS avg_wind,
                MIN(rel_humidity) AS min_humidity,
                MAX(rel_humidity) AS max_humidity,
                AVG(rel_humidity) AS avg_humidity,
                MIN(co2_ppm) AS min_co2,
                MAX(co2_ppm) AS max_co2,
                AVG(co2_ppm) AS avg_co2
            FROM dbo.SensorReadings
            GROUP BY sensor_id
            ORDER BY sensor_id;
        """)

        results = []
        for row in cursor.fetchall():
            results.append({
                "sensor_id": row.sensor_id,
                "temperature": {
                    "min": round(row.min_temp, 2),
                    "max": round(row.max_temp, 2),
                    "avg": round(row.avg_temp, 2)
                },
                "wind_mph": {
                    "min": round(row.min_wind, 2),
                    "max": round(row.max_wind, 2),
                    "avg": round(row.avg_wind, 2)
                },
                "rel_humidity": {
                    "min": row.min_humidity,
                    "max": row.max_humidity,
                    "avg": round(row.avg_humidity, 2)
                },
                "co2_ppm": {
                    "min": row.min_co2,
                    "max": row.max_co2,
                    "avg": round(row.avg_co2, 2)
                }
            })

        return func.HttpResponse(
            json.dumps(results, indent=2),
            mimetype="application/json",
            status_code=200
        )

    except Exception as e:
        logging.error(f"Error while calculating statistics: {e}")
        return func.HttpResponse(f"Database query failed: {e}", status_code=500)

    finally:
        if 'conn' in locals():
            conn.close()
