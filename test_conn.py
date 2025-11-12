import pyodbc
import os

conn_str = "Driver={ODBC Driver 18 for SQL Server};Server=tcp:cwk2-sql-71e8ef.database.windows.net,1433;Database=cwk2db;Uid=sqladmin;Pwd=GaryChen@0713;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"

try:
    conn = pyodbc.connect(conn_str)
    print("✅ Connection successful!")
    cursor = conn.cursor()
    cursor.execute("SELECT TOP 5 * FROM dbo.SensorReadings")
    for row in cursor.fetchall():
        print(row)
except Exception as e:
    print("❌ Connection failed:", e)
finally:
    conn.close()
