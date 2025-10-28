import pyodbc

conn = pyodbc.connect(
    "UID=consulta;"
    "PWD=consulta;"
    "DSN=Local_17;"
)
cursor = conn.cursor()
cursor.execute("select * from bethadba.geempre")
for row in cursor.fetchall():
    print(row)
conn.close()