import os
import sys
import pyodbc

# Necessário para que o arquivo de testes encontre
test_file_dir = os.path.dirname(os.path.abspath(__file__))
test_dir = os.path.dirname(test_file_dir)  # test/
project_root = os.path.dirname(test_dir)  # raiz do projeto
sys.path.insert(0, project_root)

from test.test_config import carregar_config_teste

# Carrega configurações do config.ini
config = carregar_config_teste()

conn = pyodbc.connect(
    f"UID={config['user']};"
    f"PWD={config['password']};"
    f"DSN={config['dsn']};"
)
cursor = conn.cursor()
cursor.execute("select * from bethadba.geempre")
for row in cursor.fetchall():
    print(row)
conn.close()