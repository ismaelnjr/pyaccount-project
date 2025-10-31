import os
import sys
import unittest

# Necessário para que o arquivo de testes encontre
test_root = os.path.dirname(os.path.abspath(__file__))
os.chdir(test_root)
sys.path.insert(0, os.path.dirname(test_root))
sys.path.insert(0, test_root)

from pyaccount.build_opening_balances import OpeningBalancesBuilder
from datetime import date

class TestBuildOpeningBalances(unittest.TestCase):

    def test_build_opening_balances(self):

        builder = OpeningBalancesBuilder(
            dsn="Local_17",
            user="consulta",
            password="consulta",
            empresa=437,
            ate=date(2024, 12, 31),
            saida="./out"
        )
        out_path = builder.execute()

if __name__ == '__main__':
    unittest.main()