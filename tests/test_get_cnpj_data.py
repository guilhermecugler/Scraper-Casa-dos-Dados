import unittest
import asyncio
from utils.get_cnpj_data import get_cnpj_data_async


class TesteCNPJData(unittest.IsolatedAsyncioTestCase):
    async def test_get_cnpj_data_async(self):
        cnpjs = ['46700647000127', '44100618000107']  # Lista de CNPJs de teste
        test = await get_cnpj_data_async(cnpjs)
        self.assertTrue(test)


if __name__ == '__main__':
    unittest.test_get_cnpj_data_async()
