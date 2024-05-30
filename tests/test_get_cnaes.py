import unittest
import asyncio
from utils.get_cnae import get_cnaes

class TestGetCnaes(unittest.IsolatedAsyncioTestCase):
    async def test_get_cnaes(self):
        test = get_cnaes()
        self.assertIsNotNone(test)

if __name__ == '__main__':
    unittest.main()
