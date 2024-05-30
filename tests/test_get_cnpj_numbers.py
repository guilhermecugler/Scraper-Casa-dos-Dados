import unittest
import asyncio
from utils.get_cnpj_numbers import get_cnpj_numbers_async

class TesteCNPJData(unittest.IsolatedAsyncioTestCase):
        
    async def test_get_cnpj_numbers_async(self):
        
        json_data = {
            'query': {
                'termo': [],
                'atividade_principal': [],
                'natureza_juridica': [],
                'uf': [],
                'municipio': [],
                'bairro': [],
                'situacao_cadastral': 'ATIVA',
                'cep': [],
                'ddd': [],
            },
            'range_query': {
                'data_abertura': {
                    'lte': None,
                    'gte': None,
                },
                'capital_social': {
                    'lte': None,
                    'gte': None,
                },
            },
            'extras': {
                'somente_mei': True,
                'excluir_mei': False,
                'com_email': False,
                'incluir_atividade_secundaria': False,
                'com_contato_telefonico': False,
                'somente_fixo': False,
                'somente_celular': False,
                'somente_matriz': False,
                'somente_filial': False,
            },
            'page': 1,
        }
        test = await get_cnpj_numbers_async(json_data)
        self.assertTrue(test > 0)

if __name__ == '__main__':
    unittest.get_cnpj_numbers_async()
