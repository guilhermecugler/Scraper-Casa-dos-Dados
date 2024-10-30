# Autor: Guilherme Cugler https://github.com/guilhermecugler
# Data: 2024-29-10
# Descrição: Scraper do site casa dos dados
import math
import time
from playwright.async_api import async_playwright
import asyncio
from utils.exceptions import NoneError, ApiError
import json

lista_cnpjs = []
page_count = 0
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


def calculate_page_count(cnpj_count):
    """
    Retorna a quantidade de páginas a partir do número de CNPJs

    Args:
        cnpj_count: Int com a quantidade de CNPJs

    Returns:
        Int com a quantidade de páginas
    """
    page_count = 1
    if cnpj_count < 200 and cnpj_count > 20 : page_count = math.ceil(cnpj_count / 20)
    elif cnpj_count > 200 : page_count = 10

    return page_count

async def get_cnpj_numbers(playwright, json_filters, progressbar, status_update, cancel):
    """
    Obtém uma lista dos CNPJS de acordo com o filtro.
    
    Args:
        playwright: Classe Página do Playwright.
        json_filters: Json com os filtros a serem buscados.

    Returns:
       Lista com o números dos CNPJs de acordo com o filtro da busca.
    """

    browser = await playwright.chromium.launch(headless=False, args=[
        '--window-position=800,800',  # Abre a janela num canto da tela
        '--window-size=400,300',      # Tamanho pequeno para a janela
        '--start-minimized'           # Para navegadores que suportam essa opção
    ])
   
    try:
        context = await browser.new_context()
        page = await context.new_page()
        await page.set_viewport_size({"width": 400, "height": 300})

        await page.goto("https://casadosdados.com.br/solucao/cnpj/pesquisa-avancada", timeout=5000, wait_until='domcontentloaded')

        response_handle = await page.evaluate_handle(
            """async (jsonData) => {
                try {
                    const response = await fetch('https://api.casadosdados.com.br/v2/public/cnpj/search', {
                        method: 'POST',
                        headers: {
                            'accept': 'application/json',
                            'accept-language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7,ja;q=0.6',
                            'content-type': 'application/json',
                            'sec-fetch-site': 'same-site',
                            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36'
                        },
                        body: JSON.stringify(jsonData)
                    });
                    return await response.json();
                } catch (error) {
                    return {error: error.message};
                }
            }""",
            json_filters
        )

        response_cnpj_count = await response_handle.json_value()
        if 'error' in response_cnpj_count:
            print(response_cnpj_count)
            return

        if not response_cnpj_count['success']:
            raise ApiError(response_cnpj_count["message"])
        
        cnpj_count = response_cnpj_count['data']['count']

        if cnpj_count == 0:
            raise NoneError()
        
        page_count = calculate_page_count(cnpj_count)

        for i in range(page_count):
            json_filters['page'] = i+1
            if cancel.is_set():
                return
            try:
                response_handle = await page.evaluate_handle(
                    """async (jsonData) => {
                        try {
                            const response = await fetch('https://api.casadosdados.com.br/v2/public/cnpj/search', {
                                method: 'POST',
                                headers: {
                                    'accept': 'application/json',
                                    'accept-language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7,ja;q=0.6',
                                    'content-type': 'application/json',
                                    'sec-fetch-site': 'same-site',
                                    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36'
                                },
                                body: JSON.stringify(jsonData)
                            });
                            return await response.json();
                        } catch (error) {
                            return {error: error.message};
                        }
                    }""",
                    json_filters
                )

                response = await response_handle.json_value()

                lista_cnpjs_pagina = [empresa['cnpj'] for empresa in response['data']['cnpj']]
                lista_cnpjs.extend(lista_cnpjs_pagina)
                print(f"Lista de CNPJs (página {i+1}):", lista_cnpjs_pagina)
                # print(f"Página atual {response['page']['current']}")
                step = i / page_count
                status_update(text=f"Calculando quantidade aproximada de CNPJs...({len(set(lista_cnpjs))})")
                time.sleep(1)
                progressbar(step)
            except Exception as e:
                print(e)
                continue
    except Exception as e:
        status_update(text=f"Erro {e}")
        print(e)
        await browser.close()
    finally:
        await browser.close()
    
    return lista_cnpjs

async def get_cnpj_numbers_async(json_data, progressbar, status_update, cancel):
    async with async_playwright() as playwright:
        await get_cnpj_numbers(playwright, json_data, progressbar, status_update, cancel)

    cnpjs = list(set(lista_cnpjs))
    return cnpjs


if __name__ == "__main__":
    asyncio.run(get_cnpj_numbers_async(json_data))
    print(f"Quantidade de CNPJs {len(list(set(lista_cnpjs)))}")
