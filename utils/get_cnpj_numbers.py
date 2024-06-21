import math
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError
import asyncio
from utils.exceptions import NoneError, ApiError

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
        'cep': ["11760000"],
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


def create_evaluate_request_string(json):
    """
    Converte objeto json python em json javascript

    Args:
        json: Objeto json em python

    Returns:
        String json no formato de javascript
    """
    json_filters = str(json).replace("None", "null").replace(
        "False", "false").replace("True", "true")

    evaluate_request = """
    async () => {
        const response = await fetch('https://api.casadosdados.com.br/v2/public/cnpj/search', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify(json_filters)
        });
        const data = await response.json();
        return data;
    }
    """.replace("json_filters", json_filters)

    return evaluate_request


def calculate_page_count(cnpj_count):
    """
    Retorna a quantidade de páginas a partir do número de CNPJs

    Args:
        cnpj_count: Int com a quantidade de CNPJs

    Returns:
        Int com a quantidade de páginas
    """
    page_count = 1
    if cnpj_count < 1000 and cnpj_count > 20:
        page_count = math.ceil(cnpj_count / 20)
    elif cnpj_count > 1000:
        page_count = 50

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

    evaluate_request = create_evaluate_request_string(json_filters)
    attempts = 0
    max_attempts = 2
    lista_cnpjs = []

    while attempts < max_attempts:
        browser = await playwright.chromium.launch(headless=False, args=["--start-minimized"])
        try:
            context = await browser.new_context(
                # Reduz a resolução da tela
                viewport={"width": 100, "height": 100},
                permissions=['geolocation'],  # Define permissões específicas
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.0 Safari/537.36",
            )

            # Desativa as animações
            await context.add_init_script(evaluate_request)

            page = await context.new_page()

            await page.goto("https://casadosdados.com.br/solucao/cnpj/pesquisa-avancada/")
            response_cnpj_count = await page.evaluate(evaluate_request)
            # print(response_cnpj_count)

            if not response_cnpj_count['success']:
                raise ApiError(response_cnpj_count["message"])

            cnpj_count = response_cnpj_count['data']['count']

            if cnpj_count == 0:
                raise NoneError()

            page_count = calculate_page_count(cnpj_count)

            evaluate_request = evaluate_request.replace(
                "'page': 1", "'page': numero_pagina")
            for i in range(page_count):
                if cancel.is_set():
                    return
                try:
                    # Executar o JavaScript para fazer uma requisição para a API
                    response = await page.evaluate(evaluate_request.replace("numero_pagina", str(i + 1)))

                    lista_cnpjs_pagina = [empresa['cnpj']
                                          for empresa in response['data']['cnpj']]
                    lista_cnpjs.extend(lista_cnpjs_pagina)

                    step = i / page_count
                    status_update(
                        text=f"Calculando quantidade aproximada de CNPJs...({len(set(lista_cnpjs))})")
                    progressbar(step)
                except Exception as e:
                    print(e)
                    continue

            # Se chegamos aqui, a tentativa foi bem-sucedida, podemos sair do loop
            break
        except PlaywrightTimeoutError as e:
            attempts += 1
            status_update(text=f"Tempo excedido, tentativa {attempts} de {
                          max_attempts}. Tentando novamente...")
            print(f"Timeout occurred. Attempt {
                  attempts} of {max_attempts}. Retrying...")
            await browser.close()  # Fecha o browser antes de tentar novamente
        except Exception as e:
            status_update(text=f"Erro ao buscar número de CNPJs: {e}")
            print(f"Erro ao buscar número de CNPJs: {e}")
            await browser.close()  # Fecha o browser em caso de outras exceções
            break
        finally:
            await browser.close()  # Garante que o browser será fechado

    return lista_cnpjs


async def get_cnpj_numbers_async(json_data, progressbar, status_update, cancel):
    async with async_playwright() as playwright:
        lista_cnpjs = await get_cnpj_numbers(playwright, json_data, progressbar, status_update, cancel)

    cnpjs = list(set(lista_cnpjs))

    return cnpjs


if __name__ == "__main__":
    asyncio.run(get_cnpj_numbers_async(json_data))
    print(f"Quantidade de CNPJs {len(list(set(lista_cnpjs)))}")
