import asyncio
import aiohttp
import pandas as pd
from utils.excel_utils import save_excel
from utils.date_utils import format_date
import traceback


lista_dict_dados_cnpj = []

async def get_cnpj_data(session, semaphore, cnpj, cancel):
    """
    Obtém dados de um CNPJ

    Args:
        session: Instância da classe ClientSession
        semaphore: Semáforo para limitar requests assíncronos
        cnpj: Número do CNPJ
    
    Returns:
        Adiciona dicionário de dados do CNPJ na lista > lista_dict_dados_cnpj
    """
    if cancel.is_set():
        return
    try:
        url = f"https://api.casadosdados.com.br/v4/public/cnpj/{cnpj}"
    
        async with semaphore:
            async with session.get(url) as response:
                if response.status == 200:
                    json_response = await response.json()
                    # print(cnpj)
                    # Selecionando apenas as colunas desejadas
                    dados_selecionados = {
                        "cnpj": json_response["cnpj"],
                        "razao_social": json_response["razao_social"],
                        "nome_fantasia": json_response["nome_fantasia"],
                        "matriz_filial": json_response["matriz_filial"],
                        "codigo_natureza_juridica": json_response["codigo_natureza_juridica"],
                        "descricao_natureza_juridica": json_response["descricao_natureza_juridica"],
                        "situacao_atual": json_response["situacao_cadastral"]["situacao_atual"],
                        "cep": json_response["endereco"]["cep"],
                        "tipo_logradouro": json_response["endereco"]["tipo_logradouro"],
                        "logradouro": json_response["endereco"]["logradouro"],
                        "numero": json_response["endereco"]["numero"],
                        "complemento": json_response["endereco"]["complemento"],
                        "bairro": json_response["endereco"]["bairro"],
                        "uf": json_response["endereco"]["uf"],
                        "municipio": json_response["endereco"]["municipio"],
                        "data_abertura": format_date(json_response["data_abertura"]),  # Convertendo e formatando a data de abertura
                        "capital_social": json_response["capital_social"],
                        "atividade_principal": json_response["atividade_principal"]["descricao"],
                        # "atividade_secundaria": json_response["atividade_secundaria"],
                        "bloqueado": json_response["bloqueado"],
                        "mei_optante": json_response["mei"]["optante"],
                        "versao": json_response["versao"]
                    }

                    # Adicionando colunas para cada contato telefônico
                    if not json_response["contato_telefonico"]:
                        dados_selecionados[f"contato_telefonico_1"] = ""
                    elif len(json_response["contato_telefonico"]) == 1:
                        dados_selecionados[f"contato_telefonico_1"] = json_response["contato_telefonico"][0]["completo"]
                    else:
                        for i, contato in enumerate(json_response["contato_telefonico"], start=1):
                            dados_selecionados[f"contato_telefonico_{i}"] = contato["completo"]

                    # Adicionando colunas para cada e-mail
                    if not json_response["contato_email"]:
                        dados_selecionados[f"contato_email_1"] = ""
                    elif len(json_response["contato_email"]) == 1:
                        dados_selecionados[f"contato_email_1"] = json_response["contato_email"][0]["email"]
                    else:
                        for i, email in enumerate(json_response["contato_email"], start=1):
                            dados_selecionados[f"contato_email_{i}"] = email["email"]

                    lista_dict_dados_cnpj.append(dados_selecionados)

                else:
                    print(f"Falha ao buscar CNPJ {cnpj}: {response.status}")
    except Exception as e:
        print(f"Erro ao buscar CNPJ {cnpj}")
        traceback.print_exc()
        pass

async def get_cnpj_data_async(cnpjs, file_name, status_update, cancel):
    """
    Busca lista de CNPJs e salva dados em excel

    Args:
        cnpjs: Lista de CNPJs
    
    Returns:
        True se ocorrer sem erros ou False se ocorrerem erros
    """
    if cancel.is_set():
        return
    try:
        semaphore = asyncio.Semaphore(15)
        
        async with aiohttp.ClientSession() as session:
            tasks = [get_cnpj_data(session, semaphore, cnpj, cancel) for cnpj in cnpjs]
            await asyncio.gather(*tasks)
        
        
        # Transforma o dicionário em um DataFrame
        df = pd.DataFrame(lista_dict_dados_cnpj)

        # Salva o DataFrame em um arquivo Excel
        # file_name = "dados_empresas"
        save_excel(df, file_name)
        status_update(f"Finalizado... salvos {len(lista_dict_dados_cnpj)} CNPJ(s)")
        # print(f"Foram salvos {len(lista_dict_dados_cnpj)} CNPJs")
        return df
    except Exception as e:
       print(f"Erro ao buscar CNPJs {e}")
       traceback.print_exc()
       status_update(f"Erro ao buscar CNPJs {e}")
       pass

if __name__ == "__main__":
    
    cnpjs = ['46700647000127', '44100618000107', '97953114000198', '97919352000187', '97237692000128', '97692739000143', '97710800000138', '97122456000166', '97429348000130', '97300570000139', '97797058000140', '97104345000127', '97621893000124', '97748787000106', '43870476000196', '08802558000526', '08815533000133', '08822480000188', '08822483000111', '08809123000180']  # Add more CNPJs as needed

    asyncio.run(get_cnpj_data_async(cnpjs))
    print(f"Foram encontrados dados de {len(lista_dict_dados_cnpj)} CNPJs")