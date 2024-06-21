import requests


def get_cnaes() -> list:
    """
    Returns:
        Lista com código e descrição do cnae
    """
    response = requests.get(
        "https://api.casadosdados.com.br/v4/public/cnpj/busca/cnae")

    cnae_name = []
    cnae_code = []

    for cnae in response.json():
        cnae_name.append(cnae['name'])
        cnae_code.append(cnae['code'])

    return cnae_name, cnae_code


if __name__ == "__main__":
    cnaes = get_cnaes()
    print(cnaes)
