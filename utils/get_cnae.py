import requests

def get_cnaes() -> list:
    """
    Returns:
        Lista com código e descrição do cnae
    """
    cnaes = requests.get("https://api.casadosdados.com.br/v4/public/cnpj/busca/cnae")
    
    return cnaes.json()

if __name__ == "__main__":
    cnaes = get_cnaes()
    print(cnaes)