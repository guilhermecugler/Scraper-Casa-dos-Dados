import requests

def get_cities(estado: str) -> list:
    """
    Obtém lista de cidades de um estado

    Args:
        estado: Sigla do estado
    
    Returns:
        Lista de cidades do estado
    """
        
    response = requests.get(f"https://api.casadosdados.com.br/v4/public/cnpj/busca/municipio/{estado}")
    cities = []
    for city in response.json():
        cities.append(city["name"])

    return cities

if __name__ == "__main__":
    cities = get_cities("SP")
    print(cities)