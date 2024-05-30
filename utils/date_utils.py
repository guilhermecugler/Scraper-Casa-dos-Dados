from datetime import datetime

def format_date(date):
    """
    Função para converter e formatar a data

    Args:
        date (str): Data no formato "%Y-%m-%dT%H:%M:%SZ"

    Returns:
        str: Data formatada no formato "dd/mm/AAAA"
    """
    data_obj = datetime.strptime(date, "%Y-%m-%dT%H:%M:%SZ")
    return data_obj.strftime("%d/%m/%Y")