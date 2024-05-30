class CustomError(Exception):
    """Base class for other exceptions"""
    pass

class NoneError(CustomError):
    """Exceção para nenhum CNPJ encontrado na pesquisa"""
    def __init__(self, message="Nenhum resultado para sua pesquisa"):
        self.message = message
        super().__init__(self.message)

class ApiError(CustomError):
    """Exceção para nenhum CNPJ encontrado na pesquisa"""
    def __init__(self, message="Erro na resposta da API"):
        self.message = message
        super().__init__(self.message)

class ValidationError(CustomError):
    """Raised when there is a validation error"""
    def __init__(self, message="There was a validation error"):
        self.message = message
        super().__init__(self.message)

class DatabaseError(CustomError):
    """Raised when a database error occurs"""
    def __init__(self, message="There was a database error"):
        self.message = message
        super().__init__(self.message)

class NetworkError(CustomError):
    """Raised when a network error occurs"""
    def __init__(self, message="There was a network error"):
        self.message = message
        super().__init__(self.message)

if __name__ == "__main__":
    print("Testes")