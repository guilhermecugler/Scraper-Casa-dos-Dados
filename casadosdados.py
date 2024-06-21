import sys
import os
import customtkinter as ctk
from screens.main import App

# Adiciona o diretório principal ao sys.path para permitir importações
# sys.path.append(os.path.dirname(os.path.abspath(__file__)))


def main():
    app = App()
    app.mainloop()


if __name__ == "__main__":
    main()
