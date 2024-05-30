import asyncio
import customtkinter as ctk
import os 
from utils import get_cnpj_numbers_async, get_cnpj_data_async, get_cnaes, get_cities, page_count, save_excel, lista_cnpjs, exceptions
from PIL import Image
from datetime import datetime
import time
from tkinter import filedialog as fd
from threading import Thread, enumerate as enumt, Event
import pandas as pd

global cancel
cancel = Event()


global list_df_all_cnpj_details
list_df_all_cnpj_details = []

global progress_step
progress_step = 0
global iter_step


def start_thread(function_name):
    print(f'Thread {function_name} started')

    global stop
    stop = 0

    t = Thread(target=function_name)
    t.daemon = True
    t.start()

class FiltersFrame(ctk.CTkFrame):
    def __init__(self, master, title):
        super().__init__(master)
        self.grid_columnconfigure(2, weight=1)
        self.title = title
        self.checkboxes = []


        self.title = ctk.CTkLabel(self, text=self.title, corner_radius=6)
        self.title.grid(row=0, column=0, padx=10, pady=(10, 0), sticky="ew", columnspan=3)

        try:
             cnaes = get_cnaes()
        except:
             cnaes = []

        self.check_somente_mei_var = ctk.BooleanVar(value=False)
        self.check_excluir_mei_var = ctk.BooleanVar(value=False)
        self.check_com_telefone_var = ctk.BooleanVar(value=False)
        self.check_somente_fixo_var = ctk.BooleanVar(value=False)
        self.check_somente_matriz_var = ctk.BooleanVar(value=False)
        self.check_somente_filial_var = ctk.BooleanVar(value=False)
        self.check_somente_celular_var = ctk.BooleanVar(value=False)
        self.check_com_email_var = ctk.BooleanVar(value=False)
        self.check_atividade_secundaria_var = ctk.BooleanVar(value=False)
        self.combobox_estados_var = ctk.StringVar(value='Todos Estados')
        self.combobox_municipios_var = ctk.StringVar(value='Todos Municipios')
        self.combobox_cnae_var = ctk.StringVar(value='Todas Atividades')
        self.cnae_code_var = ctk.StringVar(value='')

        
        def combobox_estados_callback(choice):
                    self.combobox_estados_var.set(choice)
                    self.combobox_municipios_var.set('Todos Municipios')
                    self.combobox_municipios.configure(values=get_cities(self.combobox_estados_var.get()))

        def combobox_municipios_callback(choice):
            self.combobox_municipios_var.set(choice)    

        def combobox_cnae_callback(choice):
            for i, cnae in enumerate(cnaes[0]):
                if cnae == choice:
                    self.cnae_code_var.set(cnaes[1][i])
                    print(f"cnae selected code: {self.cnae_code_var.get()}")

        def format_date_inicial(event):
            
            text = self.entry_data_inicial.get().replace("/", "")[:8]
            new_text = ""

            if event.keysym.lower() == "backspace": return
            
            for index in range(len(text)):
                if not text[index] in "0123456789": continue
                if index in [1, 3]: new_text += text[index] + "/"
                else: new_text += text[index]

            self.entry_data_inicial.delete(0, "end")
            self.entry_data_inicial.insert(0, new_text)

        def format_date_final(event):
            
            text = self.entry_data_final.get().replace("/", "")[:8]
            new_text = ""

            if event.keysym.lower() == "backspace": return
            
            for index in range(len(text)):
                if not text[index] in "0123456789": continue
                if index in [1, 3]: new_text += text[index] + "/"
                else: new_text += text[index]

            self.entry_data_final.delete(0, "end")
            self.entry_data_final.insert(0, new_text)

        self.entry_termo = ctk.CTkEntry(
            self,
            placeholder_text='Razão Social ou Termo - Ex: Celular'
            )
        self.entry_termo.grid(row=1, columnspan=3, column=0, padx=10, pady=10, sticky='ew')

        self.check_somente_mei = ctk.CTkCheckBox(
            self,
            text='Somente MEI',
            variable=self.check_somente_mei_var,
            onvalue=True,
            offvalue=False,
        )
        self.check_somente_mei.grid(row=2, column=0, padx=10, pady=10, sticky='ew')

        self.check_excluir_mei = ctk.CTkCheckBox(
            self,
            text='Excluir MEI',
            variable=self.check_excluir_mei_var,
            onvalue=True,
            offvalue=False,
        )
        self.check_excluir_mei.grid(row=2, column=1, padx=10, pady=10, sticky='ew')

        self.check_com_telefone = ctk.CTkCheckBox(
            self,
            text='Com Telefone',
            variable=self.check_com_telefone_var,
            onvalue=True,
            offvalue=False,
        )
        self.check_com_telefone.grid(row=2, column=2, padx=10, pady=10, sticky='ew')

        self.check_somente_fixo = ctk.CTkCheckBox(
            self,
            text='Somente Fixo',
            variable=self.check_somente_fixo_var,
            onvalue=True,
            offvalue=False,
        )
        self.check_somente_fixo.grid(row=3, column=0, padx=10, pady=10, sticky='ew')

        self.check_somente_matriz = ctk.CTkCheckBox(
            self,
            text='Somente Matriz',
            variable=self.check_somente_matriz_var,
            onvalue=True,
            offvalue=False,
        )
        self.check_somente_matriz.grid(row=3, column=1, padx=10, pady=10, sticky='ew')

        self.check_somente_filial = ctk.CTkCheckBox(
            self,
            text='Somente Filial',
            variable=self.check_somente_filial_var,
            onvalue=True,
            offvalue=False,
        )
        self.check_somente_filial.grid(row=3, column=2, padx=10, pady=10, sticky='ew')

        self.check_somente_celular = ctk.CTkCheckBox(
            self,
            text='Somente Celular',
            variable=self.check_somente_celular_var,
            onvalue=True,
            offvalue=False,
        )
        self.check_somente_celular.grid(row=4, column=0, padx=10, pady=10, sticky='ew')

        self.check_com_email = ctk.CTkCheckBox(
            self,
            text='Com E-mail',
            variable=self.check_com_email_var,
            onvalue=True,
            offvalue=False,
        )
        self.check_com_email.grid(row=4, column=1, padx=10, pady=10, sticky='ew')

        self.check_atividade_secundaria = ctk.CTkCheckBox(
            self,
            text='Atividade Secundária',
            variable=self.check_atividade_secundaria_var,
            onvalue=True,
            offvalue=False,
        )
        self.check_atividade_secundaria.grid(
            row=4, column=2, padx=10, pady=10, sticky='ew'
        )

        self.combobox_estados = ctk.CTkComboBox(
            self, values = ['Todos Estados','AC','AL','AP','AM','BA','CE','DF','ES','GO','MA','MS','MT','MG','PA','PB','PR','PE','PI','RJ','RN','RS','RO','RR','SC','SP','SE','TO',],
            command=combobox_estados_callback,
            variable=self.combobox_estados_var,
        )
        self.combobox_estados.grid(row=5, column=0, padx=10, pady=10, sticky='ew')

        # self.combobox_municipios = CTkScrollableDropdown(
        #     self,
        #     values=[''],
        #     command=combobox_municipios_callback,
        #     variable=self.combobox_municipios_var,
        # )
        # self.combobox_municipios.grid(row=5, column=1, padx=10, pady=10, sticky='ew')
        self.combobox_municipios = ctk.CTkComboBox(
            self,
            values=[''],
            command=combobox_municipios_callback,
            variable=self.combobox_municipios_var,
        )
        self.combobox_municipios.grid(row=5, column=1, padx=10, pady=10, sticky='ew')

        self.combobox_cnaes = ctk.CTkComboBox(
            self,
            values=[''],
            command=combobox_cnae_callback,
            variable=self.combobox_cnae_var)
      
        self.entry_bairro = ctk.CTkEntry(self, placeholder_text='Bairro')
        self.entry_bairro.grid(row=5, column=2, padx=10, pady=10, sticky='ew')

        self.entry_CEP = ctk.CTkEntry(self, placeholder_text='CEP')
        self.entry_CEP.grid(row=7, column=1, padx=10, pady=10, sticky='ew')

        self.entry_DDD = ctk.CTkEntry(self, placeholder_text='DDD')
        self.entry_DDD.grid(row=7, column=2, padx=10, pady=10, sticky='ew')

        self.combobox_cnaes.grid(row=7, column=0, padx=10, pady=10, sticky='ew')
        self.combobox_cnaes.configure(values=cnaes[0])

        self.label_data = ctk.CTkLabel(self, text="Período de abertura")
        self.label_data.grid(row=8, column=0, padx=10, pady=10, sticky='ew')


        self.entry_data_inicial = ctk.CTkEntry(self, placeholder_text='Inicio - 01/01/2023')
        self.entry_data_inicial.bind("<KeyRelease>", command=format_date_inicial)
        self.entry_data_inicial.grid(row=8, column=1, padx=10, pady=10, sticky='ew')
        

        self.entry_data_final = ctk.CTkEntry(self, placeholder_text='Fim - 01/12/2023')
        self.entry_data_final.bind("<KeyRelease>", command=format_date_final)
        self.entry_data_final.grid(row=8, column=2, padx=10, pady=10, sticky='ew')


        

    def button_buscar_callback(self):
        cnpjs = ["55102193000183", "55015814000191", "55076151000115", "55037548000106", "55083185000137", "55078462000113", "55070390000168", "55069481000183", "55126786000180", "55052180000147", "55124811000196", "55070518000193"]
        asyncio.run(get_cnpj_data_async(cnpjs))


class App(ctk.CTk):
    def __init__(self):
        super().__init__()
        if cancel.is_set():
            App.status_update(self, text="Cancelado com sucesso!")
            return

        self.width = 550   # Width
        self.height = 650   # Height

        screen_width = self.winfo_screenwidth()
        screen_height = self.winfo_screenheight() 

        x = (screen_width / 2) - (self.width / 2)
        y = (screen_height / 2) - (self.height / 2)

        self.title("Casa dos Dados")
        self.geometry('%dx%d+%d+%d' % (self.width, self.height, x, y-30))
        self.grid_columnconfigure((0, 1), weight=1)
        self.grid_rowconfigure(2, weight=1)
        self.resizable(width=False, height=False)

        assets = os.path.join(os.path.dirname(__file__), "..", "assets")

        self.iconbitmap(os.path.join(assets, "icon.ico"))
        
        self.radio_var = ctk.IntVar(value=0)

        self.home_image = ctk.CTkImage(light_image=Image.open(os.path.join(assets, "logo_casa_dos_dados_light.png")),
                                                 dark_image=Image.open(os.path.join(assets, "logo_casa_dos_dados_dark.png")), size=(500, 56))
        self.home_image_label = ctk.CTkLabel(self, text="", image=self.home_image)
        self.home_image_label.grid(row=0, column=0, padx=10, pady=(30, 10), sticky="ew", columnspan=4)

        self.filters_frame = FiltersFrame(self, "Filtros")
        self.filters_frame.grid(row=1, column=0, padx=10, pady=(10, 0), sticky="nsew", columnspan=4)


        self.label_numero_buscas = ctk.CTkLabel(self, text="Repetir")
        self.label_numero_buscas.grid(row=5, column=0, padx=(20, 0), pady=10, sticky="w")

        self.button_diminuir_buscas = ctk.CTkButton(self, text="-", width=20, command=self.button_diminuir_buscas_callback)
        self.button_diminuir_buscas.grid(row=5, column=0, padx=(70, 0), pady=10, sticky="w")

        self.entry_quantidade_buscas_var = ctk.IntVar()
        self.entry_quantidade_buscas_var.set(1)
        self.entry_quantidade_buscas = ctk.CTkEntry(self, placeholder_text="1", width=30, textvariable=self.entry_quantidade_buscas_var)
        self.entry_quantidade_buscas.grid(row=5, column=0, padx=(0, 23), pady=10, sticky="e")

        self.button_aumentar_buscas = ctk.CTkButton(self, text="+", width=20, command=self.button_aumentar_buscas_callback)
        self.button_aumentar_buscas.grid(row=5, column=0, padx=0, pady=10, sticky="e")

        self.button_buscar_empresas = ctk.CTkButton(self, text="Buscar Empresas", command=self.button_buscar_empresas_callback)
        self.button_buscar_empresas.grid(row=5, column=1, padx=10, pady=10, sticky="ew", columnspan=2)

        self.button_cancelar = ctk.CTkButton(self, text="Cancelar", command=self.button_cancelar_callback, state="disabled")
        self.button_cancelar.grid(row=5, column=3, padx=10, pady=10, sticky="ew", columnspan=1)

        self.status = ctk.CTkLabel(self, text="Faça uma busca!")
        self.status.grid(row=6, column=0, padx=0, pady=0, sticky="ew", columnspan=4)

        self.progress_bar = ctk.CTkProgressBar(self, orientation='horizontal')
        self.progress_bar.grid(row=7, column=0, padx=10, pady=(5, 5), sticky="ew", columnspan=4)
        self.progress_bar.set(0)

        self.label_file_type = ctk.CTkLabel(self, text="Tipo de arquivo:")
        self.label_file_type.grid(row=8, column=1)

        self.file_type_var = ctk.StringVar(value='xlsx')

        self.radio_xlsx = ctk.CTkRadioButton(self, text='Planilha', value='xlsx', variable=self.file_type_var, command=self.radiobutton_event, radiobutton_width=13, radiobutton_height=13)
        self.radio_xlsx.grid(row=8, column=2, padx=0, pady=0, sticky="w")

        self.radio_csv = ctk.CTkRadioButton(self, text='CSV', value='csv', variable=self.file_type_var, command=self.radiobutton_event, radiobutton_width=13, radiobutton_height=13)
        self.radio_csv.grid(row=8, column=3, padx=0, pady=0, sticky="e")

        self.file_entry_var = ctk.Variable(value=f"{datetime.strftime(datetime.now(), '%d-%m-%Y %H-%M')}.{self.file_type_var.get()}")
        self.file_entry = ctk.CTkEntry(self, textvariable=self.file_entry_var)
        self.file_entry.grid(row=9, column=1, padx=10, pady=20, sticky="ew")

        self.button_select_folder = ctk.CTkButton(self, text="Selecionar Pasta", command=self.button_select_folder_callback)
        self.button_select_folder.grid(row=9, column=2, padx=10, pady=10, sticky="ew", columnspan=2)

        
        self.appearance_mode_menu = ctk.CTkOptionMenu(self, values=["Sistema", "Escuro", "Claro"],
                                                                command=self.change_appearance_mode_event)
        self.appearance_mode_menu.grid(row=9, column=0, padx=10, pady=20, sticky="ws")

    

    def get_save_folder(self):

        directory = fd.askdirectory(
            title='Selecionar pasta'
        )
        return directory
    
    def button_aumentar_buscas_callback(self):
        novo_valor = self.entry_quantidade_buscas_var.get()+1
        self.entry_quantidade_buscas_var.set(novo_valor)

    def button_diminuir_buscas_callback(self):
        novo_valor = self.entry_quantidade_buscas_var.get()-1
        if novo_valor < 1:
            self.entry_quantidade_buscas_var.set(1)
        else:
            self.entry_quantidade_buscas_var.set(novo_valor)

    def button_select_folder_callback(self):
        directory = self.get_save_folder()
        if directory == '':
            return
        file_location = f"{directory}/{datetime.strftime(datetime.now(), '%d-%m-%Y %H-%M')}.{self.file_type_var.get()}"
        self.file_entry_var.set(file_location.replace('//', '/'))

    def radiobutton_event(self):
        self.file_entry_var.set(f"{datetime.strftime(datetime.now(), '%d-%m-%Y %H-%M')}.{self.file_type_var.get()}")

    def progress_bar_update(self, step):
        self.progress_bar.set(step)
        self.update_idletasks()

    def status_update(self, text):
        self.status.configure(text=text)
        self.update_idletasks()

    def button_cancelar_callback(self):
        # self.button_cancelar.configure(state='normal')

        cancel.set()
        # self.button_buscar_empresas.configure(state='normal')
        self.button_cancelar.configure(state='disabled')

        App.status_update(self, "Cancelando, aguarde...")
        if cancel.is_set():
            self.button_buscar_empresas.configure(state='normal')
            return
        # report all tasks
        # global stop_flag
        # stop_flag = True

        # tasks = asyncio.all_tasks()
        # print("CLICOU CANCELAR")
        # for task in tasks:
            # print(task.get_name)

    def button_aumentar_buscas_callback(self):
        novo_valor = self.entry_quantidade_buscas_var.get()+1
        self.entry_quantidade_buscas_var.set(novo_valor)

    def button_diminuir_buscas_callback(self):
        novo_valor = self.entry_quantidade_buscas_var.get()-1
        if novo_valor < 1:
            self.entry_quantidade_buscas_var.set(1)
        else:
            self.entry_quantidade_buscas_var.set(novo_valor)

    def button_buscar_empresas_callback(self):
        self.button_buscar_empresas.configure(state='disabled')
        self.button_cancelar.configure(state='normal')
        
        cancel.clear()
        json_filters = {}
        try:
            json_filters.update(
                {
            'query': {
                'termo': [] if self.filters_frame.entry_termo.get() == '' else [self.filters_frame.entry_termo.get()],
                'atividade_principal': [] if self.filters_frame.cnae_code_var.get() == '' else [self.filters_frame.cnae_code_var.get()],
                'natureza_juridica': [],
                'uf': [] if self.filters_frame.combobox_estados_var.get() == 'Todos Estados' else [self.filters_frame.combobox_estados_var.get()],
                'municipio': [] if self.filters_frame.combobox_municipios_var.get() == 'Todos Municipios' else [self.filters_frame.combobox_municipios_var.get()],
                'cep': [] if self.filters_frame.entry_CEP.get() == '' else [self.filters_frame.entry_CEP.get()],
                'ddd': [] if self.filters_frame.entry_DDD.get() == '' else [self.filters_frame.entry_DDD.get()],
                }, 
            'range_query':{
                'data_abertura':{
                    'lte': None if self.filters_frame.entry_data_final.get() == '' else datetime.strftime(datetime.strptime(self.filters_frame.entry_data_final.get(), '%d/%m/%Y'), '%Y-%m-%d'),
                    'gte': None if self.filters_frame.entry_data_inicial.get() == '' else datetime.strftime(datetime.strptime(self.filters_frame.entry_data_inicial.get(), '%d/%m/%Y'), '%Y-%m-%d'),
                }
            },
            'extras':{
            'somente_mei':self.filters_frame.check_somente_mei_var.get(),
            'excluir_mei':self.filters_frame.check_excluir_mei_var.get(),
            'com_email':self.filters_frame.check_com_email_var.get(),
            'incluir_atividade_secundaria':self.filters_frame.check_atividade_secundaria_var.get(),
            'com_contato_telefonico':self.filters_frame.check_somente_fixo_var.get(),
            'somente_fixo':self.filters_frame.check_somente_fixo_var.get(),
            'somente_celular':self.filters_frame.check_somente_celular_var.get(),
            'somente_matriz':self.filters_frame.check_somente_matriz_var.get(),
            'somente_filial':self.filters_frame.check_somente_filial_var.get()
        },
        'page': 1
        })
            
        except ValueError as e:
            print(f'Error: {e}')
            self.button_buscar_empresas.configure(state='normal')
            self.button_cancelar.configure(state='disabled')

            return

        self.progress_bar.set(0)
        # print(functions.json_filters)
        
        def buscar():
            start_time = time.time()

            list_df_all_cnpj_details.clear()

            
            App.status_update(self, text=f"Iniciando módulo de busca... aguarde!")

            repetir = self.entry_quantidade_buscas_var.get()

            try:
                self.progress_bar.configure(mode="determinate")

                for i in (range(repetir)):
                    if cancel.is_set():
                        return
                    cnpjs = asyncio.run(get_cnpj_numbers_async(json_filters, self.progress_bar_update, self.status_update, cancel))

            except exceptions.NoneError as e:
                App.status_update(self, text=e.message)
                self.button_buscar_empresas.configure(state='normal')
                self.button_cancelar.configure(state='disabled')
                return

            self.progress_bar.stop()
            self.progress_bar.configure(mode="determinate")
            
            cnpjs = list(set(cnpjs)) #Remove duplicados
            if cancel.is_set():
                return
            App.status_update(self, text=f"Encontrados {len(cnpjs)} CNPJ(s), iniciando extração...")
            self.progress_bar.configure(mode="indeterminate")
            self.progress_bar.start()

            file_name = self.file_entry_var.get()
            if cancel.is_set():
                return

            dados_cnpjs = asyncio.run(get_cnpj_data_async(cnpjs, file_name, self.status_update, cancel))

            # if self.file_type_var.get() == 'xlsx':
            #     save_excel(dados_cnpjs, file_name)
            # else:
            #     pd.concat(dados_cnpjs).to_csv(file_name, index=False)

            # App.status_update(self, f"Finalizado... salvos {len(list_df_all_cnpj_details)} CNPJ(s)")
            self.progress_bar.stop()
            self.progress_bar.configure(mode="determinate")

            list_df_all_cnpj_details.clear()
            
            self.button_buscar_empresas.configure(state='normal')
            self.button_cancelar.configure(state='disabled')
            
            cnpjs.clear()
            print("--- %s seconds ---" % (time.time() - start_time))
            self.progress_bar.set(1)


            teste = enumt()
            # print(teste)
        

        start_thread(buscar)
        if cancel.is_set():
            App.status_update(self, text="Cancelado com sucesso!")
            return

        teste = enumt()
        # print(teste)
   
    def change_appearance_mode_event(self, new_appearance_mode):
        if new_appearance_mode == "Escuro": new_appearance_mode = "Dark"
        if new_appearance_mode == "Claro": new_appearance_mode = "Light"
        if new_appearance_mode == "Sistema": new_appearance_mode = "System"

        ctk.set_appearance_mode(new_appearance_mode)

if __name__ == "__main__":
    app = App()
    app.mainloop()
