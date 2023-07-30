import PySimpleGUI as sg
from functions import buscarEmpresas, adicionarPlanilha, enviarEmail
import os
from urllib.request import Request, urlopen, urlretrieve
from urllib.error import URLError, HTTPError
import pandas as pd
import json
import requests
from bs4 import BeautifulSoup
import sys

def Convert(lst):
    res_dct = {lst[i]: lst[i + 1] for i in range(0, len(lst), 2)}
    return res_dct

def buscarTodosCNPJ(json_data, quantidade_paginas):
    dados_json = {}
    pagina = []
    quantidadeCNPJ = []
    lista_cnpjs = []

    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/76.0.3809.100 Safari/537.36'}
    api_search = "https://api.casadosdados.com.br/v2/public/cnpj/search"

    for pagina in range(quantidade_paginas):

        try:

            json_data['page'] = pagina

            response = requests.post(api_search, headers=headers, json=json_data)

        except HTTPError as e:
            print(e.status, e.reason)

        except URLError as e:
            print(e.reason)
        
        dados_json = json.loads(response.content)

        if(dados_json['success'] == False):
            print("Erro ao pegar dados: \n"+dados_json['message'])
            raise Exception("Erro ao pegar dados")
        

        pagina = dados_json['page']['current']

        for i, d in enumerate(dados_json['data']['cnpj']):
            lista_cnpjs.append(dados_json['data']['cnpj'][i]['cnpj'])

        janela['-STAT-'].update(f"Buscando lista de CNPJ da Página {pagina+1}") 
        janela.Refresh()
        

    return lista_cnpjs

def buscarCNPJ(lista_cnpj):
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/76.0.3809.100 Safari/537.36'}
    
    dados_empresas = []

    for cnpj in lista_cnpj:
        try:
            response = requests.post(f'https://casadosdados.com.br/solucao/cnpj/{cnpj}', headers=headers)
        
        except HTTPError as e:
            print(e.status, e.reason)

        except URLError as e:
            print(e.reason)
        
        soup = BeautifulSoup(response.content, 'html.parser')
        dadosEmpresa = soup.find_all("div", class_="column is-narrow")
        dados = []
            
        for i, e in enumerate(dadosEmpresa):
            dadosEmpresa = soup.find_all("div", class_="column is-narrow")[i]
                
            for p in dadosEmpresa.find_all('p'):
                if(len(dadosEmpresa.find_all('p')) ==2):
                    dados.append(p.text)

            dictCNPJ = Convert(dados)


            municipio = dictCNPJ.get('Município')
            uf = dictCNPJ.get('UF')

            if(municipio != None):
                municipio_F = {"Município": municipio.strip()}
                dictCNPJ.update(municipio_F)
            if(uf != None):
                uf_F = {"UF": uf.strip()}
                dictCNPJ.update(uf_F)
        janela['-STAT-'].update(f"Carregando dados do CNPJ: {dictCNPJ['CNPJ']}") 
        janela.Refresh()
        df = pd.DataFrame(data=dictCNPJ, index=[1])

        dados_empresas.append(df)
            



    return dados_empresas

try:
    filtros = open('filtros.json')
    json_data = json.load(filtros)

    if json_data['query']['uf'] == "":
        json_data['query']['uf'] = "Todos"

    filtros.close()
except FileNotFoundError:
    json_data = {
    'query': {
        'termo': [],
        'atividade_principal': [],
        'natureza_juridica': [],
        'uf': ['Todos'],
        'municipio': [],
        'situacao_cadastral': 'ATIVA',
        'cep': [],
        'ddd': [],
    },
    'range_query': {
        'data_abertura': {
            'lte': '',
            'gte': '',
        },
        'capital_social': {
            'lte': None,
            'gte': None,
        },
    },
    'extras': {
        'somente_mei': False,
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
    with open("filtros.json", "w") as outfile:
        json.dump(json_data, outfile, indent=4)






extras = json_data['extras']
query = json_data['query']
range_query =json_data['range_query']

sg.theme('DarkBlue13')

siglasUF=['Todos','AC','AL','AP','AM','BA','CE','DF','ES','GO','MA','MS','MT','MG','PA','PB','PR','PE','PI','RJ','RN','RS','RO','RR','SC','SP','SE','TO']
quantidade_paginas = ""
dados_empresas = []
lista_cnpj = []
cnpj_empresas = []
dados_cnpjs = {}

layout = [
    [sg.Text("Pegar dados casadosdados.com.br"), sg.Push(), sg.Text("Planilha:"), sg.Input(default_text="Resultados.xlsx", key="nome_planilha", justification="r", size=(15, 1))],
    [sg.Text("Filtros:")],
    [sg.Checkbox('Somente MEI:', key="somente_mei", default=extras['somente_mei'], size=(13,1)), sg.Checkbox('Excluir MEI:', key="excluir_mei", default=extras['excluir_mei'], size=(13,1)), sg.Checkbox('Com contato telefone:', key="com_contato_telefonico", default=extras['com_contato_telefonico'])],
    [sg.Checkbox('Somente Fixo:', key="somente_fixo", default=extras['somente_fixo'], size=(13,1)), sg.Checkbox('Somente Matriz:', key="somente_matriz", default=extras['somente_matriz'], size=(13,1)), sg.Checkbox('Somente Filial:', key="somente_filial", default=extras['somente_filial'])],
    [sg.Checkbox('Somente Celular:', key="somente_celular", default=extras['somente_celular'], size=(13,1)), sg.Checkbox('Com E-mail:', key="com_email", default=extras['com_email'], size=(13,1)), sg.Checkbox("Atividade Secundária", key="incluir_atividade_secundaria", default=extras['incluir_atividade_secundaria'])],
    [sg.Text("Estado(s):"), sg.Combo(siglasUF, key="UF", default_value=query['uf'], size=(13,1)), sg.Text('CEP'), sg.InputText(key='cep', size=(15,1), default_text=query['cep']), sg.Text('DDD'), sg.InputText(key='ddd', size=(15,1), default_text=query['ddd'])],
    [sg.Text('Data de Abertura'), sg.InputText(key='DataAbertura', size=(13,1), default_text=range_query['data_abertura']['gte']),sg.CalendarButton("Selecionar",close_when_date_chosen=True, target="DataAbertura", format='%Y-%m-%d'), sg.Text('Até'), sg.InputText(key='DataAberturaFim', size=(12,1), default_text=range_query['data_abertura']['lte']),sg.CalendarButton("Selecionar",close_when_date_chosen=True, target="DataAberturaFim", format='%Y-%m-%d')],
    [sg.Text('Capital Social'), sg.InputText(key='CapitalSocial', size=(10,1), default_text=range_query['capital_social']['gte']), sg.Text('Até'), sg.InputText(key='CapitalSocialFim', size=(10,1), default_text=range_query['capital_social']['lte']), sg.Text('Situação Cadastral'), sg.Combo(['ATIVA', 'BAIXADA', 'INAPTA', 'SUSPENSA', 'NULA'], key="situacao_cadastral", default_value=query['situacao_cadastral'])],
    [sg.Text('Atividade Principal (Código CNAE)'), sg.InputText(key='atividade_principal', size=(10,1), default_text=query['atividade_principal']), sg.Text('Natureza Jurídica(Código)'), sg.InputText(key='natureza_juridica', size=(6,1), default_text=query['natureza_juridica'])],



    [sg.Button("Buscar filtros"), sg.Button("Carregar lista CNPJ"), sg.Button("Carregar dados das empresas"), sg.Button("Cancelar")],
    [sg.Text('Marque para "Enviar e-mail = Sim"'), sg.Checkbox('', key="EnviarEmail", default=False), sg.Text('Assinatura e-mail"'), sg.Checkbox('', key="EnviarAssinatura", default=False)],
    [sg.Button("Salvar na planilha"), sg.Button("Abrir planilha"), sg.Button("Enviar e-mail"), sg.Button("Salvar filtros")],
    [sg.StatusBar("",key="-STAT-", size=(20, 1), auto_size_text=True, justification="c")]

]

janela = sg.Window("BOT CADADOSDADOS.COM.BR", layout)

while True:
    evento, valores = janela.read()
    if evento == sg.WIN_CLOSED or evento == "Cancelar":
        break

    if evento == "Salvar filtros":
        extras['somente_mei'] = valores['somente_mei']
        extras['excluir_mei'] = valores['excluir_mei']
        extras['com_email'] = valores['com_email']
        extras['incluir_atividade_secundaria'] = valores['incluir_atividade_secundaria']
        extras['com_contato_telefonico'] = valores['com_contato_telefonico']
        extras['somente_celular'] = valores['somente_celular']
        extras['somente_fixo'] = valores['somente_fixo']
        extras['somente_matriz'] = valores['somente_matriz']
        extras['somente_filial'] = valores['somente_filial']
        
        a=[]
        
        if(valores['UF'] == ''):
            valores['UF'] = 'Todos'
        else:
            valores['UF'] = [valores['UF']]


        query['uf'] = valores['UF']
        query['atividade_principal'] = [valores['atividade_principal']]
        query['natureza_juridica'] = [valores['natureza_juridica']]
        #query['municipio'] = valores['municipio']
        query['situacao_cadastral'] = valores['situacao_cadastral']
        query['cep'] = [valores['cep']]
        query['ddd'] = [valores['ddd']]

        range_query['data_abertura']['gte'] = valores['DataAbertura']
        range_query['data_abertura']['lte'] = valores['DataAberturaFim']
        range_query['capital_social']['gte'] = valores['CapitalSocial']
        range_query['capital_social']['lte'] = valores['CapitalSocialFim']


        if(query['atividade_principal'] == ['']):
            query['atividade_principal'] = a

        if(query['cep'] == ['']):
            query['cep'] = a
        
        if(query['ddd'] == ['']):
            query['ddd'] = a
            
        if(query['natureza_juridica'] == ['']):
            query['natureza_juridica'] = a

        with open("filtros.json", "w") as outfile:
            json.dump(json_data, outfile, indent=4)

        janela['-STAT-'].update("Filtros salvos!")
        janela.Refresh()

    if evento == "Abrir planilha":
        os.system(f"start {valores['nome_planilha']}")

    if evento == "Buscar filtros":
        janela['-STAT-'].update("Buscando filtros, aguarde...")
        janela.Refresh()


        extras['somente_mei'] = valores['somente_mei']
        extras['excluir_mei'] = valores['excluir_mei']
        extras['com_email'] = valores['com_email']
        extras['incluir_atividade_secundaria'] = valores['incluir_atividade_secundaria']
        extras['com_contato_telefonico'] = valores['com_contato_telefonico']
        extras['somente_celular'] = valores['somente_celular']
        extras['somente_fixo'] = valores['somente_fixo']
        extras['somente_matriz'] = valores['somente_matriz']
        extras['somente_filial'] = valores['somente_filial']
        
        a=[]
        
        if(valores['UF'] == 'Todos'):
            valores['UF'] = a
        else:
            valores['UF'] = [valores['UF']]


        query['uf'] = valores['UF']
        query['atividade_principal'] = [valores['atividade_principal']]
        query['natureza_juridica'] = [valores['natureza_juridica']]
        #query['municipio'] = valores['municipio']
        query['situacao_cadastral'] = valores['situacao_cadastral']
        query['cep'] = [valores['cep']]
        query['ddd'] = [valores['ddd']]

        range_query['data_abertura']['gte'] = valores['DataAbertura']
        range_query['data_abertura']['lte'] = valores['DataAberturaFim']
        range_query['capital_social']['gte'] = valores['CapitalSocial']
        range_query['capital_social']['lte'] = valores['CapitalSocialFim']


        if(query['atividade_principal'] == ['']):
            query['atividade_principal'] = a

        if(query['cep'] == ['']):
            query['cep'] = a
        
        if(query['ddd'] == ['']):
            query['ddd'] = a
            
        if(query['natureza_juridica'] == ['']):
            query['natureza_juridica'] = a

        try:
            lista_cnpj = buscarEmpresas(json_data)
            janela['-STAT-'].update(f"{lista_cnpj[2]} empresas encontradas")
            quantidade_paginas = lista_cnpj[1]
            janela.Refresh()

        except Exception:
            janela['-STAT-'].update("Nenhum dado encontrado")
            janela.Refresh()


    if evento == "Carregar lista CNPJ":
        try:
            janela['-STAT-'].update("Carregando lista de CNPJ, aguarde...") 
            janela.Refresh()
            cnpj_empresas = buscarTodosCNPJ(json_data, quantidade_paginas)
            janela['-STAT-'].update(f"CNPJ de {len(cnpj_empresas)} empresas carregados") 
            janela.Refresh()
        except TypeError:
            sg.Popup("Busque os filtros primeiro!")
            janela['-STAT-'].update("Busca os filtros primeiro!")

    if evento == "Carregar dados das empresas":
        janela['-STAT-'].update("Carregando dados de cada empresa, aguarde...(pode levar um tempo)") 
        janela.Refresh()
        dados_cnpjs = buscarCNPJ(cnpj_empresas)
        janela['-STAT-'].update("Dados carregados com sucesso!") 
        janela.Refresh()

    if evento == "Enviar e-mail":
        janela['-STAT-'].update("Enviando e-mails, aguarde...") 
        janela.Refresh()
        enviarEmail(valores['EnviarAssinatura'])
        janela['-STAT-'].update("E-mail enviado!") 
        janela.Refresh()


    if evento == "Salvar na planilha":
        try:
            
            # print(dados_cnpjs)
            janela['-STAT-'].update("Enviando para planilha, aguarde...") 
    

            resposta = adicionarPlanilha(valores['nome_planilha'], dados_cnpjs, valores['EnviarEmail'])
            if resposta == 13:
                raise PermissionError

            janela['-STAT-'].update(f"Enviando CNPJ {resposta} para planilha, aguarde...") 
            janela.Refresh()




            janela['-STAT-'].update("Planilha salva!") 
            janela.Refresh()

        except NameError:
            sg.Popup("Faça a busca primeiro.")
            janela['-STAT-'].update("Faça a busca primeiro...")
        except IndexError:
            sg.Popup("Faça a busca primeiro")
            janela['-STAT-'].update("Faça a busca primeiro...")
        except PermissionError:
            sg.Popup("Feche a planilha")
            janela['-STAT-'].update("Feche a planilha...")
        except ValueError:
            sg.Popup("Faça a busca primeiro")
            janela['-STAT-'].update("Faça a busca primeiro...")




janela.close()


