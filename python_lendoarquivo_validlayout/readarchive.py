import os
import time

# multiplas threads
from multiprocessing.pool import ThreadPool as Pool

import pyodbc
import redis
from config import filtros


def inicia(caminho, caminholog):

    # Contagem de linhas totais ja tratadas
    count = 0
    # Contagem de linhas em tratamento
    linhas = 0
    # Lista que contem as linhas atuais
    texto = []
    # Quantidade de linhas a serem processadas por vez
    quantidade = 100000

    r = redis.Redis(host='localhost', port='6379')
    inicio = time.time()
    print("Iniciando processamento de leitura....")
    print(f'Arquivo a ser lido: {caminho}')
    r.set('tempo_inicial', inicio)

    try:
        arq = open(caminho, 'r', encoding='UTF8')
        arqlog = open(caminholog, 'w', encoding='UTF8')
    except OSError:
        return "Caminho especificado esta invalido valide se possui \\ entre os diretorios"

    print("Arquivo lido com sucesso iniciando obtenção das primeiras linhas")
    with arq as f:

        iniciotm = time.time()
        for line in f:
            texto.append(line)
            linhas += 1
            if linhas > quantidade:
                count += linhas
                r.set('linha', count)

                print(f"Tempo total para obter as {linhas} linhas de um total de {count} "
                      f"linhas: {time.time() - iniciotm}  ")

                r.set('situacao', 'Iniciando retirada de duplicidade... Linhas atuais:{}'.format(str(count)))

                # Retira duplicidade do arquivo caso exista
                iniciotm = time.time()
                texto = filtros.duplicidade(texto)

                print(f"Tempo total para retirar duplicidades das {linhas} linhas de um total de {count} "
                      f"linhas: {time.time() - iniciotm}  ")
                r.set('situacao', 'Tratativa de duplicidade executada! Linhas atuais:{}'.format(str(count)))

                r.set('situacao', 'aplicando regras de layout Linhas atuais: {}'.format(str(count)))
                iniciotm = time.time()
                result = list(map(filtros.filtros, texto))

                print(f"Tempo total para aplicar regras de layout das {linhas} linhas de um total de {count} "
                      f"linhas: {time.time() - iniciotm} ")

                pool = Pool(processes=os.cpu_count())
                iniciotm = time.time()

                returnlog = pool.map(insertbd, result)

                print(f"Tempo total para inserir no SQL Server das {linhas} linhas de um total de {count} "
                      f"linhas: {time.time() - iniciotm} ")

                for linha in returnlog:
                    if len(linha) > 0:
                        arqlog.write(linha)
                texto = []
                linhas = 0

    fim = time.time()
    arq.close()
    arqlog.close()
    print(f"Tempo de processamento: {fim - inicio}")

    return "Processado com sucesso, tempo total: {}".format(str(fim - inicio))


# Função para tratamento de inserção dos dados no banco de dados
# E inserção no arquivo de log em caso de erro do insert ou layout

def insertbd(lista):
    code = lista[1]
    errolayout = lista[0][0] if len(lista[0]) > 0 else ""
    if len(errolayout) == 0:
        try:
            stringinsert = code[0] if len(code[0]) > 0 else ""
        except:
            stringinsert = " Erro trativa de if linha 71 sobre a lista: {}".format(str(code))
        retorno = ""

        conn_str = 'DRIVER={SQL Server};SERVER=DESKTOP-32JQ24J\\SQLEXPRESS;DATABASE=Estudo;Integrated Security=true'
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()

        if len(stringinsert) > 0:
            try:
                cursor.execute(stringinsert)

            except pyodbc.ProgrammingError:
                return "Erro de T-SQL na inserção, validar: {} \n".format(stringinsert)

        conn.commit()
    else:
        retorno = errolayout
    return retorno



