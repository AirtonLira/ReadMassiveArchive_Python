from config import filtros
import time
import redis
import functools
import pyodbc
# multiplas threads
from multiprocessing.pool import ThreadPool as Pool
import os


def inicia(caminho, caminholog):
    r = redis.Redis(host='localhost', port='6379')
    inicio = time.time()
    print("Iniciando processamento de leitura....")
    print(f'Arquivo a ser lido: {caminho}')
    r.set('tempo_inicial', inicio)

    arq = None
    try:
        arq = open(caminho, 'r', encoding='UTF8')
        total = sum(1 for line in open(caminho, 'r', encoding='UTF8'))
    except OSError:
        return ("Caminho especificado esta invalido valide se possui \\ entre os diretorios")

    count = 0
    texto = []
    linhas = 0

    with arq as f:

        for line in f:
            r.set('linha', count)
            iniciotm = time.time()
            if linhas < 500000:
                texto.append(line)
                count += 1
                linhas += 1
            else:
                print(f"Tempo total para obter as {linhas} linhas de um total de {count} "
                      f"linhas: {time.time() - iniciotm}  ")

                r.set('situacao', 'Iniciando retirada de duplicidade... Linhas atuais:' + str(count))

                # Retira duplicidade do arquivo caso exista
                iniciotm = time.time()
                texto = filtros.duplicidade(texto)
                print(f"Tempo total para retirar duplicidades das {linhas} linhas de um total de {count} "
                      f"linhas: {time.time() - iniciotm}  ")
                r.set('situacao', 'Tratativa de duplicidade executada! Linhas atuais:' + str(count))

                r.set('situacao', 'aplicando regras de layout Linhas atuais:' + str(count))
                iniciotm = time.time()
                result = list(map(filtros.filtros, texto))
                print(f"Tempo total para aplicar regras de layout das {linhas} linhas de um total de {count} "
                      f"linhas: {time.time() - iniciotm} ")

                pool = Pool(processes=os.cpu_count())
                iniciotm = time.time()
                returnlog = pool.map(insertbd, result)
                print(f"Tempo total para inserir no SQL Server das {linhas} linhas de um total de {count} "
                      f"linhas: {time.time() - iniciotm} ")

                arqlog = open(caminholog, 'w', encoding='UTF8')

                for linha in returnlog:
                    if len(linha) > 0:
                        arqlog.write(linha)
                texto = []
                linhas = 0

        # Caso tenha lido todas as linhas mas ainda ficou pendente alguma linha

        if len(texto) > 0:
            r.set('situacao', 'Iniciando retirada de duplicidade... Linhas atuais:' + str(count))

            # Retira duplicidade do arquivo caso exista
            texto = filtros.duplicidade(texto)
            r.set('situacao', 'Tratativa de duplicidade executada! Linhas atuais:' + str(count))

            r.set('situacao', 'aplicando regras de layout Linhas atuais:' + str(count))
            result = list(map(filtros.filtros, texto))

            pool = Pool(processes=os.cpu_count())
            returnlog = pool.map(insertbd, result)

            arqlog = open(caminholog, 'w', encoding='UTF8')

            for linha in returnlog:
                if len(linha) > 0:
                    arqlog.write(linha)

        fim = time.time()
        arq.close()
        print(f"Tempo de processamento: {fim - inicio}")

    return "Processado com sucesso, tempo total: " + str(fim - inicio)



# Função para tratamento de inserção dos dados no banco de dados
# E inserção no arquivo de log em caso de erro do insert ou layout

def insertbd(lista):
    code = lista[1]
    try:
        stringinsert = code[0] if len(code[0]) > 0 else ""
    except:
        stringinsert = " Erro trativa de if linha 71 sobre a lista: " + str(code)
    retorno = ""

    conn_str = 'DRIVER={SQL Server};SERVER=DESKTOP-32JQ24J\SQLEXPRESS;DATABASE=Estudo;Integrated Security=true'
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()

    if len(stringinsert) > 0:
        try:
            cursor.execute(stringinsert)

        except pyodbc.ProgrammingError:
            retorno = "Erro de T-SQL na inserção, validar:  " + stringinsert + " \n"

    conn.commit()
    return retorno
