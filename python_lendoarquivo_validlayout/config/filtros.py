import redis
import pyodbc

def duplicidade(linhas):
    result = list(set(linhas)) #Cria um conjunto e depois transforma em lista, conjunto não pode ter dados repetidos, logo o mesmo retira a duplicidade.

    return result



def filtros(linha):
    filtros = {} #Dicionario com os filtros para cada linha
    result  = [] #Resultado com a chave do filtro que retornou falso

    tratativas = {} #Dicionario com tratativa de linhas para importação
    registro = [] #Lista com os registros a serem inseridos na base

    insbd = []


    """Filtros que serão aplicados linhha a linha"""
    filtros.update({"NM000":lambda linha: len(linha[0:9].strip(" ")) == 9})
    filtros.update({"NAME":lambda linha: len(linha[13:20].strip(" ")) >= 1})

    """Tratativas das linhas para importação"""
    tratativas.update({"NM000":lambda linha: linha[0:9].strip(" ")})
    tratativas.update({"NAME":lambda linha: linha[13:26].strip(" ")})

    for retira in filtros.items():
        teste = filtros[retira[0]]
        if not teste(linha): #Caso o filtro não seja verdadeiro, layout invalido.
            result.append(retira[0])
        else:
            funcfiltro = tratativas[retira[0]]
            retstring = funcfiltro(linha)
            registro.append(retstring)

    if len(registro) == 2: #Somente inseri no banco se todos os filtros foram verdadeiros
        stringinsert = 'INSERT INTO ESTAGETABLE VALUES '+str(tuple(registro))
        insbd.append(stringinsert)

    return [result, insbd]