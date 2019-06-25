import redis
import pyodbc


def duplicidade(linhas):
    # Cria um conjunto e depois transforma em lista, conjunto não pode ter dados repetidos,
    # logo o mesmo retira a duplicidade.
    result = list(set(linhas))

    return result


def filtros(linha):
    # Dicionario com os filtros para cada linha
    regras = {}
    # Resultado com a chave do filtro que retornou falso
    result = []

    # Dicionario com tratativa de linhas para importação
    tratativas = {}
    # Lista com os registros a serem inseridos na base
    registro = []

    insbd = []

    """Filtros que serão aplicados linha a linha"""
    regras.update({"NM000": lambda: len(linha[0:9].strip(" ")) == 9})
    regras.update({"NAME": lambda: len(linha[13:20].strip(" ")) >= 1})

    """Tratativas das linhas para importação"""
    tratativas.update({"NM000": lambda: linha[0:9].strip(" ")})
    tratativas.update({"NAME": lambda: linha[13:26].strip(" ")})

    for retira in regras.items():
        teste = regras[retira[0]]
        if not teste(linha):
            result.append("Erro de layout {} para o conteudo:{}".format(retira[0], linha))
        else:
            funcfiltro = tratativas[retira[0]]
            retstring = funcfiltro(linha)
            registro.append(retstring)

    # Somente inseri no banco se todos os filtros foram verdadeiros
    if len(registro) == 2:
        stringinsert = 'INSERT INTO ESTAGETABLE VALUES '+str(tuple(registro))
        insbd.append(stringinsert)

    return [result, insbd]
