from flask import Flask, jsonify, request
import readarchive
import redis
import time
app = Flask(__name__)

r = redis.Redis(host='localhost', port='6379')
##
# COMENTARIOS ABAIXO SÃO APENAS PARA CONHECIMENTO
# devs = [
#     {
#         'name': 'Airton Lira',
#         'lang': 'python'
#     },
#     {
#         'name': 'John doe',
#         'lang': 'NodeJs'
#     }
# ]

# @app.route('/devs', methods=['GET'])
# def home():
#     return jsonify(devs), 200
#
#
# @app.route('/devs/<string:linguagem>', methods=['GET'])
# def devs_por_linguagem(linguagem):
#     devs_por_linguagem = [dev for dev in devs if dev['lang'] == linguagem]
#     return jsonify(devs_por_linguagem), 200
##


@app.route('/readarchive', methods=['POST'])
def save_dev():
    data = request.get_json()

    arquivo = data['arquivo']
    arquivo_log = data['arquivo_log']

    resultproc = readarchive.inicia(arquivo, arquivo_log)

    return jsonify(resultproc), 201


@app.route('/situation', methods=['GET'])
def obter_situa():
    situacao = str(r.get('situacao'))
    situacao += "\n tempo total: "+str((time.time() - float(r.get('tempo_inicial')))/60) + " minutos"
    return situacao


if __name__ == '__main__':
    app.run(debug=True)

