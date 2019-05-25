from flask import Flask, jsonify, request
import readarchive
import redis
import time
app = Flask(__name__)

r = redis.Redis(host='localhost', port='6379')


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

