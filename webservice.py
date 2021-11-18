# -*- coding: utf-8 -*-
"""
Created at 18/11/2021, 19:54
__author__ = 'danie'

__version__ = '2021.11.1'
"""
import json

import boto3
import botocore
from dynamodb_json import json_util as dynamodb_json
from boto3.dynamodb.conditions import Key
from flask import Flask
from werkzeug.exceptions import abort
from werkzeug.routing import BaseConverter

def conectar_ao_dynamo(config: dict,
                       configuracao: dict):
    dynamo_db = boto3.resource('dynamodb',
                               region_name = "sa-east-1",
                               aws_access_key_id = config['AccessKey'],
                               aws_secret_access_key = config['SecretAccessKey'])
    tabelas = dynamo_db.meta.client.list_tables()["TableNames"]
    try:
        if configuracao["TableName"] not in tabelas:
            tabela = dynamo_db.create_table(TableName = configuracao["TableName"],
                                            AttributeDefinitions = configuracao["AttributeDefinitions"],
                                            KeySchema = configuracao["KeySchema"],
                                            BillingMode = configuracao["BillingMode"],
                                            GlobalSecondaryIndexes = configuracao["GlobalSecondaryIndex"])
            waiter = dynamo_db.meta.client.get_waiter("table_exists")
            waiter.wait(TableName = configuracao["TableName"])
    except botocore.exceptions.ClientError as ce:
        print(ce.__dict__)
        return None
    else:
        tabela = dynamo_db.Table(configuracao["TableName"])
    return tabela

class RegexConverter(BaseConverter):
    def __int__(self, url_map, *items):
        super(RegexConverter, self).__init__(url_map)
        self.regex = items[0]

app = Flask(__name__)
app.url_map.converters['regex'] = RegexConverter
app.debug = True

with open("secrets.json", 'r') as fp:
    propriedades = json.load(fp)

with open("config.json", 'r') as fp:
    CONFIG = json.load(fp)


@app.route("/describe/<regex('[a-f\d]{40}'):arquivo>")


@app.route("/list")


@app.route("/get/chunk/<regex('[a-f\d]{40}'):arquivo>/<int:chunk>")
def get_distribuicao_chunks(arquivo, chunk):
    print(f"Usuario pediu o chunk {chunk} do arquivo {arquivo}")
    tbl_pedacos = conectar_ao_dynamo(propriedades, CONFIG["Documentos"]["distribuicao"])
    if tbl_pedacos is None:
        abort(500)

    documento = tbl_pedacos.query(IndexName = "chunk",
                                  Select = "ALL_PROJECTED_ATTRIBUTES",
                                  KeyConditionExpression = Key("hash_arquivo").eq(arquivo) &
                                  Key("numero_chunk").eq(chunk))

    if documento.get("Items") is None:
        abort(404)

    if documento.get("Count") != 1:
        abort(500)

    lista_de_pedacos = dynamodb_json.loads(documento.get("Items")[0])

    return app.response_class(response = json.dumps(lista_de_pedacos, indent = 2),
                              status = 200,
                              mimetype = "application/json")

@app.route("/")
def hello():
    return "Oi"

@app.errorhandler(500)
def erro(erro):
    return app.response_class(status = 500)

@app.errorhandler(404)
def erro404(erro):
    return app.response_class(status = 404)

if __name__ == "__main__":
    app.run(host = "0.0.0.0",
            port = 9876,
            use_reloader = True)
