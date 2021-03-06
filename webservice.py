# -*- coding: utf-8 -*-
"""
Created at 18/11/2021, 19:54
__author__ = 'danie'

__version__ = '2021.11.1'
"""
import json
import random

import boto3
import botocore
from dynamodb_json import json_util as dynamodb_json
from boto3.dynamodb.conditions import Key
from flask import Flask
from flask import render_template
from werkzeug.exceptions import abort
from werkzeug.routing import BaseConverter


def put_item(tabela,
             documento: dict,
             condition_expression: str = None):
    try:
        registro = dynamodb_json.loads(documento)
        if condition_expression is None:
            tabela.put_item(Item = registro)
        else:
            tabela.put_item(Item = registro,
                            ConditionExpression = condition_expression)
    except botocore.exceptions.ClientError as pe:
        if pe.response.get("Error").get("Code") == "ConditionalCheckFailedException":
            # Registro duplicado
            return False
    else:
        return True


def gera_links(registro, resposta):
    numlinks = CONFIG["NumeroDeLinks"]
    timeout = CONFIG["TimeOut"]

    if len(registro["localizacacao_partes"]) == 0:
        resposta["status"] = 404
    elif len(registro["localizacacao_partes"]) < numlinks:
        numlinks = len(registro["localizacacao_partes"])
        resposta["status"] = 206
    else:
        resposta["status"] = 200

    resposta["numlinks"] = numlinks
    resposta["hash_arquivo"] = registro["hash_arquivo"]
    resposta["hash_chunk"] = registro["hash_chunk"]
    buckets = random.sample(registro["localizacacao_partes"], numlinks)
    resposta["urls"] = list()

    s3 = boto3.client('s3',
                  aws_access_key_id = propriedades['AccessKey'],
                  aws_secret_access_key = propriedades['SecretAccessKey'])
    for opcao in buckets:
        url = s3.generate_presigned_url("get_object",
                                        Params = {"Bucket": opcao["nome"],
                                                  "Key": registro["hash_arquivo"]+"."+registro["hash_chunk"]+".part"},
                                        ExpiresIn = timeout,
                                        HttpMethod = "GET")
        partes = url.split("/")
        url = "http://" + partes[3] + "." + partes[2] + "/" + "/".join(partes[4:])
        resposta["urls"].append(url)


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


@app.route("/")
@app.route("/list")
def filelist():
    tbl_metadados = conectar_ao_dynamo(propriedades, CONFIG["Documentos"]["metadados"])
    if tbl_metadados is None:
        abort(500)

    arquivos = tbl_metadados.scan()
    if arquivos["Count"] == 0:
        abort(404)

    dados = arquivos["Items"]
    while "LastEvaluatedKey" in arquivos:
        arquivos = tbl_metadados.scan(ExclusiveStartKey = arquivos["LastEvaluatedKey"])
        dados.extend(arquivos["Items"])

    dados = dynamodb_json.loads(dados)
    resposta = dict()
    lista_de_arquivos = list()
    for arquivo in dados:
        entrada = dict()
        entrada["nome_arquivo"] = arquivo["nome_arquivo"]
        entrada["tamanho"] = arquivo["tamanho"]
        entrada["hash_arquivo"] = arquivo["hash_arquivo"]
        lista_de_arquivos.append(entrada)

    return render_template("lista_de_arquivos.html", rows = lista_de_arquivos, num_arquivos = len(dados))


@app.route("/get/<regex('[a-f\d]{40}'):arquivo>")
def get_metados(arquivo):
    tbl_metadados = conectar_ao_dynamo(propriedades, CONFIG["Documentos"]["metadados"])
    if tbl_metadados is None:
        abort(500)

    key = dict()
    key["hash_arquivo"] = arquivo
    item = tbl_metadados.get_item(Key = key)

    if item is None:
        abort(404)

    metadados = dynamodb_json.loads(item.get("Item"))
    header = dict()
    header["Content-Disposition"] = f"attachment;filename={metadados['nome_arquivo']}.meutorrent"

    return app.response_class(response = json.dumps(metadados),
                              mimetype = "application/json",
                              headers = header)


@app.route("/health/<regex('[a-f\d]{40}'):arquivo>")
def mostra_saude_arquivos(arquivo):
    print(f"Vamos verificar a saude do arquivo {arquivo}")

    tbl_pedacos = conectar_ao_dynamo(propriedades, CONFIG["Documentos"]["distribuicao"])
    if tbl_pedacos is None:
        abort(500)

    documento = tbl_pedacos.query(IndexName = "todososchunks",
                                  Select = "ALL_PROJECTED_ATTRIBUTES",
                                  KeyConditionExpression = Key("hash_arquivo").eq(arquivo))

    partes = documento["Items"]
    while "LastEvaluatedKey" in documento:
        documento = tbl_pedacos.query(IndexName = "todososchunks",
                                      Select = "ALL_PROJECTED_ATTRIBUTES",
                                      KeyConditionExpression = Key("hash_arquivo").eq(arquivo),
                                      ExclusiveStartKey = documento["LastEvaluatedKey"])
        partes.extend(documento["Items"])
    partes = sorted(dynamodb_json.loads(partes), key=lambda d: d["numero_chunk"])

    lista_buckets = CONFIG["Buckets"]

    matriz_de_situacao = list()
    s3client = boto3.client('s3',
                  aws_access_key_id = propriedades['AccessKey'],
                  aws_secret_access_key = propriedades['SecretAccessKey'])
    for bucket in lista_buckets:
        linha = dict()
        linha["bucket"] = bucket["friendly_name"]
        linha["partes"] = list()
        for chunk in partes:
            if len(list(filter(lambda d: d["nome"] == bucket["nome"], chunk["localizacacao_partes"]))) == 0: # nao esta
                linha["partes"].append(1)
            else:
                remote_key = arquivo + "." + chunk["hash_chunk"] + ".part"
                try:
                    resposta = s3client.head_object(Bucket = bucket["nome"], Key = remote_key)
                except botocore.exceptions.ClientError as e:
                    if e.response.get("Error").get("Code") == "404":  # Eh isso que eu quero!!!
                        linha["partes"].append(2)
                else:
                    if resposta.get("ResponseMetadata").get("HTTPStatusCode") == 200:
                        linha["partes"].append(3)
                    else:
                        linha["partes"].append(2)

        matriz_de_situacao.append(linha)

    return render_template("saude_do_arquivo.html",
                           rows = matriz_de_situacao,
                           num_partes = len(partes),
                           arquivo = arquivo)


@app.route("/repair/<regex('[a-f\d]{40}'):arquivo>")
def repara_arquivo(arquivo):
    print(f"Vamos verificar a saude do arquivo {arquivo}")

    tbl_pedacos = conectar_ao_dynamo(propriedades, CONFIG["Documentos"]["distribuicao"])
    if tbl_pedacos is None:
        abort(500)

    documento = tbl_pedacos.query(IndexName = "todososchunks",
                                  Select = "ALL_PROJECTED_ATTRIBUTES",
                                  KeyConditionExpression = Key("hash_arquivo").eq(arquivo))

    partes = documento["Items"]
    while "LastEvaluatedKey" in documento:
        documento = tbl_pedacos.query(IndexName = "todososchunks",
                                      Select = "ALL_PROJECTED_ATTRIBUTES",
                                      KeyConditionExpression = Key("hash_arquivo").eq(arquivo),
                                      ExclusiveStartKey = documento["LastEvaluatedKey"])
        partes.extend(documento["Items"])
    partes = sorted(dynamodb_json.loads(partes), key=lambda d: d["numero_chunk"])

    lista_buckets = CONFIG["Buckets"]

    s3client = boto3.client('s3',
                  aws_access_key_id = propriedades['AccessKey'],
                  aws_secret_access_key = propriedades['SecretAccessKey'])

    for chunk in partes:
        remote_key = arquivo + "." + chunk["hash_chunk"] + ".part"
        lista_dos_locais_que_tem_o_chunk = list()
        for local in chunk["localizacacao_partes"]:
            # Verificando se a parte esta no local
            erro = False
            try:
                resposta = s3client.head_object(Bucket = local["nome"], Key = remote_key)
            except botocore.exceptions.ClientError as e:
                if e.response.get("Error").get("Code") == "404":
                    erro = True
                    pass
            else:
                if resposta.get("ResponseMetadata").get("HTTPStatusCode") == 200:
                    lista_dos_locais_que_tem_o_chunk.append(local)
                else:
                    continue
            if erro:
                continue
        if len(lista_dos_locais_que_tem_o_chunk) == CONFIG["NumeroDeReplicas"]:
            print(f"A parte {chunk['numero_chunk']} esta ok...")
            continue

        print(f"A parte {chunk['numero_chunk']} precisa ser reparada")
        if len(lista_dos_locais_que_tem_o_chunk) == 0:
            print(f"A parte {chunk['numero_chunk']} nao pode ser recuperada pq nao existe copia dela em lugar algum")
            continue

        fonte = random.choice(lista_dos_locais_que_tem_o_chunk)
        copias_a_fazer = CONFIG['NumeroDeReplicas'] - len(lista_dos_locais_que_tem_o_chunk)
        source = {
            "Bucket": fonte["nome"],
            "Key": remote_key
        }
        print(f"Precisamos gerar mais {copias_a_fazer} replicas")

        lista_de_destinos = list()
        for copia in range(copias_a_fazer):
            while True:
                destino = random.choice(CONFIG["Buckets"])
                if fonte == destino:
                    continue
                if destino in lista_dos_locais_que_tem_o_chunk:
                    continue
                erro = False
                print(f"Fazendo a copia {copia + 1} para {destino['friendly_name']}")
                try:
                    resposta = s3client.copy(source, destino["nome"], remote_key)
                except botocore.exceptions.ClientError as e2:
                    print(f"Erro na copia para {destino['friendly_name']}. Tentando outro...")
                    erro = True
                    pass
                else:
                    lista_dos_locais_que_tem_o_chunk.append(destino)
                    break
                if erro:
                    continue
        print(f"Parte {chunk['numero_chunk']} reparada!")
        chunk["localizacacao_partes"] = lista_dos_locais_que_tem_o_chunk
        chave = {"uniqueKey": arquivo+chunk["hash_chunk"]}
        tbl_pedacos.delete_item(Key = dynamodb_json.loads(chave))
        if put_item(tbl_pedacos, chunk, condition_expression = "attribute_not_exists(uniqueKey)"):
            print(f"Tabela de pedacos atualizada para o chunk {chunk['numero_chunk']}")
        else:
            print(f"Problemas para registrar a distribuicao do chunk {chunk['numero_chunk']}")


    return filelist()


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
    resposta = dict()

    gera_links(lista_de_pedacos, resposta)

    return app.response_class(response = json.dumps(resposta, indent = 2),
                              status = resposta["status"],
                              mimetype = "application/json")


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

