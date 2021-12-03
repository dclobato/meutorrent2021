# -*- coding: utf-8 -*-
"""
Created at 11/11/2021, 19:07
__author__ = 'danie'

__version__ = '2021.11.1'
"""
# Conectar no SQS ok
# Para cada mensagem que houver na fila: ok
#  Baixar o arquivo de metadados ok
#  Baixar o arquivo original ok
#  Validar os metadados ok
#  Para cada um dos chunks do arquivo:
#     Escolher um conjunto de n locais
#     Armazenar o chunk nesses locais
#     Atualizar a tabela com a distribuicao
import hashlib
import json
import random
from pathlib import Path

import boto3
import botocore.exceptions
import dynamodb_json.json_util
from filechunkio import FileChunkIO
from boto3.s3.transfer import TransferConfig

def s3_send_file(s3client,
                 bucket_name: str,
                 input_file: Path,
                 offset: int,
                 bytes_to_send: int,
                 remote_key: str):
    if bytes_to_send == 0 and offset == 0:
        bytes_to_send = input_file.stat().st_size

    if bytes_to_send == 0 and offset != 0:
        bytes_to_send = input_file.stat().st_size - offset

    if bytes_to_send > input_file.stat().st_size - offset:
        bytes_to_send = input_file.stat().st_size - offset

    config = TransferConfig(multipart_threshold = 5 * 1024 * 1024, use_threads = True)

    try:
        resposta = s3client.head_object(Bucket = bucket_name,
                                        Key = remote_key)
    except botocore.exceptions.ClientError as e:
        if e.response.get("Error").get("Code") == "403":
            # Parte existe, e nao tenho permissao para ler
            return False
        if e.response.get("Error").get("Code") == "404": # Eh isso que eu quero!!!
            try:
                with FileChunkIO(input_file, mode = "rb", offset = offset, bytes = bytes_to_send) as fp:
                    s3client.upload_fileobj(fp, bucket_name, remote_key)
            except botocore.exceptions.ClientError as client_error:
                print(client_error.__dict__)
                return False
            else:
                return True
    else:
        if resposta.get("ResponseMetadata").get("HTTPStatusCode") == 200:
            # Ja existe o chunk la... Problema
            return False
        else:
            print(resposta.get("ResponseMetadata"))
            raise OSError
    return True

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

def put_item(tabela,
             documento: dict,
             condition_expression: str = None):
    try:
        registro = dynamodb_json.json_util.loads(documento)
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

with open("secrets.json", 'r') as fp:
    propriedades = json.load(fp)

with open("config.json", 'r') as fp:
    CONFIG = json.load(fp)

sqs = boto3.client('sqs',
                   aws_access_key_id = propriedades['AccessKey'],
                   aws_secret_access_key = propriedades['SecretAccessKey'],
                   region_name = 'sa-east-1')

fila = sqs.create_queue(QueueName = CONFIG["FilaDeEntrada"],
                        Attributes = {'VisibilityTimeout': '120'})

s3 = boto3.client('s3',
                  aws_access_key_id = propriedades['AccessKey'],
                  aws_secret_access_key = propriedades['SecretAccessKey'])

## Criar os buckets para distribuicao
for bucket in CONFIG["Buckets"]:
    try:
        resposta = s3.create_bucket(Bucket = bucket["nome"],
                                    CreateBucketConfiguration = {
                                        "LocationConstraint": bucket["regiao"]
                                    })
    except botocore.exceptions.ClientError as e:
        if e.response.get("Error").get("Code") == "BucketAlreadyOwnedByYou": #Na realidade, eh sucesso
            print(f"Bucket {bucket['friendly_name']} ja existia")
            continue
        if e.response.get("Error").get("Code") == "BucketAlreadyExists": #Aqui eh ruim mesmo...
            print(f"Bucket {bucket['friendly_name']} foi criado por outro, e nao temos acesso")
            exit(1)
    else:
        print(f"Bucket {bucket['friendly_name']} criado")

mensagens = sqs.receive_message(QueueUrl = fila['QueueUrl'],
                                MaxNumberOfMessages = 1,
                                AttributeNames = ["All"],
                                MessageAttributeNames = ["All"])

while "Messages" in mensagens:
    mensagem = mensagens.get("Messages")[0]

    nome_do_arquivo = mensagem["Body"]
    nome_do_arquivo_metadados = nome_do_arquivo + ".meutorrent"
    handler = mensagem["ReceiptHandle"]
    print(f"Processando o arquivo {nome_do_arquivo}...")


    arquivo_metadados = Path(nome_do_arquivo_metadados)
    with open(arquivo_metadados, "wb") as fp:
        s3.download_fileobj(CONFIG["BucketEntrada"], nome_do_arquivo_metadados, fp)

    arquivo = Path(nome_do_arquivo)
    with open(nome_do_arquivo, "wb") as fp:
        s3.download_fileobj(CONFIG["BucketEntrada"], nome_do_arquivo, fp)

    with open(nome_do_arquivo_metadados, "r") as fp:
        metadados = json.load(fp)

    with open(nome_do_arquivo, "rb") as fd_input:
        hash_geral = hashlib.sha1()
        for chunk in range(metadados['total_chunks']):
            hash_chunk = hashlib.sha1()
            buf = fd_input.read(metadados['chunksize'])
            hash_geral.update(buf)
            hash_chunk.update(buf)
            hash_calculado = hash_chunk.hexdigest()
            if hash_calculado != metadados['hash_chunks'][chunk]:
                print(f"O hash do chunk {chunk} esta incorreto...")
                exit(1)
            else:
                print(f"O hash do chunk {chunk} esta correto")
        hash_calculado = hash_geral.hexdigest()
        if hash_calculado != metadados['hash_arquivo']:
            print("O hash global do arquivo esta incorreto")
        else:
            print("O hash global do arquivo esta correto")

    tbl_metadados = conectar_ao_dynamo(propriedades, CONFIG["Documentos"]["metadados"])
    tbl_pedacos = conectar_ao_dynamo(propriedades, CONFIG["Documentos"]["distribuicao"])
    if not put_item(tbl_metadados, metadados, condition_expression = "attribute_not_exists(hash_arquivo)"):
        print(f"O arquivo {metadados['nome_arquivo']} ja esta no repositorio")
        continue

    for chunk in range(metadados['total_chunks']):
        lista_de_destinos = random.sample(CONFIG["Buckets"], CONFIG["NumeroDeReplicas"])
        documento = dict()
        documento["uniqueKey"] = metadados["hash_arquivo"]+metadados["hash_chunks"][chunk]
        documento["hash_arquivo"] = metadados["hash_arquivo"]
        documento["hash_chunk"] = metadados["hash_chunks"][chunk]
        documento["numero_chunk"] = chunk
        documento["offset"] = chunk * metadados["chunksize"]
        if chunk != metadados["total_chunks"] - 1:
            documento["tamanho"] = metadados["chunksize"]
        else:
            documento["tamanho"] = metadados["tamanho"] % metadados["chunksize"]
        documento["localizacacao_partes"] = list()
        for local in lista_de_destinos:
            print(f"Guardando o chunk {chunk} em {local['friendly_name']}")
            nome_remoto = metadados["hash_arquivo"]+"."+metadados["hash_chunks"][chunk]+".part"
            if not s3_send_file(s3,
                            local["nome"],
                            Path(nome_do_arquivo),
                            metadados["chunksize"] * chunk,
                            metadados["chunksize"],
                            nome_remoto):
                print(f"Problemas para enviar o chunk {chunk} para {local['friendly_name']}")
            documento["localizacacao_partes"].append(local)
        # GUARDAR NO DYNAMO PARA ONDE FOI ESTE PEDACO
        # Colocar esse documento na tabela de distribuicao
        #print(json.dumps(documento, indent = 2))
        if not put_item(tbl_pedacos, documento, condition_expression = "attribute_not_exists(uniqueKey)"):
            print(f"Problemas para registrar a distribuicao do chunk {chunk}")
            continue

    print(f"Distribuicao do arquivo {nome_do_arquivo} concluida")

    sqs.delete_message(QueueUrl = fila['QueueUrl'],
                       ReceiptHandle = handler)
    s3.delete_object(Bucket = CONFIG["BucketEntrada"],
                     Key = nome_do_arquivo_metadados)
    s3.delete_object(Bucket = CONFIG["BucketEntrada"],
                     Key = nome_do_arquivo)
    arquivo.unlink()
    arquivo_metadados.unlink()

    mensagens = sqs.receive_message(QueueUrl = fila['QueueUrl'],
                                    MaxNumberOfMessages = 1,
                                    AttributeNames = ["All"],
                                    MessageAttributeNames = ["All"])

