# -*- coding: utf-8 -*-
"""
Created at 05/11/2021, 19:09
__author__ = 'danie'

__version__ = '2021.11.1'
"""
# Fatiamento do arquivo original e geracao dos metadados
import hashlib
import json
import math
import os.path

import boto3

metadados = dict()
"""
Nome do arquivo - Ok
Tamanho do arquivo - Ok
SHA1 do arquivo
Tamanho do chunk - Ok
# de chunks - Ok
SHA1 do chunk1
SHA1 do chunk2
:
SHA1 do chunkn
"""

with open("secrets.json", 'r') as fp:
    propriedades = json.load(fp)

arquivo_raw = input("Qual eh o arquivo que vamos fatiar? ")
metadados['chunksize'] = int(input("Qual o tamanho do chunk? "))

(drive, caminho) = os.path.splitdrive(arquivo_raw)
(caminho, arquivo) = os.path.split(caminho)

metadados['nome_arquivo'] = arquivo

try:
    metadados['tamanho'] = os.path.getsize(arquivo_raw)
except:
    print("Problemas para acesso ao arquivo")
    exit(2)

metadados['total_chunks'] = int(math.ceil(metadados['tamanho'] / float(metadados['chunksize'])))

with open(arquivo_raw, 'rb') as fd_input:
    hash_geral = hashlib.sha1()
    metadados['hash_chunks'] = list()
    for chunk in range(metadados['total_chunks']):
        hash_chunk = hashlib.sha1()
        buf = fd_input.read(metadados['chunksize'])
        hash_geral.update(buf)
        hash_chunk.update(buf)
        metadados['hash_chunks'].append(hash_chunk.hexdigest())
    metadados['hash_arquivo'] = hash_geral.hexdigest()

arquivo_saida = os.path.join(drive, caminho, arquivo + ".meutorrent")
with open(arquivo_saida, 'w') as fd_output:
    json.dump(metadados, fd_output, indent = 2)


########
# Enviar o arquivo para um repositorio remoto
s3 = boto3.client('s3',
                  aws_access_key_id = propriedades['AccessKey'],
                  aws_secret_access_key = propriedades['SecretAccessKey'])

with open(arquivo_raw, 'rb') as fp:
    s3.upload_fileobj(fp, 'br.edu.ifsp.ctd.dclobato.entrada', arquivo)
with open(arquivo_saida, 'rb') as fp:
    s3.upload_fileobj(fp, 'br.edu.ifsp.ctd.dclobato.entrada', arquivo+'.meutorrent')

# Notificar o distribuidor da existencia de um arquivo novo
sqs = boto3.client('sqs',
                   aws_access_key_id = propriedades['AccessKey'],
                   aws_secret_access_key = propriedades['SecretAccessKey'],
                   region_name = 'sa-east-1')
fila = sqs.create_queue(QueueName = 'br_edu_ifsp_ctd_dclobato_entrada',
                        Attributes = {'VisibilityTimeout': '120'})

resposta = sqs.send_message(QueueUrl = fila['QueueUrl'],
                            MessageBody = arquivo)

# Finalizar
