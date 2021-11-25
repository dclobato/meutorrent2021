# -*- coding: utf-8 -*-
"""
Created at 25/11/2021, 19:05
__author__ = 'danie'

__version__ = '2021.11.1'
"""
import hashlib
import json
import os
import random
from pathlib import Path
import bitstring
import requests as requests


def obter_nome_arquivo():
    while True:
        nome = input("Qual o nome do arquivo? ")
        arquivo = Path(nome).resolve()
        if arquivo.is_file():
            break
    return arquivo


def mostra_estado_arquivo (mapa: bitstring):
    print("Situacao atual do arquivo")
    print(f"   Partes recuperadas... {mapa.count(1)}")
    print(f"   Partes faltantes..... {mapa.count(0)}")
    print(f"   Mapa das partes...... ", end = "")
    for pedaco in range(len(mapa)):
        if mapa[pedaco]:
            print("██", end = "")
        else:
            print("░░", end = "")
    print()


if __name__ == "__main__":
    nome_do_arquivo_metadados = obter_nome_arquivo()
    with nome_do_arquivo_metadados.open(mode = "r") as fd:
        metadados = json.load(fd)

    mapa_de_bits = bitstring.BitArray(metadados["total_chunks"])

    nome_do_arquivo = Path(metadados["nome_arquivo"]).resolve()
    try:
        arquivo = open(nome_do_arquivo, "r+b")
    except IOError:
        arquivo = open(nome_do_arquivo, "w")
        arquivo.close()
        arquivo = open(nome_do_arquivo, "r+b")

    arquivo.truncate(metadados["tamanho"])

    print("Verificando a situacao do arquivo...")
    print()
    print("Legenda:")
    print("    ██ Presente")
    print("    ░░ Ausente")
    for chunk in range(metadados["total_chunks"]):
        arquivo.seek(chunk * metadados["chunksize"], os.SEEK_SET)
        hash_chunk = hashlib.sha1()
        buf = arquivo.read(metadados['chunksize'])
        hash_chunk.update(buf)
        if hash_chunk.hexdigest() == metadados["hash_chunks"][chunk]:
            mapa_de_bits.set(1, chunk)
            print("██", end="")
        else:
            mapa_de_bits.set(0, chunk)
            print("░░", end="")
    print()

    print(f"Fazendo o download do arquivo {metadados['nome_arquivo']}")

    #urlBase = f"http://instancia1.lobato.org:9876/get/chunk/{metadados['hash_arquivo']}/"
    #urlBase = f"http://10.0.1.100:9876/get/chunk/{metadados['hash_arquivo']}/"
    urlBase = f"http://{metadados['rastreadores'][0]['endereco']}:"\
              "{metadados['rastreadores'][0]['porta']}"\
              "/get/chunk/{metadados['hash_arquivo']}/"

    err_conexao = 0
    err_timeout = 0
    err_hash    = 0
    err_interno = 0

    while mapa_de_bits.count(0) > 0 and \
            err_conexao < 10 and \
            err_timeout < 10 and \
            err_hash    < 10 and \
            err_interno < 10:
        mostra_estado_arquivo(mapa_de_bits)
        chunk_para_baixar = random.choice(list(mapa_de_bits.findall(bitstring.Bits(bin = "0b0"))))
        print(f"Tentando baixar o pedaco {chunk_para_baixar}")
        hash_esperado = metadados["hash_chunks"][chunk_para_baixar]

        url = urlBase + str(chunk_para_baixar)

        try:
            lista_de_pedacos = requests.get(url)
        except requests.exceptions.Timeout:
            print("Timeout no acesso ao servico web")
            err_timeout = err_timeout + 1
            continue
        except requests.exceptions.ConnectionError:
            print("Erro de conexao ao servico web")
            err_conexao = err_conexao + 1
            continue

        if lista_de_pedacos.status_code == 404:
            print(f"PROBLEMA SERIO: nao existem copias do chunk {chunk_para_baixar}")
            break

        if lista_de_pedacos.status_code == 500:
            print("Erro interno do servico web")
            err_interno = err_interno + 1
            continue

        lista_de_pedacos = lista_de_pedacos.json()

        print(f"Recebemos {lista_de_pedacos['numlinks']} opcoes de download para o chunk {chunk_para_baixar}")
        for tentativa in range(int(lista_de_pedacos["numlinks"])):
            print(f"Tentativa #{tentativa}")
            try:
                requisicao = requests.get(lista_de_pedacos["urls"][tentativa])
            except requests.exceptions.Timeout:
                print(f"  O repositorio nao respondeu. Tentando o proximo...")
                err_timeout = err_timeout + 1
                continue
            except requests.exceptions.ConnectionError:
                print(f"  Erro na conexao ao repositorio. Tentando o proximo...")
                err_conexao = err_conexao + 1
                continue

            try:
                requisicao.raise_for_status()
            except requests.exceptions.HTTPError:
                print(f"  A requisicao teve erro {requisicao.status_code}")
                err_interno = err_interno + 1
                continue

            dado_a_guardar = requisicao.content
            hasher = hashlib.sha1()
            hasher.update(dado_a_guardar)
            if hasher.hexdigest() == hash_esperado:
                print(f"  Parte {chunk_para_baixar} baixada com sucesso")
                arquivo.seek(chunk_para_baixar * metadados["chunksize"], os.SEEK_SET)
                arquivo.write(dado_a_guardar)
                mapa_de_bits.set(1, chunk_para_baixar)
                break
            else:
                print(f"  Parte {chunk_para_baixar} baixada com erro")
                print(f"    Esperado {hash_esperado}")
                print(f"    Obtido   {hasher.hexdigest()}")
                err_hash = err_hash + 1

    arquivo.flush()
    arquivo.close()

    print(f"Download finalizado...")
    mostra_estado_arquivo(mapa_de_bits)
    if mapa_de_bits.count(0) == 0:
        print("Arquivo baixado na totalidade com sucesso")
    else:
        print(f"Faltam {mapa_de_bits.count(0)} chunks do arquivo")
        if err_conexao >= 10:
            print(f"Download interrompido por erros de conexao")
        if err_timeout >= 10:
            print(f"Download interrompido por timeouts")
        if err_interno >= 10:
            print(f"Download interrompido por erros internos")
        if err_hash >= 10:
            print(f"Download interrompido por erros de recuperacao")
