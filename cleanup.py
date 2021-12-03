import json

import boto3
from pathlib import Path

def load_config(configfile: Path) -> dict:
    if not configfile.exists():
        print("Problema para ler o arquivo de configuracao")
        print("> " + str(configfile.resolve()))
        exit(1)
    with configfile.open() as config:
        config_to_return = json.load(config)
    return config_to_return


def limpa_fila(credencial_dict: dict,
               config_dict: dict):
    sqs = boto3.resource("sqs",
                             aws_access_key_id = credencial_dict["AccessKey"],
                             aws_secret_access_key = credencial_dict["SecretAccessKey"],
                             region_name = 'sa-east-1')

    try:
        queue = sqs.meta.client.get_queue_url(QueueName = config_dict.get("FilaDeEntrada"))
    except sqs.meta.client.exceptions.QueueDoesNotExist:
        print("Fila de entrada nao existia")
        pass
    else:
        sqs.meta.client.delete_queue(QueueUrl = queue.get("QueueUrl"))
        print("Fila de entrada removida")


def limpa_entrada(credencial_dict: dict,
                  config_dict: dict):
    s3 = boto3.resource("s3",
                        aws_access_key_id = credencial_dict["AccessKey"],
                        aws_secret_access_key = credencial_dict["SecretAccessKey"])
    bucket = s3.Bucket(config_dict.get("BucketEntrada"))
    try:
        for objeto in bucket.objects.all():
            print(f"Removendo {objeto.key} do bucket de entrada")
            objeto.delete()
        print("Removendo o bucket de entrada")
        bucket.delete()
    except s3.meta.client.exceptions.NoSuchBucket:
        print("Bucket de entrada nao existe")
        pass


def limpa_fragmentos(credencial_dict: dict,
                     config_dict: dict):
    s3 = boto3.resource("s3",
                        aws_access_key_id = credencial_dict["AccessKey"],
                        aws_secret_access_key = credencial_dict["SecretAccessKey"])
    for bucket_name in config_dict["Buckets"]:
        bucket = s3.Bucket(bucket_name.get("nome"))
        try:
            for objeto in bucket.objects.all():
                print(f"Removendo {objeto.key} do {bucket_name.get('friendly_name').lower()}")
                objeto.delete()
            print(f"Removendo o bucket {bucket_name.get('friendly_name').lower()}")
            bucket.delete()
        except s3.meta.client.exceptions.NoSuchBucket:
            print(f"{bucket_name.get('friendly_name').lower()} nao existe")
            pass


def limpa_tabelas(credencial_dict: dict,
                  config_dict: dict):
    db_connection = boto3.resource("dynamodb",
                                   region_name = 'sa-east-1',
                                   aws_access_key_id = credencial_dict.get("AccessKey"),
                                   aws_secret_access_key = credencial_dict.get("SecretAccessKey"))

    table = db_connection.Table("distribuicao")
    try:
        table.delete()
    except table.meta.client.exceptions.ResourceNotFoundException:
        print("Tabela de distribuicao nao existe")
    except table.meta.client.exceptions.ResourceInUseException:
        print("Tabela de distribuicao esta em uso. Tentar novamente depois")
    else:
        print("Tabela de distribuicao removida")

    table = db_connection.Table("metadados")
    try:
        table.delete()
    except table.meta.client.exceptions.ResourceNotFoundException:
        print("Tabela de metadados nao existe")
    except table.meta.client.exceptions.ResourceInUseException:
        print("Tabela de metadados esta em uso. Tentar novamente depois")
    else:
        print("Tabela de metadados removida")


if __name__ == "__main__":
    print("Limpando tudo para comecar novamente")

    credencial = load_config(Path("secrets.json").resolve())
    config = load_config(Path("config.json").resolve())

    loop = True

    while loop:
        print("Vamos limpar o que?")
        print("1. Fila de entrada")
        print("2. Bucket de entrada")
        print("3. Buckets dos fragmentos")
        print("4. Tabelas")
        print("0. Finalizar")

        escolha = int(input("> "))

        if escolha == 1:
            limpa_fila(credencial, config)
        elif escolha == 2:
            limpa_entrada(credencial, config)
        elif escolha == 3:
            limpa_fragmentos(credencial, config)
        elif escolha == 4:
            limpa_tabelas(credencial, config)
        elif escolha == 0:
            loop = False
        else:
            print("Escolha invalida")
