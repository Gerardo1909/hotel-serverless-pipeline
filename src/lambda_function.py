"""
Módulo que contiene el handler principal de la función Lambda.

Orquesta el flujo de procesamiento: lectura de CSVs desde S3,
transformación de datos de hoteles y escritura de resultados
en formato Parquet particionado.
"""

import json
import logging
from io import BytesIO
from typing import Any

import pandas as pd

from config import settings
from processors.batch_processor import BatchProcessor
from services.s3_service import S3Service
from utils.ingestion_utils import extract_ingestion_datetime
from utils.partition_utils import build_partitioned_key

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def lambda_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    """
    Punto de entrada de la función Lambda disparada por eventos S3.

    Procesa archivos CSV de hoteles depositados en un directorio de
    ingestion dentro del bucket S3, aplica transformaciones y reglas
    de validación, y escribe los resultados (procesados y rechazados)
    como archivos Parquet particionados por fecha de ingesta.

    Args:
        event: Evento S3 con la información del objeto que disparó la Lambda.
        context: Contexto de ejecución proporcionado por AWS Lambda.

    Returns:
        Diccionario con statusCode y body indicando el resultado de la operación.

    """
    s3 = S3Service()
    processor = BatchProcessor()

    record = event["Records"][0]
    bucket: str = record["s3"]["bucket"]["name"]
    key: str = record["s3"]["object"]["key"]

    if not key.startswith(settings.RAW_PREFIX):
        logger.info("Objeto '%s' fuera del prefijo de datos crudos. Ignorado.", key)
        return {"statusCode": 200, "body": "Ignorado: prefijo no coincide"}

    prefix = "/".join(key.split("/")[:-1]) + "/"

    ingestion_dt = extract_ingestion_datetime(prefix)
    if ingestion_dt is None:
        logger.info(
            "Objeto '%s' no pertenece a un directorio de ingesta válido. Ignorado.", key
        )
        return {"statusCode": 200, "body": "Ignorado: no es directorio de ingesta"}

    batch_name = prefix.split("/")[-2]
    logger.info("Procesando lote '%s' del bucket '%s'.", batch_name, bucket)

    processed_key = build_partitioned_key(
        settings.PROCESSED_PREFIX, ingestion_dt, batch_name
    )

    file_keys = s3.list_objects(bucket, prefix)

    dataframes: list[pd.DataFrame] = []
    for file_key in file_keys:
        if not file_key.endswith(".csv"):
            continue
        content_bytes = s3.get_object(bucket, file_key)
        df = pd.read_csv(BytesIO(content_bytes))
        dataframes.append(df)

    if not dataframes:
        logger.warning("No se encontraron archivos CSV en '%s'.", prefix)
        return {"statusCode": 200, "body": "No se encontraron archivos CSV"}

    processed_bytes, rejected_bytes = processor.process_batch(dataframes)

    s3.put_object(bucket, processed_key, processed_bytes)

    rejected_key = processed_key.replace(
        settings.PROCESSED_PREFIX, settings.REJECTED_PREFIX
    )
    s3.put_object(bucket, rejected_key, rejected_bytes)

    logger.info(
        "Lote '%s' procesado. Procesados: '%s', Rechazados: '%s'.",
        batch_name,
        processed_key,
        rejected_key,
    )

    return {
        "statusCode": 200,
        "body": json.dumps(
            {
                "lote": batch_name,
                "clave_procesados": processed_key,
                "clave_rechazados": rejected_key,
            }
        ),
    }
