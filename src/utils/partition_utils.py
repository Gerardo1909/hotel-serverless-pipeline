"""
Módulo utilitario para la construcción de claves particionadas en S3.
"""

from datetime import datetime


def build_partitioned_key(
    base_prefix: str, ingestion_date: datetime, batch_name: str
) -> str:
    """
    Construye la clave S3 particionada por fecha de ingestión.

    Genera una ruta con formato Hive-style (ingestion_date=YYYY-MM-DD)
    para facilitar el descubrimiento de particiones por herramientas
    como Athena o Glue Crawler.

    Args:
        base_prefix: Prefijo base en S3 (ej: "processed/", "rejected/").
        ingestion_date: Fecha y hora de la ingestión extraída del directorio fuente.
        batch_name: Nombre del lote de datos (se usa como nombre del archivo parquet).

    Returns:
        Clave S3 completa con la partición y el nombre del archivo parquet.
    """
    partition = ingestion_date.strftime("%Y-%m-%d")
    return f"{base_prefix}ingestion_date={partition}/{batch_name}.parquet"
