"""
Módulo de utilidades para la extracción de metadatos de ingestión.
"""

import re
from datetime import datetime
from typing import Optional


def extract_ingestion_datetime(prefix: str) -> Optional[datetime]:
    """
    Extrae la fecha y hora de ingestión a partir del nombre del directorio.

    Busca el patrón 'ingestion_YYYYMMDD_HHMMSS' dentro del prefijo proporcionado
    y lo convierte en un objeto datetime.

    Args:
        prefix: Prefijo o ruta del directorio que contiene el patrón de ingestión.

    Returns:
        Objeto datetime con la fecha/hora de ingestión, o None si el patrón no coincide.
    """
    match = re.search(r"ingestion_(\d{8})_(\d{6})", prefix)
    if not match:
        return None

    date_part, time_part = match.groups()
    return datetime.strptime(date_part + time_part, "%Y%m%d%H%M%S")
