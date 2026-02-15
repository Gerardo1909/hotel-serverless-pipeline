"""
Servicio de acceso a Amazon S3.

Encapsula las operaciones de lectura, escritura y consulta de objetos
en buckets de S3 utilizando boto3.
"""

import logging

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


class S3Service:
    """Cliente simplificado para operaciones comunes sobre Amazon S3."""

    def __init__(self) -> None:
        self._client = boto3.client("s3")

    def get_object(self, bucket: str, key: str) -> bytes:
        """Descarga el contenido de un objeto de S3 como bytes.

        Args:
            bucket: Nombre del bucket.
            key: Clave (ruta) del objeto dentro del bucket.

        Returns:
            Contenido del objeto en bytes.

        Raises:
            ClientError: Si la operación de lectura falla en S3.
        """
        try:
            response = self._client.get_object(Bucket=bucket, Key=key)
            return response["Body"].read()
        except ClientError:
            logger.error(
                "Error al descargar objeto. Bucket: '%s', Key: '%s'.",
                bucket,
                key,
                exc_info=True,
            )
            raise

    def put_object(self, bucket: str, key: str, body: bytes) -> None:
        """Sube un objeto al bucket de S3.

        Args:
            bucket: Nombre del bucket.
            key: Clave (ruta) de destino del objeto.
            body: Contenido del objeto en bytes.

        Raises:
            ClientError: Si la operación de escritura falla en S3.
        """
        try:
            self._client.put_object(Bucket=bucket, Key=key, Body=body)
        except ClientError:
            logger.error(
                "Error al subir objeto. Bucket: '%s', Key: '%s'.",
                bucket,
                key,
                exc_info=True,
            )
            raise

    def list_objects(self, bucket: str, prefix: str) -> list[str]:
        """Lista las claves de objetos que coinciden con un prefijo.

        Args:
            bucket: Nombre del bucket.
            prefix: Prefijo para filtrar objetos.

        Returns:
            Lista de claves encontradas bajo el prefijo dado.

        Raises:
            ClientError: Si la operación de listado falla en S3.
        """
        try:
            response = self._client.list_objects_v2(Bucket=bucket, Prefix=prefix)
            return [obj["Key"] for obj in response.get("Contents", [])]
        except ClientError:
            logger.error(
                "Error al listar objetos. Bucket: '%s', Prefix: '%s'.",
                bucket,
                prefix,
                exc_info=True,
            )
            raise

    def object_exists(self, bucket: str, key: str) -> bool:
        """Verifica si un objeto existe en el bucket.

        Args:
            bucket: Nombre del bucket.
            key: Clave (ruta) del objeto.

        Returns:
            True si el objeto existe, False en caso contrario.
        """
        try:
            self._client.head_object(Bucket=bucket, Key=key)
            return True
        except ClientError as e:
            # 404 es el caso esperado cuando el objeto no existe
            error_code = e.response.get("Error", {}).get("Code", "")
            if error_code == "404":
                return False
            logger.error(
                "Error al verificar existencia del objeto. Bucket: '%s', Key: '%s'.",
                bucket,
                key,
                exc_info=True,
            )
            raise
