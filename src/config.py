"""
Módulo que contiene la configuración centralizada del sistema.
"""

from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    """
    Configuración global e inmutable del pipeline de transformación.
    """

    RAW_PREFIX: str = "raw/"
    PROCESSED_PREFIX: str = "processed/"
    REJECTED_PREFIX: str = "rejected/"


settings = Settings()
