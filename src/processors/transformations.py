"""
Modulo de transformaciones de datos especificas para hoteles de Booking.com.
"""

import pandas as pd


def apply_transformations(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aplica las transformaciones de negocio sobre los datos crudos de hoteles.

    Transformaciones realizadas:
        - Conversion de fechas de checkin/checkout a datetime.
        - Calculo de noches de estadia.
        - Casteo de precios.
        - Calculo de precio por noche.
        - Limpieza de puntaje, calificacion y cantidad de reviews.
        - Extraccion de barrio y sub-barrio desde la ubicacion.

    Args:
        df: DataFrame crudo con las columnas del CSV de hoteles.

    Returns:
        DataFrame transformado con columnas adicionales calculadas.
    """
    df = df.copy()

    # -- Fechas y noches --
    df["checkin_date"] = pd.to_datetime(df["checkin_date"])
    df["checkout_date"] = pd.to_datetime(df["checkout_date"])
    df["noches"] = (df["checkout_date"] - df["checkin_date"]).dt.days

    # -- Precios --
    df["precio_inicial"] = df["precio_inicial"].astype(float)
    df["precio_impuesto"] = df["precio_impuesto"].astype(float)
    df["precio_final"] = df["precio_final"].astype(float)
    df["precio_por_noche"] = df["precio_final"] / df["noches"]

    # -- Metricas de evaluacion --
    df["calificacion"] = df["calificacion"].replace("N/A", pd.NA)
    df["puntaje"] = pd.to_numeric(df["puntaje"], errors="coerce")
    df["cantidad_reviews"] = pd.to_numeric(
        df["cantidad_reviews"], errors="coerce"
    ).fillna(0)

    # -- Ubicacion --
    df["barrio"] = df["ubicacion"].str.split(",").str[0].str.strip()
    df["sub_barrio"] = _extraer_sub_barrio(df["ubicacion"])
    df["ciudad"] = "Buenos Aires"

    return df


def _extraer_sub_barrio(ubicacion: pd.Series) -> pd.Series:
    """Extrae el sub-barrio desde el texto entre parentesis de la ubicacion.

    Ejemplo: 'Palermo, Buenos Aires (Palermo Soho)' -> 'Palermo Soho'
    """
    return ubicacion.str.extract(r"\(([^)]+)\)", expand=False).fillna("")
