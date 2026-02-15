"""
Fixtures compartidas para los tests del pipeline de transformación de hoteles.
"""

import pandas as pd
import pytest


@pytest.fixture
def raw_hotel_row() -> dict:
    """Fila individual con datos válidos de un hotel."""
    return {
        "nombre_hotel": "Trendy Apartments in Palermo - by BueRentals",
        "ubicacion": "Palermo, Buenos Aires (Palermo Soho)",
        "checkin_date": "2026-02-16",
        "checkout_date": "2026-02-18",
        "precio_inicial": 201338.0,
        "precio_impuesto": 65987.0,
        "precio_final": 267325.0,
        "calificacion": "N/A",
        "puntaje": "N/A",
        "cantidad_reviews": "N/A",
        "link_detalle": "https://www.booking.com/hotel/ar/trendy-luxury-in-buenos-aires.html",
    }


@pytest.fixture
def raw_hotel_df(raw_hotel_row: dict) -> pd.DataFrame:
    """DataFrame con una fila válida de hotel."""
    return pd.DataFrame([raw_hotel_row])


@pytest.fixture
def raw_hotel_df_with_scores(raw_hotel_row: dict) -> pd.DataFrame:
    """DataFrame con puntaje y calificación numéricos válidos."""
    row = raw_hotel_row.copy()
    row["calificacion"] = "Muy bueno"
    row["puntaje"] = "8.5"
    row["cantidad_reviews"] = "120"
    return pd.DataFrame([row])


@pytest.fixture
def raw_hotel_df_multiple() -> list[pd.DataFrame]:
    """Dos DataFrames simulando dos archivos CSV de un mismo lote."""
    df1 = pd.DataFrame(
        [
            {
                "nombre_hotel": "Hotel A",
                "ubicacion": "Palermo, Buenos Aires (Palermo Soho)",
                "checkin_date": "2026-02-16",
                "checkout_date": "2026-02-18",
                "precio_inicial": 100000.0,
                "precio_impuesto": 30000.0,
                "precio_final": 130000.0,
                "calificacion": "Muy bueno",
                "puntaje": "8.5",
                "cantidad_reviews": "120",
                "link_detalle": "https://www.booking.com/hotel/ar/hotel-a.html",
            }
        ]
    )
    df2 = pd.DataFrame(
        [
            {
                "nombre_hotel": "Hotel B",
                "ubicacion": "Recoleta, Buenos Aires (Recoleta)",
                "checkin_date": "2026-03-01",
                "checkout_date": "2026-03-05",
                "precio_inicial": 250000.0,
                "precio_impuesto": 80000.0,
                "precio_final": 330000.0,
                "calificacion": "Excelente",
                "puntaje": "9.2",
                "cantidad_reviews": "340",
                "link_detalle": "https://www.booking.com/hotel/ar/hotel-b.html",
            }
        ]
    )
    return [df1, df2]


@pytest.fixture
def raw_hotel_df_invalid_precio(raw_hotel_row: dict) -> pd.DataFrame:
    """DataFrame con precio_final negativo (debe ser rechazado)."""
    row = raw_hotel_row.copy()
    row["precio_final"] = -5000.0
    return pd.DataFrame([row])


@pytest.fixture
def raw_hotel_df_invalid_puntaje(raw_hotel_row: dict) -> pd.DataFrame:
    """DataFrame con puntaje fuera de rango (debe ser rechazado)."""
    row = raw_hotel_row.copy()
    row["puntaje"] = "15.0"
    return pd.DataFrame([row])


@pytest.fixture
def raw_hotel_df_zero_noches(raw_hotel_row: dict) -> pd.DataFrame:
    """DataFrame con checkin == checkout, 0 noches (debe ser rechazado)."""
    row = raw_hotel_row.copy()
    row["checkout_date"] = row["checkin_date"]
    return pd.DataFrame([row])
