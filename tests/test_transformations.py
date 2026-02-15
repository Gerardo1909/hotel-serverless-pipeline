"""
Tests unitarios para el módulo de transformaciones de datos de hoteles.
"""

import pandas as pd
import pytest
import pytest_check as check

from processors.transformations import apply_transformations


@pytest.mark.unit
class TestApplyTransformations:
    """Tests para la función apply_transformations."""

    def test_checkin_date_should_be_datetime_when_input_is_string(
        self, raw_hotel_df: pd.DataFrame
    ):
        # Arrange: fixture provee DataFrame con fechas como string

        # Act
        result = apply_transformations(raw_hotel_df)

        # Assert
        assert pd.api.types.is_datetime64_any_dtype(result["checkin_date"])

    def test_checkout_date_should_be_datetime_when_input_is_string(
        self, raw_hotel_df: pd.DataFrame
    ):
        # Arrange: fixture provee DataFrame con fechas como string

        # Act
        result = apply_transformations(raw_hotel_df)

        # Assert
        assert pd.api.types.is_datetime64_any_dtype(result["checkout_date"])

    def test_noches_should_calculate_days_between_dates_when_dates_are_valid(
        self, raw_hotel_df: pd.DataFrame
    ):
        # Arrange: checkin=2026-02-16, checkout=2026-02-18 -> 2 noches

        # Act
        result = apply_transformations(raw_hotel_df)

        # Assert
        assert result["noches"].iloc[0] == 2

    def test_precios_should_be_float_when_input_has_numeric_values(
        self, raw_hotel_df: pd.DataFrame
    ):
        # Arrange: fixture provee precios numéricos

        # Act
        result = apply_transformations(raw_hotel_df)

        # Assert
        check.equal(result["precio_inicial"].dtype, float)
        check.equal(result["precio_impuesto"].dtype, float)
        check.equal(result["precio_final"].dtype, float)

    def test_precio_final_should_preserve_source_value_when_csv_provides_it(
        self, raw_hotel_df: pd.DataFrame
    ):
        # Arrange: precio_final original = 267325.0

        # Act
        result = apply_transformations(raw_hotel_df)

        # Assert
        assert result["precio_final"].iloc[0] == 267325.0

    def test_precio_por_noche_should_divide_final_by_noches_when_noches_positive(
        self, raw_hotel_df: pd.DataFrame
    ):
        # Arrange: precio_final=267325.0, noches=2

        # Act
        result = apply_transformations(raw_hotel_df)

        # Assert
        expected = 267325.0 / 2
        assert result["precio_por_noche"].iloc[0] == pytest.approx(expected)

    def test_calificacion_should_be_na_when_input_is_na_string(
        self, raw_hotel_df: pd.DataFrame
    ):
        # Arrange: calificacion="N/A" en fixture

        # Act
        result = apply_transformations(raw_hotel_df)

        # Assert
        assert pd.isna(result["calificacion"].iloc[0])

    def test_calificacion_should_preserve_value_when_input_is_valid_text(
        self, raw_hotel_df_with_scores: pd.DataFrame
    ):
        # Arrange: calificacion="Muy bueno" en fixture

        # Act
        result = apply_transformations(raw_hotel_df_with_scores)

        # Assert
        assert result["calificacion"].iloc[0] == "Muy bueno"

    def test_puntaje_should_be_nan_when_input_is_na_string(
        self, raw_hotel_df: pd.DataFrame
    ):
        # Arrange: puntaje="N/A" en fixture

        # Act
        result = apply_transformations(raw_hotel_df)

        # Assert
        assert pd.isna(result["puntaje"].iloc[0])

    def test_puntaje_should_be_numeric_when_input_is_valid_number_string(
        self, raw_hotel_df_with_scores: pd.DataFrame
    ):
        # Arrange: puntaje="8.5" en fixture

        # Act
        result = apply_transformations(raw_hotel_df_with_scores)

        # Assert
        assert result["puntaje"].iloc[0] == pytest.approx(8.5)

    def test_cantidad_reviews_should_be_zero_when_input_is_na_string(
        self, raw_hotel_df: pd.DataFrame
    ):
        # Arrange: cantidad_reviews="N/A" en fixture

        # Act
        result = apply_transformations(raw_hotel_df)

        # Assert
        assert result["cantidad_reviews"].iloc[0] == 0

    def test_cantidad_reviews_should_be_numeric_when_input_is_valid_number_string(
        self, raw_hotel_df_with_scores: pd.DataFrame
    ):
        # Arrange: cantidad_reviews="120" en fixture

        # Act
        result = apply_transformations(raw_hotel_df_with_scores)

        # Assert
        assert result["cantidad_reviews"].iloc[0] == 120

    def test_barrio_should_extract_first_segment_when_ubicacion_has_comma(
        self, raw_hotel_df: pd.DataFrame
    ):
        # Arrange: ubicacion="Palermo, Buenos Aires (Palermo Soho)"

        # Act
        result = apply_transformations(raw_hotel_df)

        # Assert
        assert result["barrio"].iloc[0] == "Palermo"

    def test_sub_barrio_should_extract_parenthetical_when_ubicacion_has_parentheses(
        self, raw_hotel_df: pd.DataFrame
    ):
        # Arrange: ubicacion="Palermo, Buenos Aires (Palermo Soho)"

        # Act
        result = apply_transformations(raw_hotel_df)

        # Assert
        assert result["sub_barrio"].iloc[0] == "Palermo Soho"

    def test_sub_barrio_should_be_empty_when_ubicacion_has_no_parentheses(
        self, raw_hotel_row: dict
    ):
        # Arrange
        row = raw_hotel_row.copy()
        row["ubicacion"] = "Centro, Buenos Aires"
        df = pd.DataFrame([row])

        # Act
        result = apply_transformations(df)

        # Assert
        assert result["sub_barrio"].iloc[0] == ""

    def test_ciudad_should_always_be_buenos_aires(self, raw_hotel_df: pd.DataFrame):
        # Arrange: fixture provee datos de Buenos Aires

        # Act
        result = apply_transformations(raw_hotel_df)

        # Assert
        assert result["ciudad"].iloc[0] == "Buenos Aires"

    def test_original_columns_should_not_be_modified_when_transformation_runs(
        self, raw_hotel_df: pd.DataFrame
    ):
        # Arrange
        original_values = raw_hotel_df.copy()

        # Act
        apply_transformations(raw_hotel_df)

        # Assert: el DataFrame original no debe ser mutado
        pd.testing.assert_frame_equal(raw_hotel_df, original_values)

    def test_new_columns_should_exist_when_transformation_completes(
        self, raw_hotel_df: pd.DataFrame
    ):
        # Arrange
        expected_new_columns = [
            "noches",
            "precio_por_noche",
            "barrio",
            "sub_barrio",
            "ciudad",
        ]

        # Act
        result = apply_transformations(raw_hotel_df)

        # Assert
        for col in expected_new_columns:
            check.is_in(col, result.columns)
