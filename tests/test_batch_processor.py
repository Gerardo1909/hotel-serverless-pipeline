"""
Tests unitarios para el módulo de procesamiento por lotes de datos de hoteles.
"""

from io import BytesIO

import pandas as pd
import pytest
import pytest_check as check

from processors.batch_processor import BatchProcessor


@pytest.fixture
def processor() -> BatchProcessor:
    return BatchProcessor()


@pytest.mark.unit
class TestProcessBatch:
    """Tests para el método process_batch del BatchProcessor."""

    def test_process_batch_should_return_two_byte_objects_when_input_is_valid(
        self, processor: BatchProcessor, raw_hotel_df_multiple: list[pd.DataFrame]
    ):
        # Arrange: fixture provee lista de DataFrames válidos

        # Act
        processed_bytes, rejected_bytes = processor.process_batch(raw_hotel_df_multiple)

        # Assert
        check.is_instance(processed_bytes, bytes)
        check.is_instance(rejected_bytes, bytes)

    def test_process_batch_should_produce_valid_parquet_when_input_is_valid(
        self, processor: BatchProcessor, raw_hotel_df_multiple: list[pd.DataFrame]
    ):
        # Arrange: fixture provee lista de DataFrames válidos

        # Act
        processed_bytes, rejected_bytes = processor.process_batch(raw_hotel_df_multiple)

        # Assert: ambos outputs deben ser legibles como parquet
        processed_df = pd.read_parquet(BytesIO(processed_bytes))
        rejected_df = pd.read_parquet(BytesIO(rejected_bytes))
        check.greater(len(processed_df), 0)
        check.greater_equal(len(rejected_df), 0)

    def test_process_batch_should_combine_dataframes_when_multiple_inputs_provided(
        self, processor: BatchProcessor, raw_hotel_df_multiple: list[pd.DataFrame]
    ):
        # Arrange: fixture provee 2 DataFrames con 1 fila cada uno

        # Act
        processed_bytes, _ = processor.process_batch(raw_hotel_df_multiple)

        # Assert
        result = pd.read_parquet(BytesIO(processed_bytes))
        assert len(result) == 2

    def test_process_batch_should_keep_record_when_all_fields_are_valid(
        self, processor: BatchProcessor, raw_hotel_df_with_scores: pd.DataFrame
    ):
        # Arrange: fixture con puntaje=8.5, precio_final válido, 2 noches

        # Act
        processed_bytes, rejected_bytes = processor.process_batch(
            [raw_hotel_df_with_scores]
        )

        # Assert
        processed_df = pd.read_parquet(BytesIO(processed_bytes))
        rejected_df = pd.read_parquet(BytesIO(rejected_bytes))
        check.equal(len(processed_df), 1)
        check.equal(len(rejected_df), 0)

    def test_process_batch_should_keep_record_when_puntaje_is_nan(
        self, processor: BatchProcessor, raw_hotel_df: pd.DataFrame
    ):
        # Arrange: fixture con puntaje="N/A" que se convierte a NaN

        # Act
        processed_bytes, rejected_bytes = processor.process_batch([raw_hotel_df])

        # Assert: puntaje NaN es válido (hotel sin evaluaciones)
        processed_df = pd.read_parquet(BytesIO(processed_bytes))
        rejected_df = pd.read_parquet(BytesIO(rejected_bytes))
        check.equal(len(processed_df), 1)
        check.equal(len(rejected_df), 0)


@pytest.mark.unit
class TestRejectionRules:
    """Tests para las reglas de rechazo del BatchProcessor."""

    def test_process_batch_should_reject_record_when_precio_final_is_negative(
        self,
        processor: BatchProcessor,
        raw_hotel_df_invalid_precio: pd.DataFrame,
    ):
        # Arrange: fixture con precio_final=-5000.0

        # Act
        processed_bytes, rejected_bytes = processor.process_batch(
            [raw_hotel_df_invalid_precio]
        )

        # Assert
        processed_df = pd.read_parquet(BytesIO(processed_bytes))
        rejected_df = pd.read_parquet(BytesIO(rejected_bytes))
        check.equal(len(processed_df), 0)
        check.equal(len(rejected_df), 1)

    def test_process_batch_should_reject_record_when_precio_final_is_zero(
        self, processor: BatchProcessor, raw_hotel_row: dict
    ):
        # Arrange
        row = raw_hotel_row.copy()
        row["precio_final"] = 0.0
        df = pd.DataFrame([row])

        # Act
        processed_bytes, rejected_bytes = processor.process_batch([df])

        # Assert
        processed_df = pd.read_parquet(BytesIO(processed_bytes))
        rejected_df = pd.read_parquet(BytesIO(rejected_bytes))
        check.equal(len(processed_df), 0)
        check.equal(len(rejected_df), 1)

    def test_process_batch_should_reject_record_when_noches_is_zero(
        self,
        processor: BatchProcessor,
        raw_hotel_df_zero_noches: pd.DataFrame,
    ):
        # Arrange: fixture con checkin == checkout -> 0 noches

        # Act
        processed_bytes, rejected_bytes = processor.process_batch(
            [raw_hotel_df_zero_noches]
        )

        # Assert
        processed_df = pd.read_parquet(BytesIO(processed_bytes))
        rejected_df = pd.read_parquet(BytesIO(rejected_bytes))
        check.equal(len(processed_df), 0)
        check.equal(len(rejected_df), 1)

    def test_process_batch_should_reject_record_when_puntaje_exceeds_ten(
        self,
        processor: BatchProcessor,
        raw_hotel_df_invalid_puntaje: pd.DataFrame,
    ):
        # Arrange: fixture con puntaje=15.0

        # Act
        processed_bytes, rejected_bytes = processor.process_batch(
            [raw_hotel_df_invalid_puntaje]
        )

        # Assert
        processed_df = pd.read_parquet(BytesIO(processed_bytes))
        rejected_df = pd.read_parquet(BytesIO(rejected_bytes))
        check.equal(len(processed_df), 0)
        check.equal(len(rejected_df), 1)

    def test_process_batch_should_reject_record_when_puntaje_is_negative(
        self, processor: BatchProcessor, raw_hotel_row: dict
    ):
        # Arrange
        row = raw_hotel_row.copy()
        row["puntaje"] = "-1.0"
        df = pd.DataFrame([row])

        # Act
        processed_bytes, rejected_bytes = processor.process_batch([df])

        # Assert
        processed_df = pd.read_parquet(BytesIO(processed_bytes))
        rejected_df = pd.read_parquet(BytesIO(rejected_bytes))
        check.equal(len(processed_df), 0)
        check.equal(len(rejected_df), 1)

    def test_process_batch_should_keep_record_when_puntaje_is_boundary_zero(
        self, processor: BatchProcessor, raw_hotel_row: dict
    ):
        # Arrange
        row = raw_hotel_row.copy()
        row["puntaje"] = "0.0"
        df = pd.DataFrame([row])

        # Act
        processed_bytes, _ = processor.process_batch([df])

        # Assert
        processed_df = pd.read_parquet(BytesIO(processed_bytes))
        assert len(processed_df) == 1

    def test_process_batch_should_keep_record_when_puntaje_is_boundary_ten(
        self, processor: BatchProcessor, raw_hotel_row: dict
    ):
        # Arrange
        row = raw_hotel_row.copy()
        row["puntaje"] = "10.0"
        df = pd.DataFrame([row])

        # Act
        processed_bytes, _ = processor.process_batch([df])

        # Assert
        processed_df = pd.read_parquet(BytesIO(processed_bytes))
        assert len(processed_df) == 1

    def test_process_batch_should_split_valid_and_invalid_when_mixed_input(
        self, processor: BatchProcessor, raw_hotel_row: dict
    ):
        # Arrange: 2 filas válidas + 1 con precio negativo + 1 con puntaje > 10
        valid_1 = raw_hotel_row.copy()
        valid_2 = raw_hotel_row.copy()
        valid_2["puntaje"] = "7.0"

        invalid_price = raw_hotel_row.copy()
        invalid_price["precio_final"] = -100.0

        invalid_score = raw_hotel_row.copy()
        invalid_score["puntaje"] = "11.0"

        df = pd.DataFrame([valid_1, valid_2, invalid_price, invalid_score])

        # Act
        processed_bytes, rejected_bytes = processor.process_batch([df])

        # Assert
        processed_df = pd.read_parquet(BytesIO(processed_bytes))
        rejected_df = pd.read_parquet(BytesIO(rejected_bytes))
        check.equal(len(processed_df), 2)
        check.equal(len(rejected_df), 2)
