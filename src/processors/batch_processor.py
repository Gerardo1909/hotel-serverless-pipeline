"""
Módulo que contiene el procesador batch de datos de hoteles.
Combina múltiples DataFrames, aplica transformaciones y separa
registros válidos de rechazados según reglas de calidad de datos.
"""

from io import BytesIO

import pandas as pd

from processors.transformations import apply_transformations


class BatchProcessor:
    """
    Procesador que unifica, transforma y clasifica lotes de datos hoteleros.
    """

    def process_batch(self, dataframes: list[pd.DataFrame]) -> tuple[bytes, bytes]:
        """
        Procesa un lote de DataFrames aplicando transformaciones y reglas de rechazo.

        Combina todos los DataFrames en uno solo, aplica las transformaciones
        de negocio y separa los registros válidos de los rechazados según
        reglas de calidad de datos.

        Args:
            dataframes: Lista de DataFrames con datos crudos de hoteles.

        Returns:
            Tupla con (bytes_procesados_parquet, bytes_rechazados_parquet).
        """
        combined_df = pd.concat(dataframes, ignore_index=True)
        transformed_df = apply_transformations(combined_df)

        # Reglas de rechazo: precio positivo, al menos 1 noche,
        # y puntaje dentro de rango válido (0-10) o ausente
        valid_mask = (
            (transformed_df["precio_final"] > 0)
            & (transformed_df["noches"] > 0)
            & (
                transformed_df["puntaje"].isna()
                | ((transformed_df["puntaje"] >= 0) & (transformed_df["puntaje"] <= 10))
            )
        )

        processed_df = transformed_df[valid_mask]
        rejected_df = transformed_df[~valid_mask]

        processed_buffer = BytesIO()
        rejected_buffer = BytesIO()

        processed_df.to_parquet(processed_buffer, index=False)
        rejected_df.to_parquet(rejected_buffer, index=False)

        return processed_buffer.getvalue(), rejected_buffer.getvalue()
