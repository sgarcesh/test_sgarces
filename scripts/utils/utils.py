import os
import shutil
from typing import List

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.utils import AnalysisException
from pyspark.sql.types import TimestampType
from pyspark.sql import functions as F


def upsert_parquet(
    spark: SparkSession,
    source_df: DataFrame,             # datos nuevos / actualizados
    target_path: str,                 # ruta parquet "histórica"
    key_columns: List[str],           # claves de negocio
    partition_columns: List[str] = None,  # columnas de partición (opcional)
    created_column: str = "created_ts",
    update_column: str = "update_ts",
):
    """
    UPSERT sobre archivos Parquet con columnas de tracking temporal:

    - Si NO existe data en target_path:
        * Escribe source_df
        * created_ts = now, update_ts = now.

    - Si SÍ existe:
        * Nuevas claves:
            - created_ts = now
            - update_ts = now
        * Claves existentes (UPDATE):
            - Mantiene created_ts original
            - update_ts = now
        * Registros no tocados:
            - Se mantienen tal cual.

    Implementación:
        1) target_without_updates = target_df LEFT ANTI keys(source)
        2) source_new vs source_existing (nuevas vs que ya estaban)
        3) Unión final:
            target_without_updates
            ∪ source_existing_con_metas
            ∪ source_new_con_metas

        Todo se escribe primero a una carpeta temporal y luego se reemplaza target_path.
    """

    tmp_path = target_path + "_tmp"

    # 1. Intentar leer el Parquet existente (target)
    try:
        target_df = spark.read.parquet(target_path)
        target_exists = True
    except AnalysisException:
        target_exists = False

    # 2. Caso base: no existe tabla/parquet aún
    if not target_exists:
        now_col = F.current_timestamp()
        source_with_meta = (
            source_df
            .withColumn(created_column, now_col)
            .withColumn(update_column, now_col)
        )

        writer = source_with_meta.write.mode("overwrite")
        if partition_columns:
            writer = writer.partitionBy(partition_columns)
        writer.parquet(target_path)
        return

    # 3. Asegurar que el target tenga columnas de tracking (si vienen de una versión vieja)
    if created_column not in target_df.columns:
        target_df = target_df.withColumn(created_column, F.lit(None).cast(TimestampType()))
    if update_column not in target_df.columns:
        target_df = target_df.withColumn(update_column, F.lit(None).cast(TimestampType()))

    # 4. Obtener claves existentes en el target
    target_keys = target_df.select(*key_columns).dropDuplicates()

    # 5. Separar source en NUEVOS y EXISTENTES
    source_new = source_df.join(target_keys, on=key_columns, how="left_anti")
    source_existing = source_df.join(target_keys, on=key_columns, how="inner")

    # 6. Registros del target que NO se actualizan (no están en el source)
    target_without_updates = target_df.join(
        target_keys,                # mismas claves que en source
        on=key_columns,
        how="left_anti"
    )

    # 7. Preparar registros EXISTENTES: conservar created_ts original y actualizar update_ts
    #    Tomamos created_ts desde el target original
    t_meta = target_df.select(
        *key_columns,
        target_df[created_column].alias("__created_old")
    )

    source_existing_joined = source_existing.join(t_meta, on=key_columns, how="left")

    now_col = F.current_timestamp()

    source_existing_meta = (
        source_existing_joined
        .withColumn(created_column, F.col("__created_old"))
        .withColumn(update_column, now_col)
        .drop("__created_old")
    )

    # 8. Preparar registros NUEVOS: created_ts = now, update_ts = now
    source_new_meta = (
        source_new
        .withColumn(created_column, now_col)
        .withColumn(update_column, now_col)
    )

    # 9. Unir todo
    final_df = (
        target_without_updates
        .unionByName(source_existing_meta)
        .unionByName(source_new_meta)
    )

    # 10. Escribir en carpeta temporal
    writer = final_df.write.mode("overwrite")
    if partition_columns:
        writer = writer.partitionBy(partition_columns)
    writer.parquet(tmp_path)

    # 11. Reemplazar destino
    if os.path.exists(target_path):
        shutil.rmtree(target_path)
    os.rename(tmp_path, target_path)

