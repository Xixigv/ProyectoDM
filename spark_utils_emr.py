"""
spark_utils_emr.py
===================
Módulo de utilería compartido para subir a S3 y usar con --py-files
en un Step de Amazon EMR (basado en spark_utils_emr.py de la Sesión 14).

Subida a S3:
    aws s3 cp spark_utils_emr.py s3://<tu-bucket>/src/spark_utils_emr.py
"""

from pyspark.sql.types import (
    BooleanType,
    DateType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


class SparkUtils:
    types_dict = {
        "StringType": StringType(),
        "IntegerType": IntegerType(),
        "LongType": LongType(),
        "DoubleType": DoubleType(),
        "FloatType": FloatType(),
        "BooleanType": BooleanType(),
        "DateType": DateType(),
        "TimestampType": TimestampType(),
    }

    @staticmethod
    def generate_schema(columns_info) -> StructType:
        """
        Construye un StructType a partir de una lista de tuplas
        (nombre_columna, tipo_como_string).
        """
        return StructType([
            StructField(name, SparkUtils.types_dict[type_str], True)
            for name, type_str in columns_info
        ])
