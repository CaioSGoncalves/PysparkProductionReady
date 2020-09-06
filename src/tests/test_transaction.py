import os
import shutil
import pandas as pd
from jobs import transaction


class TestTransactionJob:
    def test_transform_data(self, spark_session):

        test_data = spark_session.createDataFrame(
            [
                (1, "DEPOSITO", 200, "2019-02-01 15:00:00"),
                (1, "DEPOSITO", 100, "2019-02-02 15:00:00"),
                (2, "DEPOSITO", 500, "2019-02-02 15:00:00"),
                (1, "SAQUE", 150, "2019-02-03 15:00:00"),
                (2, "DEPOSITO", 500, "2019-02-03 15:00:00"),
            ],
            ["user_id", "type", "value", "timestamp"],
        )

        expected_data = spark_session.createDataFrame(
            [(1, 150), (2, 1000)],
            ["user_id", "balance"],
        ).toPandas()

        real_data = transaction._transform_data(spark_session, test_data).toPandas()

        pd.testing.assert_frame_equal(real_data, expected_data, check_dtype=False)
