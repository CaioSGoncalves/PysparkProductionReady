import os
import shutil
import pandas as pd
from jobs import purchase


class TestPurchaseJob:
    def test_transform_data(self, spark_session):

        test_data = spark_session.createDataFrame(
            [(1, 1, 1, 10, "2019-02-01 15:00:00"), (2, 2, 2, 20, "2019-02-01 16:00:00")],
            ["purchase_id", "user_id", "product_id", "quantity", "timestamp"],
        )

        expected_data = spark_session.createDataFrame(
            [(1, 1, 1, 10, "2019-02-01 15:00:00", 1, 2, 2019), (2, 2, 2, 20, "2019-02-01 16:00:00", 1, 2, 2019)],
            ["purchase_id", "user_id", "product_id", "quantity", "timestamp", "day", "month", "year"],
        ).toPandas()

        real_data = purchase._transform_data(spark_session, test_data).toPandas()

        pd.testing.assert_frame_equal(real_data, expected_data, check_dtype=False)
