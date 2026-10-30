import pytest

from src.common.spark_session import create_spark_session

@pytest.fixture(scope="session")
def spark():
    spark_session = create_spark_session("PySparkTests")
    yield spark_session
    spark_session.stop()