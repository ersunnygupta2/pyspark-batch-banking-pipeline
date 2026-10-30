from datetime import datetime

from src.transformations.transaction_transformations import (
    get_valid_transactions,
    get_invalid_transactions,
    deduplicate_transactions,
    add_transaction_amount,
)

def test_transaction_validation(spark):
    data = [
        ("TXN001", "ACC001", "DEPOSIT", 100.0, datetime(2026, 10, 30, 10, 0)),
        ("TXN002", "ACC001", "DEPOSIT", -50.0, datetime(2026, 10, 30, 10, 5)),
        ("TXN003", "ACC002", "TRANSFER", 200.0, datetime(2026, 10, 30, 10, 10)),
    ]
    
    columns = [
        "transaction_id",
        "account_id",
        "transaction_type",
        "amount",
        "event_timestamp",
    ]
    
    df = spark.createDataFrame(data, columns)
    
    valid_df = get_valid_transactions(df)
    invalid_df = get_invalid_transactions(df)
    
    assert valid_df.count() == 1
    assert invalid_df.count() == 2

def test_deduplication(spark):
    data = [
        ("TXN001", "ACC001"),
        ("TXN001", "ACC001"),
        ("TXN002", "ACC002"),
    ]
    
    df = spark.createDataFrame(
        data, 
        ["transaction_id", "account_id"],
    )
    
    result_df = deduplicate_transactions(df)
    
    assert result_df.count() == 2
    

def test_signed_amount(spark):
    data = [
        ("TXN001", "DEPOSIT", 100.0),
        ("TXN002", "WITHDRAWAL", 40.0),
    ]
    
    df = spark.createDataFrame(
        data,
        ["transaction_id", "transaction_type", "amount"],
    )
    
    result = (
        add_transaction_amount(df)
        .select("signed_amount")
        .orderBy("signed_amount")
        .collect()
    )
    
    assert result[0]["signed_amount"] == -40.0
    assert result[1]["signed_amount"] == 100.0
    