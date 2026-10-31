from pipelines.extraction.queries import SOURCE_COLUMNS
from pipelines.extraction.verify_schema import compare_schema


def test_compare_schema_passes_when_all_columns_exist() -> None:
    actual = {table: set(columns) for table, columns in SOURCE_COLUMNS.items()}

    assert compare_schema(actual) == []


def test_compare_schema_reports_missing_column() -> None:
    actual = {table: set(columns) for table, columns in SOURCE_COLUMNS.items()}
    actual["inventory"].remove("stock quantity")

    issues = compare_schema(actual)

    assert len(issues) == 1
    assert issues[0].table == "inventory"
    assert issues[0].column == "stock quantity"
    assert issues[0].problem == "column missing"


def test_compare_schema_reports_missing_table() -> None:
    actual = {table: set(columns) for table, columns in SOURCE_COLUMNS.items()}
    actual.pop("customer_traffic")

    issues = compare_schema(actual)

    assert len(issues) == 1
    assert issues[0].table == "customer_traffic"
    assert issues[0].column == "*"
