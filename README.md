# Sybase ASE -> Oracle Migrator (Java 17)

## Goal

Generic migration engine for many Sybase ASE tables to Oracle, including tables with
millions/tens of millions of rows.

The engine:
- reads source/target table mappings from `tables.csv`;
- reads column metadata from both databases;
- compares source and target columns;
- automatically maps common ASE -> Oracle types;
- treats Sybase ASE `timestamp` as an 8-byte binary value, NOT as a date;
- writes ASE timestamp bytes to Oracle `RAW(8)` with `setBytes()`;
- streams each table through a forward-only JDBC `ResultSet`; it does not require a primary key, unique column, or non-null identifier;
- uses `fetchSize` to request progressive retrieval from the Sybase JDBC driver;
- inserts with JDBC batches and commits periodically;
- logs elapsed time and row counts for each table, plus total run duration and counts;
- writes errors and a validation report.

## Important: Sybase ASE timestamp

In ASE, `timestamp` is an 8-byte binary/version value. It is NOT a date/time.
The migration therefore does:

    Sybase timestamp -> ResultSet.getBytes() -> Oracle PreparedStatement.setBytes()

If the target is RAW(8), the 8 bytes are copied without interpretation.

For an ASE column whose JDBC metadata says BINARY/VARBINARY but whose declared type name is
`timestamp`, the metadata reader uses `TYPE_NAME` and detects it as ASE TIMESTAMP.
You can also force it with `column-mappings.csv` using `sourceTypeOverride=timestamp`.

## Streaming and duplicate/null handling

The migration reads each table with one forward-only `SELECT` and iterates through its `ResultSet`.
It does not use `WHERE id > ?`, does not sort by a guessed ID, and does not require a primary key.
Therefore duplicate values in ordinary columns, composite primary keys, tables without a primary key,
and nullable non-key columns do not cause rows to be skipped by keyset pagination.

`migration.fetchSize` requests the number of rows the JDBC driver should fetch at a time. Confirm
that the installed Sybase jConnect driver honors fetch-size/streaming behavior for your configuration;
this property alone does not guarantee that the driver never buffers more data internally. Large LOB
values are materialized one row at a time before the next source row is read.

This mode is not automatically restart-safe: if a run fails after earlier Oracle commits, do not simply
rerun into a non-empty target. First clean/reconcile the target table, or implement a separately tested
restart/idempotency strategy. For a consistent copy, avoid source-table changes while migration is running
or use a source snapshot/transaction strategy supported by your Sybase environment.

## Logging and reports

Console output includes progress, per-table row counts and durations, and a global run summary.
The `migration.logDirectory` folder receives:

- `migration-report.csv`: column/type compatibility checks;
- `migration-tables-summary.csv`: per-table rows read, rows committed, elapsed time, status and error;
- `migration-run-summary.csv`: total duration, tables attempted/succeeded/failed and rows committed in completed tables.

Durations use `HH:mm:ss.SSS` format.

## Recommended procedure

1. Put the Sybase jConnect JAR in `lib/` (for example jconn4.jar or the version supplied by your company).
2. Configure `application.properties`.
3. Configure `tables.csv`.
4. Optionally configure `column-mappings.csv`.
5. Run CHECK first:
       java -cp "target/classes;lib/*;..." com.example.migrator.MigrationApplication CHECK
6. Review logs/migration-report.csv.
7. Run:
       java -cp "target/classes;lib/*;..." com.example.migrator.MigrationApplication MIGRATE

With the assembly JAR:
       java -jar target/sybase-oracle-migrator-2.1.0-jar-with-dependencies.jar CHECK
       java -jar target/sybase-oracle-migrator-2.1.0-jar-with-dependencies.jar MIGRATE

Note: the assembly contains Oracle/SLF4J dependencies but not the Sybase jConnect driver.
Keep the Sybase JAR in `lib/` and use the classpath launch if the driver is not packaged.

## Type mapping

Typical mappings:
- int/smallint/tinyint/usmallint/bigint -> Oracle NUMBER
- numeric/decimal -> Oracle NUMBER
- varchar -> VARCHAR2
- char -> CHAR
- datetime/smalldatetime -> TIMESTAMP
- date -> DATE
- time -> TIME/TIMESTAMP depending on target metadata
- text -> CLOB
- image -> BLOB
- float -> BINARY_DOUBLE/NUMBER depending on target metadata
- ASE timestamp -> RAW(8) when target is RAW(8)

The program validates actual source and target metadata before migration.

## Security

Do not commit passwords. For production, use environment variables or an external
properties file and restrict file permissions.
