package com.example.migrator;

import java.math.BigDecimal;
import java.nio.file.Path;
import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

public final class MigrationService {
    private final Db sybase;
    private final Db oracle;
    private final MetadataService metadata;
    private final StateStore state;
    private final Report report;
    private final int batchSize;
    private final int rangeSize;
    private final int fetchSize;
    private final int commitEveryBatches;

    public MigrationService(Db sybase, Db oracle, MetadataService metadata, StateStore state,
                            Report report, int batchSize, int rangeSize, int fetchSize, int commitEveryBatches) {
        this.sybase = sybase; this.oracle = oracle; this.metadata = metadata;
        this.state = state; this.report = report; this.batchSize = batchSize;
        this.rangeSize = rangeSize; this.fetchSize = fetchSize; this.commitEveryBatches = commitEveryBatches;
    }

    public void check(TableConfig t, List<ColumnMapping> mappings) throws Exception {
        List<ColumnInfo> src = metadata.readColumns(sybase.connection(), t.sourceSchema(), t.sourceTable());
        List<ColumnInfo> dst = metadata.readColumns(oracle.connection(), t.targetSchema(), t.targetTable());
        List<ResolvedColumn> resolved = resolve(src, dst, t.sourceTable(), mappings);

        System.out.println("\n=== CHECK " + t.sourceSchema() + "." + t.sourceTable() +
                " -> " + t.targetSchema() + "." + t.targetTable() + " ===");

        boolean ok = true;
        for (ResolvedColumn c : resolved) {
            boolean compatible = TypeMapper.compatible(c.sourceType, c.source, c.target);
            String result = compatible ? "OK" : "WARNING/INCOMPATIBLE";
            if (!compatible) ok = false;
            System.out.printf("%-30s %-18s -> %-18s %s%n",
                    c.source.name(), c.sourceType, c.target.jdbcTypeName(), result);
            report.add(t.sourceTable(), t.targetTable(), c.source.name(), c.sourceType,
                    c.target.jdbcTypeName(), result, TypeMapper.expectedConversion(c.sourceType, c.target));
        }
        System.out.println("CHECK RESULT: " + (ok ? "OK" : "REVIEW REQUIRED"));
    }

    public long migrate(TableConfig t, List<ColumnMapping> mappings) throws Exception {
        long startedNanos = System.nanoTime();
        long rowsRead = 0;
        long rowsCommitted = 0;
        int pendingRows = 0;
        int batchesSinceCommit = 0;
        String status = "SUCCESS";
        String failure = "";

        System.out.printf("\n=== MIGRATE %s.%s -> %s.%s ===%n",
                t.sourceSchema(), t.sourceTable(), t.targetSchema(), t.targetTable());
        System.out.printf("Streaming mode: fetchSize=%d, insertBatchSize=%d, commitEveryBatches=%d%n",
                fetchSize, batchSize, commitEveryBatches);

        try {
            List<ColumnInfo> src = metadata.readColumns(sybase.connection(), t.sourceSchema(), t.sourceTable());
            List<ColumnInfo> dst = metadata.readColumns(oracle.connection(), t.targetSchema(), t.targetTable());
            List<ResolvedColumn> cols = resolve(src, dst, t.sourceTable(), mappings);

            for (ResolvedColumn c : cols) {
                if (!TypeMapper.compatible(c.sourceType, c.source, c.target)) {
                    throw new SQLException("Incompatible mapping: " + c.source.name() + " " + c.sourceType +
                            " -> " + c.target.jdbcTypeName());
                }
            }

            String select = buildSelect(t, cols);
            String insert = buildInsert(t, cols);

            try (PreparedStatement psSelect = sybase.connection().prepareStatement(
                         select, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
                 PreparedStatement psInsert = oracle.connection().prepareStatement(insert)) {

                // Forward-only result set: no key assumptions, no pagination, no skipped duplicate IDs.
                psSelect.setFetchSize(fetchSize);
                try (ResultSet rs = psSelect.executeQuery()) {
                    while (rs.next()) {
                        for (int i = 0; i < cols.size(); i++) {
                            ResolvedColumn c = cols.get(i);
                            ValueBinder.bind(rs, c.sourceIndex, psInsert, i + 1,
                                    c.source, c.target, c.sourceType);
                        }
                        psInsert.addBatch();
                        rowsRead++;
                        pendingRows++;

                        if (pendingRows >= batchSize) {
                            psInsert.executeBatch();
                            psInsert.clearBatch();
                            batchesSinceCommit++;
                            pendingRows = 0;

                            if (batchesSinceCommit >= commitEveryBatches) {
                                oracle.connection().commit();
                                rowsCommitted = rowsRead;
                                batchesSinceCommit = 0;
                                System.out.printf("  Progress %s: read=%d, committed=%d, elapsed=%s%n",
                                        t.sourceTable(), rowsRead, rowsCommitted, formatDuration(System.nanoTime() - startedNanos));
                            }
                        }
                    }
                }

                if (pendingRows > 0) {
                    psInsert.executeBatch();
                    psInsert.clearBatch();
                    pendingRows = 0;
                    batchesSinceCommit++;
                }
                if (batchesSinceCommit > 0) {
                    oracle.connection().commit();
                    rowsCommitted = rowsRead;
                }
            }

            System.out.printf("TABLE DONE: %s | rows=%d | duration=%s%n",
                    t.sourceTable(), rowsCommitted, formatDuration(System.nanoTime() - startedNanos));
            return rowsCommitted;
        } catch (Exception e) {
            status = "ERROR";
            failure = e.getClass().getSimpleName() + ": " + e.getMessage();
            try { oracle.connection().rollback(); } catch (Exception rollbackError) {
                System.err.println("Rollback failed: " + rollbackError.getMessage());
            }
            System.err.printf("TABLE FAILED: %s | read=%d | committed-before-error=%d | duration=%s | %s%n",
                    t.sourceTable(), rowsRead, rowsCommitted, formatDuration(System.nanoTime() - startedNanos), failure);
            throw e;
        } finally {
            long elapsed = System.nanoTime() - startedNanos;
            report.addMigrationSummary(t.sourceSchema() + "." + t.sourceTable(),
                    t.targetSchema() + "." + t.targetTable(), rowsCommitted, rowsRead,
                    formatDuration(elapsed), status, failure);
        }
    }

    private String buildSelect(TableConfig t, List<ResolvedColumn> cols) {
        String columnList = cols.stream().map(c -> q(c.source.name())).collect(Collectors.joining(", "));
        return "SELECT " + columnList + " FROM " + q(t.sourceSchema()) + "." + q(t.sourceTable());
    }

    private static String formatDuration(long nanos) {
        long millis = nanos / 1_000_000L;
        long hours = millis / 3_600_000L;
        long minutes = (millis % 3_600_000L) / 60_000L;
        long seconds = (millis % 60_000L) / 1_000L;
        long ms = millis % 1_000L;
        return String.format(Locale.ROOT, "%02d:%02d:%02d.%03d", hours, minutes, seconds, ms);
    }

    private String buildInsert(TableConfig t, List<ResolvedColumn> cols) {
        String names = cols.stream().map(c -> q(c.target.name())).collect(Collectors.joining(", "));
        String params = String.join(", ", Collections.nCopies(cols.size(), "?"));
        return "INSERT INTO " + q(t.targetSchema()) + "." + q(t.targetTable()) +
                " (" + names + ") VALUES (" + params + ")";
    }

    private List<ResolvedColumn> resolve(List<ColumnInfo> src, List<ColumnInfo> dst,
                                         String sourceTable, List<ColumnMapping> mappings) throws SQLException {
        List<ResolvedColumn> result = new ArrayList<>();
        for (ColumnInfo s : src) {
            ColumnMapping m = mappings.stream()
                    .filter(x -> x.sourceTable().equalsIgnoreCase(sourceTable)
                            && x.sourceColumn().equalsIgnoreCase(s.name()))
                    .findFirst().orElse(null);
            String targetName = m != null ? m.targetColumn() : s.name();
            ColumnInfo d = find(dst, targetName);
            if (d == null) throw new SQLException("Destination column not found for " + sourceTable + "." + s.name()
                    + " -> " + targetName);
            result.add(new ResolvedColumn(s, d, s.ordinal(),
                    TypeMapper.effectiveSourceType(s, m == null ? null : m.sourceTypeOverride())));
        }
        return result;
    }

    private String sourceTypeFor(ColumnInfo c, List<ColumnMapping> mappings, String table) {
        return mappings.stream()
                .filter(x -> x.sourceTable().equalsIgnoreCase(table) &&
                        x.sourceColumn().equalsIgnoreCase(c.name()))
                .map(ColumnMapping::sourceTypeOverride).filter(Objects::nonNull).findFirst()
                .orElse(TypeMapper.effectiveSourceType(c, null));
    }

    private ColumnInfo find(List<ColumnInfo> list, String name) {
        return list.stream().filter(x -> x.name().equalsIgnoreCase(name)).findFirst().orElse(null);
    }

    private boolean isKeysetType(String t) {
        return switch (t.toLowerCase()) {
            case "int", "smallint", "tinyint", "usmallint", "bigint", "numeric", "decimal" -> true;
            default -> false;
        };
    }

    private String q(String identifier) {
        return "\"" + identifier.replace("\"", "\"\"") + "\"";
    }

    private record ResolvedColumn(ColumnInfo source, ColumnInfo target, int sourceIndex, String sourceType) {}
}
