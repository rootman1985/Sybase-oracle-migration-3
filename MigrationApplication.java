package com.example.migrator;

import java.nio.file.*;
import java.util.*;

public final class MigrationApplication {
    public static void main(String[] args) throws Exception {
        String mode = args.length == 0 ? "CHECK" : args[0].toUpperCase(Locale.ROOT);
        Path base = Paths.get(".");
        Config cfg = new Config(base.resolve("src/main/resources/application.properties"));
        List<TableConfig> tables = CsvConfigLoader.loadTables(base.resolve("src/main/resources/tables.csv"));
        List<ColumnMapping> mappings = CsvConfigLoader.loadMappings(base.resolve("src/main/resources/column-mappings.csv"));

        try (Db sybase = Db.open(cfg.get("sybase.driver"), cfg.get("sybase.url"),
                cfg.get("sybase.user"), cfg.get("sybase.password"));
             Db oracle = Db.open(cfg.get("oracle.driver"), cfg.get("oracle.url"),
                cfg.get("oracle.user"), cfg.get("oracle.password"))) {

            StateStore state = new StateStore(Paths.get(cfg.get("migration.stateDirectory", "state")));
            Report report = new Report(Paths.get(cfg.get("migration.logDirectory", "logs")));
            MigrationService service = new MigrationService(
                    sybase, oracle, new MetadataService(), state, report,
                    cfg.getInt("migration.batchSize", 1000),
                    cfg.getInt("migration.rangeSize", 10000),
                    cfg.getInt("migration.fetchSize", 1000),
                    cfg.getInt("migration.commitEveryBatches", 10)
            );

            long runStarted = System.nanoTime();
            int attempted = 0;
            int succeeded = 0;
            int failed = 0;
            long totalRowsCommitted = 0;

            for (TableConfig t : tables) {
                if (!t.enabled()) continue;
                attempted++;
                try {
                    if (mode.equals("CHECK")) {
                        service.check(t, mappings);
                    } else if (mode.equals("MIGRATE")) {
                        totalRowsCommitted += service.migrate(t, mappings);
                    } else {
                        throw new IllegalArgumentException("Use CHECK or MIGRATE");
                    }
                    succeeded++;
                } catch (Exception e) {
                    failed++;
                    System.err.println("ERROR " + t.sourceTable() + ": " + e.getMessage());
                    e.printStackTrace(System.err);
                    // Continue with the next table; any uncommitted Oracle work is rolled back.
                }
            }

            long runElapsed = System.nanoTime() - runStarted;
            String duration = formatDuration(runElapsed);
            System.out.printf("\n========== RUN SUMMARY (%s) ==========%n", mode);
            System.out.printf("Tables attempted: %d | succeeded: %d | failed: %d%n", attempted, succeeded, failed);
            if (mode.equals("MIGRATE")) System.out.printf("Rows committed in completed tables: %d%n", totalRowsCommitted);
            System.out.println("Total duration: " + duration);
            report.addRunSummary(mode, attempted, succeeded, failed, totalRowsCommitted, duration);
        }
    }
    private static String formatDuration(long nanos) {
        long millis = nanos / 1_000_000L;
        long hours = millis / 3_600_000L;
        long minutes = (millis % 3_600_000L) / 60_000L;
        long seconds = (millis % 60_000L) / 1_000L;
        long ms = millis % 1_000L;
        return String.format(Locale.ROOT, "%02d:%02d:%02d.%03d", hours, minutes, seconds, ms);
    }
}
