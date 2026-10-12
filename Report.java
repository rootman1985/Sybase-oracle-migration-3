package com.example.migrator;

import java.io.*;
import java.nio.file.*;
import java.time.*;
import java.util.*;

public final class Report {
    private final Path path;
    private final Path migrationSummaryPath;
    private final Path runSummaryPath;
    public Report(Path dir) throws IOException {
        Files.createDirectories(dir);
        path = dir.resolve("migration-report.csv");
        migrationSummaryPath = dir.resolve("migration-tables-summary.csv");
        runSummaryPath = dir.resolve("migration-run-summary.csv");
        if (!Files.exists(path)) Files.writeString(path,
                "timestamp,sourceTable,targetTable,column,sourceType,targetType,result,message\n");
        if (!Files.exists(migrationSummaryPath)) Files.writeString(migrationSummaryPath,
                "timestamp,sourceTable,targetTable,rowsCommitted,rowsRead,duration,status,message\n");
        if (!Files.exists(runSummaryPath)) Files.writeString(runSummaryPath,
                "timestamp,mode,tablesAttempted,tablesSucceeded,tablesFailed,rowsCommitted,totalDuration\n");
    }

    public synchronized void add(String sourceTable, String targetTable, String column,
                                  String sourceType, String targetType, String result, String message) {
        try {
            String line = String.join(",",
                    csv(Instant.now().toString()), csv(sourceTable), csv(targetTable), csv(column),
                    csv(sourceType), csv(targetType), csv(result), csv(message)) + "\n";
            Files.writeString(path, line, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException e) {
            System.err.println("Cannot write report: " + e.getMessage());
        }
    }


    public synchronized void addMigrationSummary(String sourceTable, String targetTable,
                                                 long rowsCommitted, long rowsRead,
                                                 String duration, String status, String message) {
        append(migrationSummaryPath, String.join(",", csv(Instant.now().toString()), csv(sourceTable),
                csv(targetTable), String.valueOf(rowsCommitted), String.valueOf(rowsRead),
                csv(duration), csv(status), csv(message)) + "\n");
    }

    public synchronized void addRunSummary(String mode, int tablesAttempted, int tablesSucceeded,
                                           int tablesFailed, long rowsCommitted, String duration) {
        append(runSummaryPath, String.join(",", csv(Instant.now().toString()), csv(mode),
                String.valueOf(tablesAttempted), String.valueOf(tablesSucceeded),
                String.valueOf(tablesFailed), String.valueOf(rowsCommitted), csv(duration)) + "\n");
    }

    private void append(Path destination, String line) {
        try {
            Files.writeString(destination, line, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException e) {
            System.err.println("Cannot write report " + destination + ": " + e.getMessage());
        }
    }

    private String csv(String s) {
        if (s == null) return "";
        return "\"" + s.replace("\"", "\"\"") + "\"";
    }
}
