# ETL Pipeline Examples

This directory contains example pipeline configurations demonstrating various features of the Go ETL system.

## Running Examples

To run any example, use:
```bash
./etl -file examples/<example_file>.yml -log info
```

## Cron Trigger Examples

### cron_pipeline.yml
A simple example showing a cron trigger that fires every 10 seconds and prints the timestamp.

**Run:**
```bash
./etl -file examples/cron_pipeline.yml -log info
```

**Expected output:**
```
2025-11-02T15:54:30Z
2025-11-02T15:54:40Z
2025-11-02T15:54:50Z
...
```

### cron_advanced.yml
An advanced example with multiple independent cron triggers:
- Health check every 30 seconds
- Backup process every minute with 2-second delay simulation

**Run:**
```bash
./etl -file examples/cron_advanced.yml -log info
```

**Expected output:**
```
2025-11-02T15:53:05Z          # Health check
2025-11-02T15:53:35Z          # Health check + Backup start
2025-11-02T15:53:35Z
Backup completed successfully  # After 2s delay
```

## Features Demonstrated

- **Cron Triggers**: Time-based pipeline execution
- **Multiple Triggers**: Independent triggers in the same pipeline
- **Step Dependencies**: Steps that wait for trigger data
- **Data Flow**: Accessing trigger outputs in downstream steps
- **JavaScript Expressions**: Using `ctx.trigger_name.output` syntax

## See Also

- [Cron Trigger Documentation](../docs/cron_trigger.md)
- [Main README](../README.md)
