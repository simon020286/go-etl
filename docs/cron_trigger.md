# Cron Trigger

The Cron trigger allows you to schedule recurring pipeline executions using time-based schedules.

## Configuration

### Basic Usage

```yaml
steps:
  - name: my_schedule
    type: cron
    config:
      schedule: "@every 5m"  # Required: schedule expression
```

### Schedule Expression Formats

The cron trigger supports multiple schedule expression formats:

#### 1. @every Syntax (Recommended)
```yaml
schedule: "@every 10s"   # Every 10 seconds
schedule: "@every 5m"    # Every 5 minutes
schedule: "@every 1h"    # Every 1 hour
schedule: "@every 1h30m" # Every 1 hour and 30 minutes
schedule: "@every 24h"   # Every 24 hours (daily)
```

#### 2. Simple Duration Format
```yaml
schedule: "30s"  # Every 30 seconds
schedule: "5m"   # Every 5 minutes
schedule: "1h"   # Every 1 hour
```

### Supported Duration Units
- `s` - seconds
- `m` - minutes
- `h` - hours

You can combine units: `1h30m`, `2h15m30s`, etc.

## Trigger Data

When a cron trigger fires, it provides the following data outputs to the pipeline:

- `default`: The timestamp in ISO 8601 format (for easy access: `ctx.trigger_name.default`)
- `timestamp`: The timestamp in ISO 8601 format (same as default)
- `schedule`: The schedule expression that was used (e.g., "@every 5m")
- `unix`: The Unix timestamp (seconds since epoch) as an integer

You can access these values using JavaScript expressions in your pipeline steps:
- `ctx.trigger_name.default` or `ctx.trigger_name.timestamp` - ISO 8601 timestamp string
- `ctx.trigger_name.schedule` - The original schedule expression
- `ctx.trigger_name.unix` - Unix timestamp number

## Examples

### Simple Example: Health Check

```yaml
steps:
  - name: health_check_timer
    type: cron
    config:
      schedule: "@every 30s"

  - name: run_health_check
    type: stdout
    inputs: ["health_check_timer"]
    config:
      value: "ctx.health_check_timer.timestamp"
```

This will print the timestamp every 30 seconds, for example:
```
2025-11-02T15:54:30Z
2025-11-02T15:55:00Z
```

### Multiple Cron Triggers

You can have multiple independent cron triggers in the same pipeline:

```yaml
steps:
  # Health check every 30 seconds
  - name: health_check
    type: cron
    config:
      schedule: "@every 30s"

  - name: check_status
    type: stdout
    inputs: ["health_check"]
    config:
      value: "Health check at ctx.health_check.timestamp"

  # Backup every hour
  - name: backup_timer
    type: cron
    config:
      schedule: "@every 1h"

  - name: run_backup
    type: stdout
    inputs: ["backup_timer"]
    config:
      value: "Running backup at ctx.backup_timer.timestamp"

  - name: backup_delay
    type: delay
    inputs: ["run_backup"]
    config:
      duration: 2s

  - name: backup_complete
    type: stdout
    inputs: ["backup_delay"]
    config:
      value: "'Backup completed'"
```

Output example:
```
2025-11-02T15:53:05Z
2025-11-02T15:53:35Z
2025-11-02T15:53:35Z
Backup completed
```

## Use Cases

1. **Periodic Health Checks**: Monitor system health at regular intervals
2. **Scheduled Backups**: Run backup operations on a schedule
3. **Data Synchronization**: Periodically sync data between systems
4. **Report Generation**: Generate reports at scheduled times
5. **Cleanup Tasks**: Remove old data or temporary files periodically
6. **Polling**: Check for new data or events at regular intervals

## Architecture Notes

- Each cron trigger runs independently
- When a trigger fires, it creates a new pipeline execution
- The trigger continues running after pipeline completion
- Triggers are stopped when the main pipeline context is cancelled (e.g., Ctrl+C)
- Uses Go's `time.Ticker` for reliable, efficient scheduling

## Limitations

- Currently does not support traditional cron expressions (e.g., `0 */2 * * *`)
- All schedules are interval-based, not time-of-day based
- Minimum recommended interval is 1 second (shorter intervals may cause performance issues)

## Future Enhancements

Planned features for future releases:

- Full cron expression support (`* * * * *` format)
- Time-of-day scheduling
- Timezone support
- Missed execution handling
- Execution history and statistics
