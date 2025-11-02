# TODO List - Go ETL Pipeline

## Completed Tasks
- ✅ Fix webhook integration with new APIServer architecture
- ✅ Resolve webhook HTTP method registration issues
- ✅ Fix route conflicts between static files and webhook endpoints
- ✅ Implement direct serving of dashboard.html

## Pending Features

### High Priority
- [x] Implement basic scheduler for recurring pipeline execution (cron-like functionality)
  - ✅ Created `steps/cron.go` with cron trigger implementation
  - ✅ Supports `@every <duration>` format (e.g., `@every 10s`, `@every 5m`)
  - ✅ Multiple independent cron triggers can run in same pipeline
  - ✅ Example pipelines in `examples/cron_pipeline.yml` and `examples/cron_advanced.yml`
- [ ] Create pipeline logs streaming endpoint and UI
- [ ] Implement resource management and concurrent execution limits

### Medium Priority
- [ ] Add pipeline template system with predefined configurations
- [ ] Enhance UI with filtering, search, and pagination
- [ ] Add statistics and monitoring dashboard
- [ ] Implement backup/restore functionality
- [ ] Add step retry logic with exponential backoff
- [ ] Implement circuit breaker pattern for resilience
- [ ] Create dead letter queue for failed executions

### Low Priority
- [ ] Add comprehensive error handling and validation
- [ ] Create documentation and API reference
- [ ] Add tests for multi-pipeline management features
- [ ] Implement pipeline dry-run mode for testing
- [ ] Add step mocking capabilities for unit testing
- [ ] Create validation schema for pipeline configurations
- [ ] Implement authentication and authorization system
- [ ] Add multi-tenancy support with pipeline isolation
- [ ] Create RBAC (Role-Based Access Control) system

## Future Advanced Features

### Data Transformation Steps
- [ ] JSON Transform step (jq-like operations)
- [ ] CSV Reader/Writer steps
- [ ] Excel Reader/Writer steps
- [ ] HTTP Client step with retry and auth
- [ ] Database connector steps (MySQL, PostgreSQL, MongoDB)
- [ ] Data validation and sanitization steps

### System Integrations
- [ ] Email notification step (SMTP)
- [ ] Slack/Teams notification steps
- [ ] S3/Cloud storage integration steps
- [ ] Message queue integration (RabbitMQ, Kafka)
- [ ] GraphQL client step

### Advanced Pipeline Features
- [ ] Pipeline versioning system
- [ ] Pipeline import/export with dependencies
- [ ] Conditional pipeline execution based on schedules
- [ ] Pipeline composition and sub-pipelines
- [ ] Dynamic pipeline generation from templates

## Recent Changes

### Cron Scheduler Implementation (2025-11-02)

- **File**: `steps/cron.go`
  - Implemented `CronStep` as a trigger type
  - Supports `@every <duration>` syntax (e.g., `@every 10s`, `@every 1h30m`)
  - Supports simple duration format (e.g., `5m`, `1h`, `30s`)
  - Uses Go's `time.Ticker` for reliable scheduling
  - Provides timestamp and schedule info to triggered pipelines
  - Graceful shutdown support with `Stop()` method

- **Examples Created**:
  - `examples/cron_pipeline.yml`: Simple example with 10-second interval
  - `examples/cron_advanced.yml`: Complex example with multiple cron triggers (health check + backup)

- **Architecture Notes**:
  - Follows same Trigger interface pattern as webhook step
  - Integrates seamlessly with existing pipeline trigger system
  - Multiple cron triggers can coexist in same pipeline
  - Each trigger fires independently and creates new pipeline execution

### Webhook Refactoring - Lazy HTTP Registration (2025-11-02)
- **File**: `steps/webhook.go`
  - **BREAKING CHANGE**: Moved HTTP endpoint registration from `init()` to `SetOnTrigger()`
  - Endpoints are now registered only when pipeline actually runs
  - Benefits:
    - No memory leak if pipeline is loaded but never executed
    - No conflicts between multiple pipelines with same webhook path
    - Consistent pattern with cron trigger (setup in SetOnTrigger)
    - Resources allocated only when needed
  - Added `path` field to `WebhookStep` struct
  - HTTP handler registration happens when pipeline enters trigger mode

### Webhook Integration Fix (2025-10-14)
- **File**: `steps/webhook.go`
  - Added `.Methods(strings.ToUpper(method))` to route registration
  - Removed redundant manual method checking
  - Cleaned up debug code and unused imports

- **File**: `web/site.go`
  - Changed `/dashboard` to serve `dashboard.html` directly with `http.ServeFile`
  - Moved static files from `PathPrefix("/")` to `PathPrefix("/static/")`
  - Added `http.StripPrefix` for proper static file serving

- **Testing**: Webhook endpoints now fully functional with proper HTTP method matching

## Architecture Notes

### Current State
- APIServer uses Gorilla Mux router for all HTTP routing
- Webhook steps register directly on APIServer via WebhookRegistry singleton
- Static files served under `/static/` prefix
- Dashboard available at `/dashboard` endpoint
- Pipeline triggers remain active after execution for recurring triggers

### Key Components
- `web/api_server.go`: Main API server with Gorilla Mux router
- `web/webhook_registry.go`: Singleton providing webhook steps access to router
- `steps/webhook.go`: Webhook step implementation with trigger support
- `pipeline/pipeline.go`: Pipeline orchestration with trigger support
- `db/pipeline_state_manager.go`: SQLite-based pipeline state persistence
