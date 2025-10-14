# TODO List - Go ETL Pipeline

## Completed Tasks
- ✅ Fix webhook integration with new APIServer architecture
- ✅ Resolve webhook HTTP method registration issues
- ✅ Fix route conflicts between static files and webhook endpoints
- ✅ Implement direct serving of dashboard.html

## Pending Features

### High Priority
- [ ] Implement basic scheduler for recurring pipeline execution (cron-like functionality)
- [ ] Create pipeline logs streaming endpoint and UI
- [ ] Implement resource management and concurrent execution limits

### Medium Priority
- [ ] Add pipeline template system with predefined configurations
- [ ] Enhance UI with filtering, search, and pagination
- [ ] Add statistics and monitoring dashboard
- [ ] Implement backup/restore functionality

### Low Priority
- [ ] Add comprehensive error handling and validation
- [ ] Create documentation and API reference
- [ ] Add tests for multi-pipeline management features

## Recent Changes (2025-10-14)

### Webhook Integration Fix
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
