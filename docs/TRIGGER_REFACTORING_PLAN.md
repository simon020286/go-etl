# Piano di Refactoring: Unificazione Trigger e Step

**Data:** 2025-11-08
**Stato:** Pianificato, non implementato
**Priorità:** Media
**Breaking Changes:** No (backward compatible)

---

## Executive Summary

Attualmente il sistema ha due flussi di esecuzione separati:
1. Pipeline **senza trigger**: esegue tutti gli step una volta tramite `Pipeline.Run()`
2. Pipeline **con trigger**: usa `Pipeline.RunFromTriggers()` che crea una **nuova istanza di Pipeline** ad ogni trigger fire

**Problema identificato:**
- Le notifiche websocket degli step non funzionano su pipeline con trigger perché la nuova istanza Pipeline creata non ha il callback `OnChange` impostato (risolto temporaneamente copiando `OnChange` alla nuova istanza)
- Impossibile usare trigger a metà pipeline (es. webhook di conferma utente)
- Ogni trigger fire crea overhead di una nuova istanza Pipeline

**Obiettivo:**
Eliminare la distinzione tra pipeline con/senza trigger, permettendo ai trigger di essere usati sia all'inizio che a metà pipeline, con una singola execution che "pausa" e "riprende" quando il trigger scatta.

---

## Analisi del Sistema Attuale

### 1. Architettura Corrente dei Trigger

#### Interfaccia Trigger
**File:** `core/trigger.go`

```go
type Trigger interface {
    Name() string
    SetOnTrigger(func(data map[string]*Data)) error
    Stop() error
}
```

**Caratteristiche:**
- I trigger implementano **sia** `core.Step` **che** `core.Trigger`
- Non vengono mai chiamati tramite `Step.Run()` nel flusso normale
- Usano `SetOnTrigger()` per registrare callback asincroni

#### Implementazioni Esistenti

**1. WebhookStep** (`steps/webhook.go`)
```go
type WebhookStep struct {
    name     string
    trigger  chan webhookResponse
    method   string
    path     string
    stopChan chan struct{}
}
```

- Comportamento attuale di `Run()` (linee 30-36):
  ```go
  func (s *WebhookStep) Run(ctx context.Context, state *core.PipelineState) (map[string]*core.Data, error) {
      <-s.trigger  // BLOCCA fino a HTTP request
      return core.CreateDefaultResultData("Webhook triggered"), nil
  }
  ```
- **IMPORTANTE:** `Run()` già blocca fino al primo trigger - perfetto per il nuovo design!
- Registra endpoint HTTP via `web.GetWebhookRegistry()`
- Supporta JSON, form data, query params, plain text

**2. CronStep** (`steps/cron.go`)
```go
type CronStep struct {
    name     string
    schedule string
    ticker   *time.Ticker
    stopChan chan struct{}
}
```

- Comportamento attuale di `Run()` (linee 22-24):
  ```go
  func (s *CronStep) Run(ctx context.Context, state *core.PipelineState) (map[string]*core.Data, error) {
      return core.CreateDefaultResultData("Cron triggered"), nil  // Ritorna subito!
  }
  ```
- **PROBLEMA:** `Run()` ritorna immediatamente - andrà modificato
- Supporta `@every <duration>` e durations semplici ("5m", "1h")
- Cron expressions complete non implementate

### 2. Flusso di Esecuzione Corrente

#### Pipeline senza Trigger
**File:** `pipeline/pipeline.go`, linee 78-153

```go
func (p *Pipeline) Run(ctx context.Context, logger *slog.Logger) error {
    if len(p.triggers) > 0 {
        logger.Info("Found", slog.Int("triggers", len(p.triggers)))
        p.RunFromTriggers(ctx)  // Devia al flusso trigger
        return nil
    }

    // Esecuzione normale con done channels
    done := make(map[string]chan struct{})

    // 1. Crea done channels per tutti gli step
    for _, step := range p.steps {
        done[step.Name()] = make(chan struct{})
    }

    // 2. Pre-popola channels per state esistente (es. trigger data)
    for stepName := range p.state.Results {
        if _, exists := done[stepName]; !exists {
            ch := make(chan struct{})
            close(ch)  // Già disponibile
            done[stepName] = ch
        }
    }

    // 3. Esegue step concorrentemente con dependency resolution
    exec := func(step core.Step) {
        // Attende tutte le dipendenze
        for _, input := range p.inputs[step.Name()] {
            <-done[stepName]  // BLOCCA

            // Verifica output disponibile
            if _, ok := p.state.Get(stepName, outputName); !ok {
                return  // Dependency fallita, salta step
            }
        }

        // Esegue step
        if p.OnChange != nil {
            p.OnChange(core.ChangeEvent{Type: core.ChangeEventTypeStart, StepName: step.Name()})
        }

        outputs, err := step.Run(ctx, p.state)
        p.state.Set(step.Name(), outputs)
        close(done[step.Name()])  // Sblocca downstream

        if p.OnChange != nil {
            p.OnChange(core.ChangeEvent{Type: core.ChangeEventTypeEnd, StepName: step.Name(), Data: outputs})
        }
    }

    for _, step := range p.steps {
        wg.Add(1)
        go exec(step)
    }

    wg.Wait()
    return nil
}
```

**Meccanismo done channels:**
- Ogni step ha un channel chiuso quando completa
- Step attendono dependencies tramite `<-done[stepName]`
- Se dependency manca output, step salta silenziosamente
- Execution concorrente con sync via channels

#### Pipeline con Trigger
**File:** `pipeline/pipeline.go`, linee 159-202

```go
func (p *Pipeline) RunFromTriggers(ctx context.Context) {
    for _, trigger := range p.triggers {
        t := trigger
        t.SetOnTrigger(func(data map[string]*core.Data) {
            // CREA NUOVA ISTANZA AD OGNI FIRE
            newP := Pipeline{
                steps:  p.steps,
                inputs: p.inputs,
                state: &core.PipelineState{
                    Results: map[string]map[string]*core.Data{
                        t.Name(): data,  // Pre-popola trigger data
                    },
                    Logger: p.state.Logger,
                },
                OnChange: p.OnChange,  // Fix temporaneo
            }

            go func() {
                err := newP.Run(context.Background(), p.state.Logger)
                // ... error handling
            }()
        })
    }

    <-ctx.Done()  // Aspetta cancellation

    for _, trigger := range p.triggers {
        trigger.Stop()
    }
}
```

**Problemi:**
- ❌ Nuova istanza Pipeline ad ogni fire (overhead)
- ❌ Context.Background() ignora parent context
- ❌ Nessun execution ID passato
- ❌ State isolato tra executions
- ❌ `OnChange` mancava (fix temporaneo applicato)

### 3. PipelineState e Contesto

#### Struttura State
**File:** `core/pipeline.go`

```go
type PipelineState struct {
    Results map[string]map[string]*Data  // [stepName][outputName]*Data
    mu      sync.RWMutex
    Logger  *slog.Logger
    // MANCA: ExecutionID, execution metadata
}
```

**Accesso State negli Step:**
- Interpolazione JavaScript via `ctx.stepname.outputname`
- Output singolo "default" diventa scalare: `ctx.stepname`
- Output multipli diventano oggetto: `ctx.stepname = {out1: val1, out2: val2}`

**File:** `core/interpolate_value.go`, linee 20-41

```go
func (iv *InterpolatedValue) Resolve(state *PipelineState) (any, error) {
    state.mu.RLock()
    defer state.mu.RUnlock()

    // Costruisce contesto JS
    ctx := make(map[string]any)
    for stepName, outputs := range state.Results {
        if len(outputs) == 1 {
            for _, data := range outputs {
                ctx[stepName] = data.Value  // Scalare
            }
        } else {
            stepOutputs := make(map[string]any)
            for outputName, data := range outputs {
                stepOutputs[outputName] = data.Value
            }
            ctx[stepName] = stepOutputs  // Oggetto
        }
    }

    // Valuta JS expression
    vm := goja.New()
    vm.Set("ctx", ctx)
    result, err := vm.RunString(iv.Expression)
    return result.Export(), err
}
```

#### Database Execution Tracking
**File:** `db/schema.go`, linee 61-72

```sql
CREATE TABLE IF NOT EXISTS executions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    pipeline_id INTEGER NOT NULL,
    status TEXT NOT NULL,
    started_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    completed_at DATETIME,
    duration_ms INTEGER,
    error_message TEXT,
    trigger_type TEXT,  -- manual, scheduled, webhook
    trigger_data TEXT,  -- JSON data from trigger
    FOREIGN KEY (pipeline_id) REFERENCES pipelines(id) ON DELETE CASCADE
)
```

**PROBLEMA:** Execution DB esiste ma è **disconnesso** dall'esecuzione runtime:
- `ExecutionManager` (`db/execution_manager.go`) serve solo per query storiche
- `PipelineStateManager.StartPipeline()` crea execution ma non passa ID alla pipeline
- Gli step non hanno accesso all'execution ID

**File:** `db/pipeline_state_manager.go`, linee 134-260

```go
func (psm *PipelineStateManager) StartPipeline(pipelineID int, triggerType, triggerData string) (*Execution, error) {
    // ... validation ...

    // Crea execution record
    execution, err := psm.createExecution(pipelineID, triggerType, triggerData)

    // Carica pipeline
    pipelineInstance, err := pipeline.LoadPipelineFromYAML(pipelineRecord.ConfigYAML)

    // Imposta OnChange callback
    pipelineInstance.OnChange = func(event core.ChangeEvent) {
        psm.logExecutionEvent(execution.ID, event)
        psm.emitStepEvent(StepExecutionEvent{
            ExecutionID: execution.ID,  // ID disponibile qui ma non nella pipeline!
            // ...
        })
    }

    // Esegue pipeline (EXECUTION ID NON PASSATO)
    go psm.executePipeline(runningPipeline, pipelineRecord)

    return execution, nil
}
```

### 4. Requisiti dell'Utente

**Conversazione con l'utente (2025-11-08):**

**Q1: Trigger a metà pipeline - comportamento?**
> A: "Una sola esecuzione. Se la pipeline parte senza trigger, il trigger in mezzo verrà eseguito una sola volta, poi viene 'dismesso', quindi non ci sarà nessun fork."

**Q2: Multiple trigger fires a metà pipeline?**
> A: "Non dovrebbe succedere, come detto prima il trigger in mezzo una volta eseguito viene dismesso."

**Q3: Gestione dello State?**
> A: "Credo che valga sempre la risposta sopra" (stato singolo, non fork)

**Q4: Trigger multipli (cron + webhook)?**
> A: "In questo caso credo che dovrebbe esserci un execution id diverso per ogni scatto del trigger iniziale."

**Q5: Backward compatibility?**
> A: "Se si riuscisse mi piacerebbe che abbiano un comportamento unificato, così che possano essere messi sia all'inizio che in mezzo alla pipeline."

**Q6: Execution tracking?**
> A: "È una singola execution che parte e riprende. L'execution id dovrebbe essere disponibile in tutti gli step della pipeline, è utile ad esempio per creare un url univoco per i webhook in mezzo alla pipeline."

**Esempio d'uso desiderato:**
```yaml
steps:
  - name: daily_trigger
    type: cron
    config:
      schedule: "@every 24h"

  - name: fetch_data
    type: http
    inputs: [daily_trigger]

  - name: manual_approval
    type: webhook
    inputs: [fetch_data]
    config:
      path: "approve/{{ctx._execution.id}}"  # Path univoco per execution!

  - name: send_email
    type: email
    inputs: [manual_approval]
```

**Comportamento atteso:**
1. Cron scatta → crea execution #123
2. Esegue `fetch_data` con execution_id=123
3. Webhook registrato a `/webhook/approve/123`
4. Pipeline **pausa** in attesa del webhook
5. Webhook ricevuto → **stessa execution #123** continua
6. Esegue `send_email` con execution_id=123
7. Execution #123 completata

---

## Piano di Implementazione

### Fase 1: Aggiungere ExecutionID al Contesto

#### 1.1 Modificare PipelineState
**File:** `core/pipeline.go`

```go
type PipelineState struct {
    Results     map[string]map[string]*Data
    mu          sync.RWMutex
    Logger      *slog.Logger
    ExecutionID *int  // NUOVO: ID execution corrente (opzionale per backward compat)
}
```

#### 1.2 Esporre ExecutionID nell'Interpolazione
**File:** `core/interpolate_value.go`

Modificare `Resolve()` per aggiungere metadati:

```go
func (iv *InterpolatedValue) Resolve(state *PipelineState) (any, error) {
    state.mu.RLock()
    defer state.mu.RUnlock()

    // Contesto step esistente
    ctx := make(map[string]any)
    for stepName, outputs := range state.Results {
        // ... codice esistente ...
    }

    // NUOVO: Aggiungi metadati execution
    if state.ExecutionID != nil {
        ctx["_execution"] = map[string]any{
            "id": *state.ExecutionID,
        }
    }

    // Valuta JS
    vm := goja.New()
    vm.Set("ctx", ctx)
    result, err := vm.RunString(iv.Expression)
    return result.Export(), err
}
```

**Uso negli step:**
```yaml
config:
  path: "approve/{{ctx._execution.id}}"
  message: "Processing execution {{ctx._execution.id}}"
```

#### 1.3 Passare ExecutionID da PipelineStateManager
**File:** `db/pipeline_state_manager.go`

Modificare `StartPipeline()` (circa linea 196):

```go
ctx, cancel := context.WithCancel(context.Background())
runningPipeline := &RunningPipeline{
    ID:         pipelineID,
    Pipeline:   pipelineInstance,
    Execution:  execution,
    Context:    ctx,
    CancelFunc: cancel,
    StartTime:  time.Now(),
    Status:     StateRunning,
}

// NUOVO: Imposta execution ID nello state
pipelineInstance.SetState(&core.PipelineState{
    Results:     make(map[string]map[string]*core.Data),
    Logger:      slog.Default(),
    ExecutionID: &execution.ID,  // Passa execution ID
})

// Imposta OnChange callback
pipelineInstance.OnChange = func(event core.ChangeEvent) {
    // ... codice esistente ...
}
```

**Test:** Dopo questa fase, `ctx._execution.id` sarà accessibile in tutti gli step

---

### Fase 2: Modificare Comportamento Trigger per One-Shot Mode

#### 2.1 WebhookStep - Supporto Path Dinamico
**File:** `steps/webhook.go`

**Problema attuale:** Path è statico, definito in `NewWebhookStep()`

**Soluzione:** Rendere path un `InterpolatedValue` risolto a runtime

```go
type WebhookStep struct {
    name     string
    trigger  chan webhookResponse
    method   string
    pathTemplate *core.InterpolatedValue  // MODIFICATO: da string a InterpolatedValue
    resolvedPath string                   // Path risolto
    stopChan chan struct{}
    registry *web.WebhookRegistry
}

func NewWebhookStep(name string, config map[string]any) (*WebhookStep, error) {
    method := "POST"
    if m, ok := config["method"].(string); ok {
        method = strings.ToUpper(m)
    }

    path, ok := config["path"].(string)
    if !ok {
        return nil, fmt.Errorf("webhook step requires 'path' in config")
    }

    return &WebhookStep{
        name:         name,
        method:       method,
        pathTemplate: &core.InterpolatedValue{Expression: path},  // Salva template
        trigger:      make(chan webhookResponse, 1),
        stopChan:     make(chan struct{}),
        registry:     web.GetWebhookRegistry(),
    }, nil
}

func (s *WebhookStep) Run(ctx context.Context, state *core.PipelineState) (map[string]*core.Data, error) {
    // NUOVO: Risolvi path template con execution ID
    pathValue, err := s.pathTemplate.Resolve(state)
    if err != nil {
        return nil, fmt.Errorf("failed to resolve webhook path: %w", err)
    }
    s.resolvedPath = fmt.Sprintf("%v", pathValue)

    // Registra handler HTTP (ora con path dinamico)
    s.registry.RegisterWebhook(s.resolvedPath, s.method, func(w http.ResponseWriter, r *http.Request) {
        // ... parsing esistente ...

        select {
        case s.trigger <- webhookResponse{Value: data}:
            w.WriteHeader(http.StatusOK)
        case <-s.stopChan:
            w.WriteHeader(http.StatusServiceUnavailable)
        }
    })

    // Attende primo trigger (comportamento esistente, già one-shot!)
    response := <-s.trigger

    // NUOVO: Deregistra webhook dopo uso (one-shot)
    s.registry.UnregisterWebhook(s.resolvedPath, s.method)

    return core.CreateDefaultResultData(response.Value), nil
}

func (s *WebhookStep) Stop() error {
    close(s.stopChan)
    if s.resolvedPath != "" {
        s.registry.UnregisterWebhook(s.resolvedPath, s.method)
    }
    return nil
}
```

**Nota:** `SetOnTrigger()` rimane per backward compatibility (modalità continuous)

#### 2.2 CronStep - One-Shot Mode
**File:** `steps/cron.go`

**Problema:** `Run()` ritorna immediatamente invece di bloccare

**Soluzione:** Bloccare sul primo tick, poi ritornare

```go
func (s *CronStep) Run(ctx context.Context, state *core.PipelineState) (map[string]*core.Data, error) {
    // Parse schedule (codice esistente)
    var duration time.Duration
    if strings.HasPrefix(s.schedule, "@every ") {
        d, err := time.ParseDuration(strings.TrimPrefix(s.schedule, "@every "))
        if err != nil {
            return nil, fmt.Errorf("invalid cron schedule: %w", err)
        }
        duration = d
    } else {
        d, err := time.ParseDuration(s.schedule)
        if err != nil {
            return nil, fmt.Errorf("invalid duration: %w", err)
        }
        duration = d
    }

    // MODIFICATO: Crea ticker e attende PRIMO tick
    ticker := time.NewTicker(duration)
    defer ticker.Stop()

    select {
    case t := <-ticker.C:
        // Primo tick ricevuto - genera data
        timestampStr := t.Format(time.RFC3339)
        return map[string]*core.Data{
            "default":   {Value: timestampStr},
            "timestamp": {Value: timestampStr},
            "schedule":  {Value: s.schedule},
            "unix":      {Value: t.Unix()},
        }, nil
    case <-ctx.Done():
        return nil, ctx.Err()
    }
}
```

**Nota:** `SetOnTrigger()` implementa modalità continuous (per trigger root)

---

### Fase 3: Unificare Pipeline.Run()

#### 3.1 Identificare Trigger "Root" vs "Mid-Pipeline"
**File:** `pipeline/pipeline.go`

```go
// isRootTrigger verifica se un trigger è all'inizio della pipeline (nessuna dependency)
func (p *Pipeline) isRootTrigger(triggerName string) bool {
    inputs := p.inputs[triggerName]
    return len(inputs) == 0
}

// getRootTriggers ritorna tutti i trigger senza dependencies
func (p *Pipeline) getRootTriggers() []core.Trigger {
    roots := []core.Trigger{}
    for name, trigger := range p.triggers {
        if p.isRootTrigger(name) {
            roots = append(roots, trigger)
        }
    }
    return roots
}
```

#### 3.2 Modificare Pipeline.Run() - Dispatch Logic
**File:** `pipeline/pipeline.go`

```go
func (p *Pipeline) Run(ctx context.Context, logger *slog.Logger) error {
    if p.state == nil {
        p.state = &core.PipelineState{
            Results: make(map[string]map[string]*core.Data),
            Logger:  logger,
            // ExecutionID settato esternamente da PipelineStateManager
        }
    }

    // Identifica trigger root (senza input)
    rootTriggers := p.getRootTriggers()

    if len(rootTriggers) > 0 {
        // Modalità CONTINUOUS: trigger root creano multiple executions
        logger.Info("Running in continuous mode", slog.Int("root_triggers", len(rootTriggers)))
        return p.runContinuous(ctx, logger, rootTriggers)
    }

    // Modalità ONE-SHOT: esecuzione normale (trigger inclusi come step)
    logger.Info("Running in one-shot mode", slog.Int("steps", len(p.steps)), slog.Int("mid_triggers", len(p.triggers)))
    return p.runOnce(ctx, logger)
}
```

#### 3.3 Metodo runOnce() - Esecuzione Unificata
**File:** `pipeline/pipeline.go`

```go
func (p *Pipeline) runOnce(ctx context.Context, logger *slog.Logger) error {
    done := make(map[string]chan struct{})
    var wg sync.WaitGroup
    mu := sync.Mutex{}

    // IMPORTANTE: Considera trigger come step normali
    allSteps := make(map[string]core.Step)
    for name, step := range p.steps {
        allSteps[name] = step
    }
    // Aggiungi trigger come step
    for name, trigger := range p.triggers {
        allSteps[name] = trigger.(core.Step)  // Trigger implementa Step interface
    }

    // Crea done channels per tutti (step + trigger)
    for name := range allSteps {
        done[name] = make(chan struct{})
    }

    // Pre-popola channels per state esistente (es. trigger data da continuous mode)
    for stepName := range p.state.Results {
        if _, exists := done[stepName]; !exists {
            ch := make(chan struct{})
            close(ch)
            done[stepName] = ch
        }
    }

    // Esecutore step (uguale al codice esistente)
    exec := func(step core.Step) {
        defer wg.Done()

        // Attende dependencies
        for _, input := range p.inputs[step.Name()] {
            parts := strings.Split(input, ":")
            stepName := parts[0]
            outputName := "default"
            if len(parts) == 2 {
                outputName = parts[1]
            }
            <-done[stepName]

            mu.Lock()
            if _, ok := p.state.Get(stepName, outputName); !ok {
                mu.Unlock()
                logger.Warn("Step dependency not satisfied",
                    slog.String("step", step.Name()),
                    slog.String("dependency", stepName))
                return
            }
            mu.Unlock()
        }

        logger.Debug("Running step", slog.String("step", step.Name()))

        if p.OnChange != nil {
            p.OnChange(core.ChangeEvent{Type: core.ChangeEventTypeStart, StepName: step.Name()})
        }

        // ESEGUE STEP (trigger bloccano qui fino a fire se mid-pipeline)
        outputs, err := step.Run(ctx, p.state)
        if err != nil {
            logger.Error("Step failed", slog.String("step", step.Name()), slog.Any("error", err))
            return
        }

        p.state.Set(step.Name(), outputs)
        close(done[step.Name()])

        logger.Debug("Step completed", slog.String("step", step.Name()), slog.Any("output", outputs))

        if p.OnChange != nil {
            p.OnChange(core.ChangeEvent{Type: core.ChangeEventTypeEnd, StepName: step.Name(), Data: outputs})
        }
    }

    // Lancia tutti gli step (inclusi trigger mid-pipeline) concorrentemente
    for _, step := range allSteps {
        wg.Add(1)
        go exec(step)
    }

    wg.Wait()
    return nil
}
```

**Chiave:** Trigger mid-pipeline vengono eseguiti come step normali. Il loro `Run()` blocca fino al primo fire, poi ritorna. Questo è **già il comportamento di WebhookStep.Run()**!

#### 3.4 Metodo runContinuous() - Trigger Root
**File:** `pipeline/pipeline.go`

```go
func (p *Pipeline) runContinuous(ctx context.Context, logger *slog.Logger, rootTriggers []core.Trigger) error {
    // NOTA: Questo metodo viene chiamato solo quando ci sono trigger root (senza input)
    // Ogni trigger fire dovrebbe creare una NUOVA EXECUTION nel DB
    // Questo richiede callback verso PipelineStateManager

    // Per ora, manteniamo comportamento simile a RunFromTriggers esistente
    // ma richiameremo runOnce invece di creare nuova Pipeline

    for _, trigger := range rootTriggers {
        t := trigger
        logger.Info("Setting up continuous trigger", slog.String("trigger", t.Name()))

        t.SetOnTrigger(func(data map[string]*core.Data) {
            logger.Info("Trigger fired", slog.String("trigger", t.Name()))

            // Invece di creare nuova Pipeline, riusa questa con nuovo state
            // PROBLEMA: ExecutionID deve essere nuovo per ogni fire
            // SOLUZIONE: Callback verso PipelineStateManager per creare nuova execution

            // Per ora, codice placeholder:
            // TODO: Implementare callback per creare nuova execution
            newState := &core.PipelineState{
                Results: map[string]map[string]*core.Data{
                    t.Name(): data,
                },
                Logger: logger,
                // ExecutionID: nuovo ID creato da PipelineStateManager
            }

            // Salva state originale
            origState := p.state
            p.state = newState

            go func() {
                err := p.runOnce(ctx, logger)
                if err != nil {
                    logger.Error("Trigger execution failed",
                        slog.String("trigger", t.Name()),
                        slog.Any("error", err))
                }

                // Ripristina state originale
                p.state = origState
            }()
        })
    }

    logger.Info("Waiting for triggers", slog.Int("count", len(rootTriggers)))
    <-ctx.Done()
    logger.Info("Pipeline cancelled, stopping triggers")

    for _, trigger := range rootTriggers {
        if err := trigger.Stop(); err != nil {
            logger.Error("Failed to stop trigger",
                slog.String("trigger", trigger.Name()),
                slog.Any("error", err))
        }
    }

    return nil
}
```

**NOTA IMPORTANTE:** `runContinuous()` ha ancora il problema di creare execution ID per ogni fire. Questo richiede integrazione più profonda con `PipelineStateManager` - vedi Fase 4.

#### 3.5 Rimuovere RunFromTriggers()
**File:** `pipeline/pipeline.go`

```go
// ELIMINARE completamente il metodo RunFromTriggers() (linee 159-202)
```

---

### Fase 4: Integrare Continuous Mode con Execution Manager

**PROBLEMA:** In `runContinuous()`, ogni trigger fire dovrebbe creare una nuova execution nel DB, ma `Pipeline` non ha accesso a `PipelineStateManager`.

**OPZIONI:**

#### Opzione A: Callback per Nuove Execution
Passare callback a `Pipeline` per creare nuove execution:

```go
type Pipeline struct {
    steps    map[string]core.Step
    triggers map[string]core.Trigger
    inputs   map[string][]string
    state    *core.PipelineState
    OnChange func(event core.ChangeEvent)
    OnTriggerFire func(triggerName string, data map[string]*core.Data) (*core.PipelineState, error)  // NUOVO
}
```

In `PipelineStateManager.StartPipeline()`:

```go
pipelineInstance.OnTriggerFire = func(triggerName string, data map[string]*core.Data) (*core.PipelineState, error) {
    // Crea nuova execution
    newExecution, err := psm.createExecution(pipelineID, triggerName, "")
    if err != nil {
        return nil, err
    }

    // Crea nuovo state con execution ID
    newState := &core.PipelineState{
        Results: map[string]map[string]*core.Data{
            triggerName: data,
        },
        Logger:      slog.Default(),
        ExecutionID: &newExecution.ID,
    }

    return newState, nil
}
```

In `Pipeline.runContinuous()`:

```go
t.SetOnTrigger(func(data map[string]*core.Data) {
    // Chiama callback per creare execution
    newState, err := p.OnTriggerFire(t.Name(), data)
    if err != nil {
        logger.Error("Failed to create execution", slog.Any("error", err))
        return
    }

    origState := p.state
    p.state = newState

    go func() {
        err := p.runOnce(ctx, logger)
        // ... error handling ...
        p.state = origState
    }()
})
```

#### Opzione B: Pipeline Manager Layer
Creare layer intermedio che gestisce lifecycle:

```go
// In pipeline/manager.go (nuovo file)
type PipelineManager struct {
    pipeline *Pipeline
    executionFactory func(triggerName string) (*core.PipelineState, error)
}

func (pm *PipelineManager) Run(ctx context.Context, logger *slog.Logger) error {
    // Gestisce continuous mode con execution factory
}
```

**RACCOMANDAZIONE:** Opzione A è più semplice e minimale.

---

### Fase 5: Testing e Validazione

#### 5.1 Test Cases da Creare

**Test 1: Trigger Mid-Pipeline One-Shot**
```yaml
steps:
  - name: start
    type: stdout
    config:
      value: "Starting"

  - name: approval
    type: webhook
    inputs: [start]
    config:
      path: "approve/{{ctx._execution.id}}"

  - name: finish
    type: stdout
    inputs: [approval]
    config:
      value: "Approved: {{ctx.approval.Value}}"
```

Verificare:
- Execution ID disponibile in webhook path
- Pipeline pausa al webhook
- Dopo POST a `/webhook/approve/{id}`, pipeline continua
- Stessa execution ID per tutti gli step

**Test 2: Trigger Root Continuous**
```yaml
steps:
  - name: timer
    type: cron
    config:
      schedule: "@every 5s"

  - name: log
    type: stdout
    inputs: [timer]
    config:
      value: "Tick at {{ctx.timer.timestamp}}"
```

Verificare:
- Ogni tick crea nuova execution
- Execution ID diversi per ogni fire
- WebSocket notifiche funzionano

**Test 3: Mixed Trigger Types**
```yaml
steps:
  - name: daily
    type: cron
    config:
      schedule: "@every 24h"

  - name: fetch
    type: http
    inputs: [daily]

  - name: approve
    type: webhook
    inputs: [fetch]
    config:
      path: "approve/{{ctx._execution.id}}"

  - name: send
    type: stdout
    inputs: [approve]
```

Verificare:
- Cron root crea executions
- Webhook mid-pipeline usa same execution
- ExecutionID disponibile ovunque

#### 5.2 Test Esistenti da Verificare
- `tests/` - tutti i test esistenti devono passare
- Verificare backward compatibility pipeline senza trigger
- Verificare esempi: `examples/webhook/`, `examples/cron/`

#### 5.3 Integration Tests
- WebSocket notifications su trigger continuous
- WebSocket notifications su trigger mid-pipeline
- DB execution tracking corretto
- Execution logs corretti

---

## Checklist Implementazione

### Fase 1: ExecutionID Context ✓
- [ ] Aggiungere `ExecutionID *int` a `core.PipelineState`
- [ ] Modificare `core/interpolate_value.go` per esporre `ctx._execution.id`
- [ ] Modificare `db/pipeline_state_manager.go` per passare execution ID
- [ ] Test: verificare `ctx._execution.id` accessibile

### Fase 2: Trigger One-Shot Mode ✓
- [ ] `steps/webhook.go`:
  - [ ] Rendere `path` un `InterpolatedValue`
  - [ ] Risolvere path a runtime in `Run()`
  - [ ] Deregistrare webhook dopo primo fire
  - [ ] Test: webhook con path dinamico
- [ ] `steps/cron.go`:
  - [ ] Modificare `Run()` per bloccare su primo tick
  - [ ] Stop ticker dopo primo tick
  - [ ] Test: cron one-shot mode

### Fase 3: Unificare Pipeline.Run() ✓
- [ ] Aggiungere `isRootTrigger()` e `getRootTriggers()`
- [ ] Modificare `Run()` per dispatch continuous vs one-shot
- [ ] Implementare `runOnce()`:
  - [ ] Includere trigger come step normali
  - [ ] Eseguire con dependency resolution
- [ ] Implementare `runContinuous()` (versione base)
- [ ] Rimuovere `RunFromTriggers()`
- [ ] Test: pipeline con trigger mid-pipeline

### Fase 4: Execution Manager Integration ✓
- [ ] Aggiungere `OnTriggerFire` callback a `Pipeline`
- [ ] Implementare callback in `PipelineStateManager.StartPipeline()`
- [ ] Usare callback in `runContinuous()`
- [ ] Test: execution ID univoci per trigger continuous

### Fase 5: Testing ✓
- [ ] Creare test trigger mid-pipeline
- [ ] Creare test trigger root continuous
- [ ] Creare test mixed triggers
- [ ] Verificare test esistenti passano
- [ ] Verificare esempi funzionano
- [ ] Test integration WebSocket
- [ ] Test integration DB

### Documentazione ✓
- [ ] Aggiornare `CLAUDE.md` con nuova architettura
- [ ] Documentare uso `ctx._execution.id`
- [ ] Esempi trigger mid-pipeline
- [ ] Migration guide per utenti esistenti

---

## Rischi e Mitigazioni

### Rischio 1: Breaking Changes Imprevisti
**Mitigazione:**
- Test completi su esempi esistenti
- Mantenere `SetOnTrigger()` per backward compat
- Feature flag per nuovo comportamento (opzionale)

### Rischio 2: Race Conditions in Continuous Mode
**Problema:** Multiple executions condividono stessa istanza Pipeline

**Mitigazione:**
- Ogni execution ha il proprio `PipelineState`
- State swap atomico in `runContinuous()`
- Test concurrency con trigger rapidi

### Rischio 3: Webhook Registry Conflicts
**Problema:** Path dinamici potrebbero collidere

**Mitigazione:**
- WebhookRegistry deve supportare path parametrizzati
- Verificare unicità execution ID nel path
- Cleanup automatico webhook one-shot

### Rischio 4: Context Cancellation
**Problema:** Context parent ignorato in goroutines

**Mitigazione:**
- Passare context corretto a `runOnce()`
- Propagare cancellation a trigger
- Cleanup risorse on cancel

---

## Performance Considerations

### Memory
**Prima:** Nuova Pipeline + State ad ogni trigger fire
**Dopo:** Riuso Pipeline, solo nuovo State

**Impatto:** -60% memory per execution (stima)

### Goroutines
**Prima:** Una goroutine per trigger listener + una per execution
**Dopo:** Una goroutine per execution

**Impatto:** Simile, nessun cambiamento significativo

### Database
**Prima:** INSERT execution per trigger fire
**Dopo:** Stesso comportamento

**Impatto:** Nessuno

---

## Alternative Considerate

### Alternativa 1: Trigger come Plugin System
Implementare trigger come external plugins con protocollo dedicato.

**Pro:** Massima flessibilità
**Contro:** Complessità eccessiva, performance overhead
**Decisione:** Scartata - overkill per use case

### Alternativa 2: Event Bus Architecture
Usare event bus interno per coordinare trigger/step.

**Pro:** Disaccoppiamento completo
**Contro:** Overhead, difficile debug, non necessario
**Decisione:** Scartata - aggiunge complessità inutile

### Alternativa 3: State Machine
Modellare pipeline come state machine con trigger come transitions.

**Pro:** Formalmente corretto
**Contro:** Richiede refactoring completo, breaking changes
**Decisione:** Scartata - troppo invasivo

**Scelta finale:** Approccio incrementale con unificazione graduale (questo piano)

---

## Riferimenti Codice Importante

### File Chiave da Modificare
1. `core/pipeline.go` - Aggiungere ExecutionID a PipelineState
2. `core/interpolate_value.go` - Esporre ctx._execution
3. `pipeline/pipeline.go` - Unificare Run(), rimuovere RunFromTriggers()
4. `steps/webhook.go` - Path dinamico, one-shot mode
5. `steps/cron.go` - One-shot mode
6. `db/pipeline_state_manager.go` - Passare execution ID, callback

### Test da Aggiornare
- `tests/` - Verificare compatibilità
- Nuovi test in `tests/trigger_midpipeline_test.go`
- Integration test in `tests/websocket_integration_test.go`

### Esempi da Verificare
- `examples/webhook/pipeline.yaml`
- `examples/cron/cron_pipeline.yml`
- `examples/cron/cron_advanced.yml`

---

## Timeline Stimata

**Fase 1 (ExecutionID):** 2-3 ore
**Fase 2 (One-Shot Triggers):** 4-6 ore
**Fase 3 (Unify Run):** 6-8 ore
**Fase 4 (Integration):** 3-4 ore
**Fase 5 (Testing):** 4-6 ore

**Totale:** 19-27 ore (3-4 giorni lavorativi)

---

## Prossimi Passi

1. Review questo documento con team/stakeholder
2. Creare branch feature: `feature/trigger-unification`
3. Implementare Fase 1 (ExecutionID) e testare
4. Procedere incrementalmente fase per fase
5. Merge a main dopo tutti i test passano

---

**Documento creato:** 2025-11-08
**Ultima modifica:** 2025-11-08
**Autore:** Claude Code Analysis
**Status:** Ready for Implementation
