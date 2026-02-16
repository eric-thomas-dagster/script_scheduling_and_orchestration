# Real-World Prefect Flows Analysis

## Test Results from Prefect Examples Repository

### Tested Flows

1. **hello_world.py** - Flow with no tasks
2. **local_concurrency_with_task_runner.py** - Tasks with `.map()` for parallel execution
3. **conditionally_retry_with_delay.py** - Task without flow wrapper

### Current Results

| Flow | Tasks Detected | Flows Detected | Ops in Graph | Status |
|------|---------------|----------------|--------------|--------|
| prefect_flow_example (our original) | 3 | 1 | 3 | ✅ Works perfectly |
| hello_world | 0 | 1 | 1 (fallback) | ⚠️ Flow-only pattern |
| local_concurrency_with_task_runner | 2 | 1 | 1 | ⚠️ Missing `.map()` calls |
| conditionally_retry_with_delay | 1 | 0 | 1 (fallback) | ⚠️ Task-only pattern |

### Patterns Discovered

#### Pattern 1: Flow Without Tasks (hello_world.py)
```python
@flow
def hello(name: str = "Marvin"):
    get_run_logger().info(f"Hello, {name}!")
```

**Issue**: Flow has no tasks, just direct Python code.
**Current Behavior**: Falls back to subprocess asset.
**Desired Behavior**: Skip graph_asset creation, run as subprocess.

---

#### Pattern 2: Task Without Flow (conditionally_retry_with_delay.py)
```python
@task(retries=2, retry_delay_seconds=[3, 9])
def make_api_call():
    response = httpx.get("https://httpbin.org/status/503")
    response.raise_for_status()
```

**Issue**: Task defined but no @flow wrapper.
**Current Behavior**: Falls back to subprocess asset.
**Desired Behavior**: Skip graph_asset creation, run as subprocess.

---

#### Pattern 3: Task.map() for Parallelism (local_concurrency_with_task_runner.py)
```python
@task
def fetch_url(url: str) -> dict:
    return httpx.get(url).json()

@task
def list_articles(pages: int):
    # Task calling another task with .map()
    _pages = fetch_url.map(...)  # Method call!
    return data

@flow
def extract(pages: int):
    article_urls = list_articles(pages)  # Regular call - we catch this
    _articles = fetch_url.map(article_urls)  # Method call - we miss this!
```

**Issue**:
1. `.map()` is a method call (`task.map()`), not a function call
2. Our AST parser only detects `ast.Call` with `ast.Name` function
3. `.map()` is `ast.Call` with `ast.Attribute` function

**Current Behavior**:
- Detects `list_articles(pages)` ✅
- Misses `fetch_url.map(...)` ❌

**Desired Behavior**: Detect `.map()` calls as task usage.

---

#### Pattern 4: Tasks Calling Tasks
```python
@task
def fetch_url(...):
    pass

@task
def list_articles():
    # Task calling another task!
    _pages = fetch_url.map(...)
    return data
```

**Issue**: Tasks can call other tasks, not just flows calling tasks.
**Current Behavior**: We don't parse task bodies for task calls.
**Consideration**: Do we want to show this in the graph? Or is flow-level visibility enough?

---

## Improvements Needed

### Priority 1: Handle .map() Calls

Update `_extract_task_calls()` to detect:
```python
# Current: Only catches ast.Name
if isinstance(stmt.value.func, ast.Name):
    task_name = stmt.value.func.id

# Need to add: Also catch ast.Attribute for .map()
if isinstance(stmt.value.func, ast.Attribute):
    if isinstance(stmt.value.func.value, ast.Name):
        task_name = stmt.value.func.value.id  # The task object
        method_name = stmt.value.func.attr      # "map"
        if method_name == "map":
            # This is a task.map() call
```

### Priority 2: Skip Graph Asset for Edge Cases

**Condition 1**: Flow with no tasks
```python
if flows and not tasks:
    logger.info(f"Flow has no tasks, using regular subprocess asset")
    return None
```

**Condition 2**: Tasks with no flow
```python
if tasks and not flows:
    logger.info(f"Tasks found but no flow, using regular subprocess asset")
    return None
```

### Priority 3: Handle Task Decorator Parameters

Tasks often have configuration:
```python
@task(
    retries=3,
    retry_delay_seconds=[10, 30, 60],
    retry_condition_fn=retry_on_503,
)
```

**Options**:
1. Parse and map to Dagster's retry policies ✨ (ambitious)
2. Ignore for now, just preserve behavior ✅ (pragmatic)

### Priority 4: Handle Return Statements in Flows

Many flows have explicit returns:
```python
@flow
def extract(pages: int) -> None:
    article_urls = list_articles(pages)
    _articles = fetch_url.map(article_urls)
    # No return, just side effects
```

Our current code looks for returns, but many flows don't return values.

---

## Implementation Plan

### Step 1: Improve AST Parser (30 min)
- Add `.map()` detection in `_extract_task_calls()`
- Add validation for flows with tasks
- Better handling of return statements

### Step 2: Add Validation (10 min)
- Skip graph_asset if no tasks found
- Skip graph_asset if no flow found
- Log clear messages about fallback

### Step 3: Test with Examples (20 min)
- Verify all 4 flows work correctly
- Check op counts in GraphQL
- Test materialization if possible

### Step 4: Document Supported Patterns (10 min)
- Update PREFECT_MAPPING_USAGE.md
- Add examples of supported patterns
- Document known limitations

---

## Expected Results After Improvements

| Flow | Expected Ops | Notes |
|------|-------------|--------|
| prefect_flow_example | 3 ops | ✅ Already works |
| hello_world | 1 op (subprocess) | Skip graph_asset, no tasks |
| local_concurrency_with_task_runner | 2 ops | Detect both tasks via .map() |
| conditionally_retry_with_delay | 1 op (subprocess) | Skip graph_asset, no flow |

---

## Limitations We Accept (For Now)

1. **Dynamic task generation**: Won't catch tasks created in loops
2. **Nested task calls**: Tasks calling tasks - we'll show flat structure at flow level
3. **Task configuration**: Won't map Prefect retries to Dagster retries
4. **Futures and async**: `as_completed()`, `.result()`, etc. are runtime, not visible in AST
5. **Task runners**: ThreadPoolTaskRunner, DaskTaskRunner - we'll ignore these

These limitations are OK because:
- We're providing **visibility**, not perfect replication
- Fallback to subprocess ensures nothing breaks
- Users can still run and observe their Prefect flows
- This is a migration aid, not a permanent solution
