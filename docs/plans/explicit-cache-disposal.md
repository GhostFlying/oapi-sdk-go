# Explicit cache and terminal WebSocket disposal

Baseline: upstream v3.12.1. Preserve upstream licensing and protocol behavior.

The cache cleaner holds its cache through a bound callback, so a cache finalizer cannot release the live cleaner. Add explicit context-aware, idempotent stop-and-join without changing Get/Set expiry semantics. Keep finalizer cleanup nonblocking.

Add a terminal WebSocket Dispose(ctx) operation that prevents future Start calls, requests normal connection shutdown, joins existing SDK workers through CloseAndWait, and stops/joins the private cache cleaner. Leave Start and CloseAndWait retry/reuse behavior unchanged for clients that have not been disposed. A caller deadline reports incomplete teardown rather than claiming success; stopping the cache is still requested.

Validate concurrent repeated cache disposal, a blocked cleaner with a caller deadline, disposal before Start, active-client shutdown, unchanged retry behavior before disposal, and cleaner-goroutine counts after repeated cancelled client lifecycles. Do not introduce protocol changes, generated-code edits, credentials or live provider calls.
