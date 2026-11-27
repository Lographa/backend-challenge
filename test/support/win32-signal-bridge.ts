/**
 * Preload bridge used ONLY by multi-process integration tests on Windows (`win32`), where
 * `child.kill()` invokes `TerminateProcess` (hard kill) instead of POSIX `SIGTERM`/`SIGINT`.
 *
 * Keeps `src/main.ts` 100% pure production code and immediately calls `process.stdin.unref()`
 * so natural event-loop shutdown (`code 0` without `process.exit(0)`) behaves identically to Linux.
 */
let buffer = '';

process.stdin.on('data', (chunk: Buffer | string) => {
  buffer += typeof chunk === 'string' ? chunk : chunk.toString('utf8');
  let newlineIdx = buffer.indexOf('\n');
  while (newlineIdx !== -1) {
    const line = buffer.slice(0, newlineIdx).trim();
    buffer = buffer.slice(newlineIdx + 1);
    if (line === 'SIGNAL:SIGTERM') {
      process.emit('SIGTERM', 'SIGTERM');
    } else if (line === 'SIGNAL:SIGINT') {
      process.emit('SIGINT', 'SIGINT');
    }
    newlineIdx = buffer.indexOf('\n');
  }
});

process.stdin.on('error', () => {
  // Ignore broken pipe when parent closes stdin after shutdown completes.
});

process.stdin.unref();
