/**
 * progress.js — tiny in-place progress reporter for batch inserts.
 *
 * Usage:
 *   const p = startProgress(total, 'UserInfo');
 *   for (...) { await insert(); p.tick(); }
 *   p.done();
 */

function formatDuration(ms) {
  const s = Math.floor(ms / 1000);
  const h = Math.floor(s / 3600);
  const m = Math.floor((s % 3600) / 60);
  const sec = s % 60;
  if (h > 0) return `${h}h${m}m${sec}s`;
  if (m > 0) return `${m}m${sec}s`;
  return `${sec}s`;
}

function renderBar(pct, width = 24) {
  const filled = Math.round((pct / 100) * width);
  return '[' + '█'.repeat(filled) + '░'.repeat(width - filled) + ']';
}

export function startProgress(total, label = '') {
  const startedAt = Date.now();
  let count = 0;
  // Print at most every 250ms OR every 50 rows, whichever comes first.
  let lastPrint = 0;
  const MIN_INTERVAL_MS = 250;
  const MIN_STEP_ROWS  = 50;
  let lastStep = 0;

  const print = (final = false) => {
    const pct      = total === 0 ? 100 : Math.floor((count / total) * 100);
    const elapsed  = Date.now() - startedAt;
    const rate     = count / (elapsed / 1000 || 1);          // rows/sec
    const eta      = count === 0 || count >= total ? 0 : ((total - count) / rate) * 1000;
    const bar      = renderBar(pct);
    const line =
      `  ${label} ${bar} ${pct.toString().padStart(3)}%  ` +
      `${count}/${total}  ` +
      `elapsed ${formatDuration(elapsed)}` +
      (final ? '' : `  ETA ${formatDuration(eta)}`);

    // Overwrite the current line
    if (process.stdout.isTTY) {
      process.stdout.write('\r' + line.padEnd(100));
      if (final) process.stdout.write('\n');
    } else {
      // Non-TTY (e.g. piped): print full lines periodically
      console.log(line);
    }
  };

  return {
    tick() {
      count++;
      const now = Date.now();
      if (
        count === total ||
        (now - lastPrint >= MIN_INTERVAL_MS && count - lastStep >= MIN_STEP_ROWS)
      ) {
        print(false);
        lastPrint = now;
        lastStep  = count;
      }
    },
    done() {
      print(true);
    },
  };
}
