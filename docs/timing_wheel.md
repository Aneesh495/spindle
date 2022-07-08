# Hierarchical Timing Wheels in Spindle

Distributed orchestrators must manage millions of concurrent timers for activity timeouts, workflow retry backoffs, and scheduled cron executions. This document explains the design, bitmask mathematics, and cascading mechanics of the 5-tier Varghese and Lauck hierarchical timing wheel implemented in `Spindle.Timers`.

---

## 1. Algorithmic Complexity Comparison

Standard runtime timer queues typically employ binary min-heaps or balanced search trees (such as red-black trees):

| Operation | Binary Min-Heap | Hierarchical Timing Wheel |
| :--- | :--- | :--- |
| **Schedule Timer** | $O(\log N)$ | **$O(1)$** |
| **Cancel Timer** | $O(\log N)$ | **$O(1)$** |
| **Advance Tick** | $O(\log N)$ | **$O(1)$ amortized** |
| **Cache Locality** | Poor (pointer chasing) | Excellent (contiguous slot arrays) |

When managing $10^6$ active timers, the $O(1)$ guarantees of the hierarchical wheel eliminate thread contention and ensure nanosecond-level scheduling latency.

---

## 2. 5-Tier Wheel Bitmask Architecture

Spindle employs a 5-tier hierarchical hashed wheel modeled after network kernel timers:

```
+-------------------------------------------------------------------------------+
| Level 4: 64 Slots  (Span: 4,294,967,296 ticks  ~= 497 days at 10ms/tick)       |
+-------------------------------------------------------------------------------+
       |
       v (Cascades into Level 3)
+-------------------------------------------------------------------------------+
| Level 3: 64 Slots  (Span: 67,108,864 ticks     ~= 7.76 days at 10ms/tick)      |
+-------------------------------------------------------------------------------+
       |
       v (Cascades into Level 2)
+-------------------------------------------------------------------------------+
| Level 2: 64 Slots  (Span: 1,048,576 ticks      ~= 2.91 hours at 10ms/tick)     |
+-------------------------------------------------------------------------------+
       |
       v (Cascades into Level 1)
+-------------------------------------------------------------------------------+
| Level 1: 64 Slots  (Span: 16,384 ticks         ~= 2.73 minutes at 10ms/tick)   |
+-------------------------------------------------------------------------------+
       |
       v (Cascades into Level 0)
+-------------------------------------------------------------------------------+
| Level 0: 256 Slots (Span: 256 ticks            ~= 2.56 seconds at 10ms/tick)   |
+-------------------------------------------------------------------------------+
```

### Slot Index Calculations

Given a timer expiration offset $\Delta = \text{ExpirationTicks} - \text{CurrentTick}$:

1. **Level 0** ($\Delta < 256$):
   $$\text{Slot} = (\text{CurrentTick} + \Delta) \ \& \ 255$$
2. **Level 1** ($256 \le \Delta < 16,384$):
   $$\text{Slot} = ((\text{CurrentTick} + \Delta) \gg 8) \ \& \ 63$$
3. **Level 2** ($16,384 \le \Delta < 1,048,576$):
   $$\text{Slot} = ((\text{CurrentTick} + \Delta) \gg 14) \ \& \ 63$$
4. **Level 3** ($1,048,576 \le \Delta < 67,108,864$):
   $$\text{Slot} = ((\text{CurrentTick} + \Delta) \gg 20) \ \& \ 63$$
5. **Level 4** ($\Delta \ge 67,108,864$):
   $$\text{Slot} = ((\text{CurrentTick} + \Delta) \gg 26) \ \& \ 63$$

---

## 3. Cascading Mechanics & Level Promotion

When the current tick counter increments:
1. Higher tiers are inspected at their respective rollover frequencies (Level 1 rolls over every 256 ticks, Level 2 every 16,384 ticks, etc.).
2. Any timers occupying the current slot in Level 1 are extracted and re-inserted into Level 0.
3. Crucially, as documented in BUG-002, higher-tier cascades must execute **before** Level 0 slot collection so that timers scheduled for the immediate boundary tick are promoted into Level 0 and fired without latency.
4. Level 0 collects all entries in `level0[currentTick & 255]` and invokes their callbacks.
