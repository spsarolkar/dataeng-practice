Evolution of Garbage Collectors in JVM (Java 5 → Java 17)

Below is a clean timeline explaining what GC existed, why new ones were added, and what problems they solved.

🟦 Java 5 (2004): Parallel GC + CMS (early days)
✔ Parallel GC (Throughput Collector)

Default GC for many years

Multi-threaded minor collections

Objective: maximize throughput (do more work per unit time)

Still has significant stop-the-world pauses.

✔ CMS (Concurrent Mark-Sweep) — introduced earlier, matured in Java 5

Purpose: Low pause times
CMS did:

Concurrent marking

Concurrent sweeping

But it did not compact memory, causing fragmentation and eventual promotion failures.

Many enterprises used CMS for low-latency applications.

🟩 Java 6 (2006): CMS improvements

CMS got tuning parameters and became more stable, but flaws remained:

Fragmentation

Many tuning knobs

Not ideal for very large heaps (>10–20 GB)

🟧 Java 7 (2011): G1GC (Garbage-First) introduced (experimental)

G1GC was designed to replace CMS.

Key features:

Region-based heap

Predictable pause times

Concurrent + incremental compaction

Aims to meet a soft pause-time target (e.g., 200ms)

This is the start of modern GC.

🟥 Java 8 (2014): G1GC becomes feature complete

Java 8 became the most popular Java version ever — GC stability mattered.

CMS still default, but:

G1GC became production-ready

Massive improvements in performance

G1 compacted memory, eliminating CMS fragmentation problems

Spark, Kafka, and many JVM frameworks were tuned around Java 8 GC behavior.

🟨 Java 9 (2017): G1 becomes the default GC

Why switch from Parallel GC → G1GC?

Because:

CMS was deprecated

Apps had larger heaps

Predictable pause times were more important

New features:

String deduplication

Unified logging

Improvements in GC ergonomics

This marks the shift to pause time–first design.

🟦 Java 11 (2018): Two Game Changers — ZGC and Epsilon
✔ ZGC (Z Garbage Collector) — Ultra-low latency

< 1 ms pauses

Concurrent compaction

Supports multi-terabyte heaps

Uses colored pointers

Built for modern, high-memory, low-latency apps (FinTech, ML, Big Data).

✔ Epsilon GC

No-op GC

For testing or very short-lived apps

Provides deterministic performance without overhead

🟫 Java 12 (2019): Shenandoah GC (RedHat)

Another low-pause collector, like ZGC, but implemented differently.

Shenandoah:

< 10ms pauses

Fully concurrent compaction

Good for large JVM heaps

Originated from RedHat for Linux distros

Java now had two serious low-latency collectors: ZGC + Shenandoah.

🟩 Java 14–15: Modernization of GC
✔ ZGC becomes production-ready (Java 15)
✔ Shenandoah also becomes production-ready
✔ CMS is removed from JDK

GC landscape simplified into a modern set:

Serial GC (small heaps)

Parallel GC (throughput)

G1GC (default for general use)

ZGC (ultra-low latency)

Shenandoah (low latency, open-source alternative)

🟦 Java 17 (2021 LTS): GC Maturity and Performance

Java 17 stabilized all collectors:

✔ ZGC improvements

Sub-millisecond pauses

Better memory usage

Reduced pointer coloring overhead

✔ G1 improvements

Better region sizing

Faster concurrent marking cycles

✔ Shenandoah is fully integrated

Predictable low-latency for large heaps

✔ JDK 17 → modern GC defaults are far more stable than Java 8
🎯 Summary Table — Evolution of GC
Java Version	Collector	Purpose
Java 5	Parallel GC, CMS	Throughput / Low pause beginnings
Java 6	CMS improvements	Reduced pauses, still fragmentation
Java 7	G1GC introduced	Predictable pauses
Java 8	G1GC stable	Alternative to CMS
Java 9	G1GC default	CMS deprecated
Java 11	ZGC + Epsilon	Ultra-low latency, testing
Java 12	Shenandoah	Low-latency open-source GC
Java 15	ZGC production	Mature sub-ms GC
Java 17	All collectors optimized	G1 default, ZGC & Shenandoah stable