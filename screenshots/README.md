# Screenshots

All nine images were made from the **real text output** of the runs on 29 September 2026
(files in `../logs/`). The text is unchanged; it was drawn into a terminal-style image with
colours added for readability. They are **not** photos of the screen.

| File | Shows | Made from |
|---|---|---|
| 01_build_and_tests.png | build + 14 automated tests passing | logs/test-output.txt |
| 02_cluster_startup.png | nodes, gateway, restaurants, riders start; N1 elected primary | logs/demo-transcript.txt lines 1-40 |
| 03_concurrent_orders.png | three customers order at the same time | lines 42-92 |
| 04_live_updates_in_order.png | pushed live updates, one re-sequenced | lines 170-213 |
| 05_round_robin.png | reads alternate between N1 and N2 | lines 214-232 |
| 06_failure_and_failover.png | N1 hard-killed, 3 missed heartbeats, failover to N2 | lines 288-330 |
| 07_recovery_resync.png | N1 restarts, pulls 6 orders, rejoins as backup | lines 401-440 |
| 08_consistency_and_results.png | identical checksums, all 10 checks passed | lines 489-517 |
| 09_raw_protocol.png | a raw TCP session with the gateway | logs/raw-protocol.txt |

## Screenshots you should take yourself

1. **The demo running in your own PowerShell window** (`.\scripts\run-demo.ps1`), at least during
   step 5 (failover) and at the final `ALL CHECKS PASSED`. A real screen capture shows your
   machine and date.
2. **The six-window manual run** from the Tutorial, section 5 (N1, N2, gateway, customer,
   restaurant, rider side by side), during one complete order. This interactive mode has no
   captured screenshot yet.
3. **Manual failover:** press Ctrl+C in the N1 window and capture the gateway window showing
   `FAILURE DETECTED` and `FAILOVER`, then restart N1 and capture `RECOVERED`.
4. **The test run** (`.\scripts\run-tests.ps1`) in your own window, to replace 01.

Windows: press Win+Shift+S to capture a window or area.
