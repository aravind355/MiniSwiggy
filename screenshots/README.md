# Screenshots

The following screenshots capture the execution, tests, failure handling, and recovery verification of the MiniSwiggy distributed cluster:

| File | Description | Corresponding Log |
|---|---|---|
| `01_build_and_tests.png` | Build process and 14 automated test cases passing | `logs/test-output.txt` |
| `02_cluster_startup.png` | Nodes, gateway, restaurants, and riders initialization; N1 elected primary | `logs/demo-transcript.txt` |
| `03_concurrent_orders.png` | Concurrent orders placed simultaneously by multiple customers | `logs/demo-transcript.txt` |
| `04_live_updates_in_order.png` | Real-time push updates delivery and sequence re-ordering | `logs/demo-transcript.txt` |
| `05_round_robin.png` | Read query load balancing alternating between N1 and N2 | `logs/demo-transcript.txt` |
| `06_failure_and_failover.png` | Primary failure detection (heartbeat timeout) and automatic failover to N2 | `logs/demo-transcript.txt` |
| `07_recovery_resync.png` | N1 reboot, snapshot synchronization, and rejoins as backup | `logs/demo-transcript.txt` |
| `08_consistency_and_results.png` | State validation across replicas and all verification checks passing | `logs/demo-transcript.txt` |
| `09_raw_protocol.png` | Raw TCP socket session and protocol communication with gateway | `logs/raw-protocol.txt` |
