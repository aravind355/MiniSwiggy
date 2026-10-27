# MiniSwiggy: a small distributed food-ordering system (raw Java sockets)

Mini project for Distributed Computing, registration number **23MCCE11**.

MiniSwiggy is a heavily simplified version of the Swiggy food-delivery app. Customers
order food, restaurants accept and cook it, riders pick it up and deliver it, and the
customer sees every step live. All communication uses plain TCP sockets (`java.net`)
with a one-line text protocol. There is no framework, no database and no external library.

## Distributed concepts that run in the code

| Concept | Where | What happens |
|---|---|---|
| Round-robin load balancing | `src/gateway/Gateway.java` (`forwardRead`) | Reads alternate between the live order nodes N1 and N2. |
| Primary-backup replication | `src/node/OrderNode.java` (`handleWrite`, `replicate`) | The primary copies each write to the backup **before** it commits and replies. |
| Heartbeat failure detection + failover | `src/gateway/Gateway.java` (`ping`, `reconcileRoles`) | A PING goes to each node every 1 s. After 3 misses the node is marked DOWN and the backup is promoted, with a new epoch number. |
| Recovery / re-sync | `src/node/OrderNode.java` (`syncFromPeer`) | A restarted node pulls a snapshot from its peer and rejoins as BACKUP. |
| Fencing with epochs | `src/node/OrderNode.java` (`acceptEpoch`) | Messages from an older epoch are refused, so an out-of-date primary cannot overwrite data. |
| Pub/sub push notifications | `src/gateway/PubSub.java` | Live order updates are pushed to the customer, restaurant and rider. |
| Message ordering (sequence numbers) | `src/client/OrderedEventBuffer.java` | Updates that arrive out of order are held back and delivered in order. |
| Idempotent retries | `src/node/OrderStore.java` | A write retried after a crash is not applied twice. |

## Folder layout

```
src/common/    message format, protocol constants, socket helpers, order + catalog data
src/node/      OrderNode (server) and OrderStore (in-memory data)
src/gateway/   Gateway (load balancer, heartbeats, failover), PubSub, ClientSession
src/client/    CustomerClient, RestaurantClient, RiderClient, GatewayClient, OrderedEventBuffer
src/demo/      DemoRunner: scripted end-to-end demo with real processes
tests/         TestRunner: 14 automated tests (no JUnit needed)
scripts/       build / run-tests / run-demo for Windows (.ps1) and Linux/macOS (.sh)
docs/          the project documents (.docx):
               ArchitectureofApp_23MCCE11, ParadigmsandAlgorithms_23MCCE11,
               RequirementSpecificationSimpleApp_23MCCE11, Tutorial_23MCCE11
screenshots/   terminal output captured from the demo and test runs (see screenshots/README.md)
presentation/  slides (.pptx)
logs/          full text output of the last demo, test and raw-protocol runs
```

## Requirements

- JDK 17 or newer (`java -version` and `javac -version` must work). Developed and tested with JDK 25.0.2 on Windows 11.
- Nothing else is needed.

## Quick start

Windows PowerShell, from the project folder:

```powershell
.\scripts\build.ps1        # compile into .\out
.\scripts\run-tests.ps1    # 14 automated tests, about 10 s
.\scripts\run-demo.ps1     # full demo with 11 processes and a crash, about 30 s
```

Linux / macOS:

```sh
sh scripts/build.sh
sh scripts/run-tests.sh
sh scripts/run-demo.sh
```

If PowerShell refuses to run scripts, run this once in the same window:
`Set-ExecutionPolicy -Scope Process Bypass`.

The demo prints `ALL CHECKS PASSED` at the end and saves everything it printed to
`logs/demo-transcript.txt`.

## Running it by hand (one terminal per component)

Build first. Then open a terminal for each component, in the project folder:

```
java -cp out node.OrderNode --id N1 --port 9101 --peer 127.0.0.1:9102
java -cp out node.OrderNode --id N2 --port 9102 --peer 127.0.0.1:9101
java -cp out gateway.Gateway --port 9000 --nodes N1=127.0.0.1:9101,N2=127.0.0.1:9102
java -cp out client.RestaurantClient --id R1
java -cp out client.RiderClient --id RD1
java -cp out client.CustomerClient --id C1
```

Each client prints its commands (type `help`). For example:

```
C1> restaurants
C1> menu R1
C1> order R1 R1-M1x2,R1-M3x1
R1> accept O-1
RD1> claim
R1> ready O-1
RD1> pickup O-1
RD1> deliver O-1
```

Restaurants and riders can also run on their own: add `--auto` (plus optional `--prep-ms 2000`
or `--ride-ms 1500`).

To see failover, press Ctrl+C in the N1 window. After about 3 seconds the gateway prints
`FAILURE DETECTED` and `FAILOVER`, and N2 takes over. Start N1 again with the same command,
and it re-syncs from N2 and rejoins as BACKUP.

Optional gateway setting: `--jitter-ms 250` adds a random delay of 0-250 ms to each pushed
event. This simulates a network where messages can overtake each other, so you can watch
the client put them back in order.

## Protocol (one message per line)

```
TYPE|key1=value1|key2=value2        values are URL-encoded
```

| Direction | Messages |
|---|---|
| client -> gateway | `HELLO`, `SUBSCRIBE`, `LIST_RESTAURANTS`, `GET_MENU`, `GET_ORDER`, `LIST_ORDERS`, `PLACE_ORDER`, `UPDATE_STATUS`, `CLAIM_ORDER`, `CLUSTER` |
| gateway -> client | `OK`, `ERROR` (replies, matched by `reqId`), `EVENT` (pushed) |
| gateway -> node | the reads/writes above, plus `PING`, `SET_ROLE` |
| node -> node | `REPLICATE`, `SYNC_REQUEST` |

Example: a customer places an order and later gets a pushed update.

```
PLACE_ORDER|restaurant=R1|items=R1-M1x2%2CR1-M3x1|reqId=C1-mb2x-2
OK|servedBy=N1|role=PRIMARY|event=PLACED|retry=false|orderId=O-1|...|version=1|reqId=C1-mb2x-2
EVENT|event=ACCEPTED|orderId=O-1|seq=2|status=ACCEPTED|customer=C1|restaurant=R1|...
```

## Ports

| Component | Default port |
|---|---|
| Gateway | 9000 |
| Order node N1 | 9101 |
| Order node N2 | 9102 |

Everything listens on 127.0.0.1 by default. To run across machines, start nodes and the
gateway with `--bind 0.0.0.0` and use real IP addresses in `--peer`, `--nodes` and `--gateway`.

## Troubleshooting

- **`Address already in use`**: an old node or gateway is still running. Close its window,
  or find it with `Get-NetTCPConnection -LocalPort 9000,9101,9102` (PowerShell)
  or `lsof -i :9000` (Linux/macOS) and stop that process.
- **`no reply from gateway`**: the gateway is not running, or it is on another port.
- **`NO_NODE_AVAILABLE`**: both order nodes are down; start at least one.

## Known limitations (on purpose, to keep it small)

- The gateway is a single point of failure. The order nodes are replicated; the gateway is not.
- Data is kept in memory only. If both nodes are down at the same time, orders are lost.
- The gateway decides who is primary (a coordinator), instead of running a consensus
  algorithm such as Raft. It works for one gateway, not for several.
- While the backup is down, the primary keeps accepting writes without a replica.
  This favours availability; the backup catches up when it restarts.
