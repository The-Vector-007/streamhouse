# Runbook

## Raise WSL memory (do this once, carefully)

Your `.wslconfig` carries a warning you wrote yourself: `memory=10GB` + `swap=4GB`
once exhausted the Windows commit limit and killed `dwm.exe`. Measured on
2026-09-01:

| | |
|---|---|
| Host physical RAM | 15.9 GB |
| Windows commit limit | 29.4 GB |
| Commit free, WSL running | 8.0 GB |
| Current reservation | 6 GB + 2 GB swap = 8 GB |
| The config that crashed it | 10 GB + 4 GB swap = **14 GB** |

**Target: `memory=8GB`, keep `swap=2GB`.** That is +2 GB over today and 4 GB below
the config that failed. Do not jump to 10 GB.

In PowerShell on the Windows side:

```powershell
# 1. Check headroom FIRST. Want >= 6.
(Get-CimInstance Win32_OperatingSystem).FreeVirtualMemory/1MB

# 2. Edit C:\Users\<you>\.wslconfig  ->  memory=8GB

# 3. Restart
wsl --shutdown        # wait ~10 seconds before reopening
```

Verify in WSL: `free -h` should show ~7.8Gi total.

If step 1 reads below 6, close Chrome and anything else large, then measure again.
Do not proceed on a low reading.

## Memory hygiene

`autoMemoryReclaim` is deliberately absent from your `.wslconfig` (you removed it
while chasing a boot wedge). So **WSL never gives memory back to Windows** — once a
Spark run balloons the VM, it stays ballooned until you restart it.

Run `wsl --shutdown` from PowerShell between heavy Spark sessions.

## Daily loop

```bash
make up PROFILE=ingest     # kafka only
make test                  # python tests
make test-java             # java tests
make down                  # stop, drop volumes
```

Spark runs on the host from the uv venv, not in a container. That is the normal
local dev loop and it saves roughly 2 GB.

## Profiles

| Profile | Services | Approx RAM |
|---|---|---|
| `ingest` | kafka, ingest-gateway, generator | ~2.0 GB |
| `pipeline` | + spark (host, not container) | ~4.5 GB |
| `serve` | + serving-api | ~5.0 GB |
| `orchestrate` | + airflow, postgres (P8 only) | ~6.5 GB |

Never run `docker compose up` with no profile — it starts everything.

## When Spark will not start

Almost always `JAVA_HOME`. `mise` sets it from `.mise.toml`; confirm with
`echo $JAVA_HOME && java -version`. PySpark 3.5 needs Java 17 or 21, not 24.
