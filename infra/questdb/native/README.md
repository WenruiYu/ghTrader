# QuestDB native deployment (non-Docker)

This runbook avoids Docker and keeps QuestDB on a direct host path on the NVMe system disk (ext4). ZFS is optional and only needed if you want compression.

## Install QuestDB (user-run)

1) Download the QuestDB tarball to `/home/ops/questdb` and extract it.
2) Copy the tuned config into the install:
   - `cp /home/ops/ghTrader/infra/questdb/native/server.conf /home/ops/questdb/conf/server.conf`
   - Note: QuestDB 9.x uses split shared workers (`shared.network/query/write.worker.count`) as set in `server.conf`.
3) Quick verification (optional):
   - `/home/ops/questdb/bin/questdb.sh status`
   - Expect `Not running` before starting the service.
3) Create the data directory on NVMe and link it to the default `db` path:
   - `mkdir -p /home/ops/questdb-data`
   - `ln -sfn /home/ops/questdb-data /home/ops/questdb/db`

## System limits (sudo required)

Apply kernel limits and file limits (adjust values if you already tuned them):

```
sudo tee /etc/sysctl.d/99-questdb.conf <<'EOF'
vm.max_map_count=2097152
fs.file-max=4194304
fs.nr_open=2097152
EOF
sudo sysctl --system
```

Set user limits for the `ops` user:

```
sudo tee /etc/security/limits.d/99-questdb.conf <<'EOF'
ops soft nofile 2097152
ops hard nofile 2097152
ops soft memlock unlimited
ops hard memlock unlimited
EOF
```

## Systemd user service (user-run)

```
mkdir -p ~/.config/systemd/user
cp /home/ops/ghTrader/infra/questdb/native/questdb.service ~/.config/systemd/user/questdb.service
systemctl --user daemon-reload
systemctl --user enable --now questdb
```

If your QuestDB launcher is not at `/home/ops/questdb/bin/questdb.sh`, update `ExecStart` in the service file.

## Systemd system service (auto-start on reboot/crash)

Use this if you want QuestDB to start even after logout/reboot without enabling linger.

```
sudo cp /home/ops/ghTrader/infra/questdb/native/questdb.system.service /etc/systemd/system/questdb.service
sudo systemctl daemon-reload
sudo systemctl enable --now questdb
sudo systemctl status questdb
```

Rollback:

```
sudo systemctl disable --now questdb
sudo rm /etc/systemd/system/questdb.service
sudo systemctl daemon-reload
```

## Migration from Docker (user-run, sudo required)

1) Locate the Docker volume:
   - `sudo docker volume ls | grep questdb`
   - `sudo docker volume inspect <volume_name> | grep Mountpoint`
2) Stop Docker QuestDB:
   - `sudo docker compose -f /home/ops/ghTrader/infra/questdb/docker-compose.yml down`
3) Copy data to the native path:
   - `sudo rsync -a <mountpoint>/db/ /home/ops/questdb-data/`
4) Fix ownership:
   - `sudo chown -R ops:ops /home/ops/questdb-data`
5) Start the native service:
   - `systemctl --user restart questdb`

### Rollback

```
systemctl --user stop questdb
sudo docker compose -f /home/ops/ghTrader/infra/questdb/docker-compose.yml up -d
```

## Validation

- `ghtrader db questdb-health`
- `curl -s http://127.0.0.1:9003/metrics | grep questdb_pg_wire_connections`
- Run a simple query via the dashboard SQL explorer to confirm read access.


ssh -N \
  -L 9000:127.0.0.1:9000 \
  -L 9003:127.0.0.1:9003 \
  ops@58.213.44.14
