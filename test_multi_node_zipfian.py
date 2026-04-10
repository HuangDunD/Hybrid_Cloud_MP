import os
import json
import test_multi_node as runner


def _parse_zipf_theta_list():
    raw = os.environ.get("ZIPF_THETA_LIST", "0.50,0.70,0.90,0.99")
    values = []
    for part in raw.split(","):
        p = part.strip()
        if not p:
            continue
        values.append(float(p))
    return values


def update_zipfian_theta(client, bench_name, theta):
    if bench_name == "smallbank":
        cfg_name = "smallbank_config.json"
        section = "smallbank"
    elif bench_name == "ycsb":
        cfg_name = "ycsb_config.json"
        section = "ycsb"
    else:
        return

    remote_cfg = os.path.join(runner.remote_workspace, "config", cfg_name)
    sftp = client.open_sftp()

    try:
        rf = sftp.open(remote_cfg, "r")
        content = rf.read().decode("utf-8")
        rf.close()
    except Exception:
        sftp.close()
        return

    data = json.loads(content)
    if section in data:
        data[section]["use_zipfian"] = 1
        data[section]["zipf_theta"] = float(theta)

    tmp_remote = os.path.join(runner.remote_workspace, "config", f".{cfg_name}.tmp")
    wf = sftp.open(tmp_remote, "w")
    wf.write(json.dumps(data, indent=2))
    wf.flush()
    wf.close()
    sftp.close()
    runner.ssh_exec(client, [f"mv {tmp_remote} {remote_cfg}"], verbose=False)


def main():
    runner.tx_hot_list = _parse_zipf_theta_list()
    runner.hot_accounts_list = []
    runner.update_hot_rate = update_zipfian_theta
    runner.main()


if __name__ == "__main__":
    main()
