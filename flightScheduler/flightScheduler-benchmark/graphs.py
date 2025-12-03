import json
import matplotlib.pyplot as plt
import numpy as np


baseline_result = "result_baseline"
abort_result = "result_abort"
vms_recovery_result = "result_vms_recovery"
coordinator_recovery_result = "result_coordinator_recovery"

# experiment = abort_result
# experiment = vms_recovery_result
experiment = baseline_result
# experiment = baseline_result

with open(f"{baseline_result}.json") as f:
    data_baseline = json.load(f)

with open(f"{experiment}.json") as f:
    data_experiment = json.load(f)


timestamp_start_global = min(info["timestampStart"] for info in data_experiment["throughputInfo"])

def load_and_normalize_throughput(data):
    throughput = data["throughputInfo"]

    total_committed = 0
    throughputs = []
    timestamps = []

    for info in throughput:
        total_committed += info["numCommittedTiDs"]
        runtime = (info["timestampEnd"] - timestamp_start_global)/1000
        running_throughput = total_committed / runtime
        throughputs.append(running_throughput)
        timestamps.append(info["timestampEnd"])

    # Convert to numpy
    throughputs_np = np.array(throughputs)
    timestamps_np = np.array(timestamps)
    timestamps_np = timestamps_np - timestamp_start_global

    return throughputs_np, timestamps_np

throughputs_np, timestamps_np = load_and_normalize_throughput(data_experiment)

plt.figure(figsize=(12, 6))
plt.plot(timestamps_np, throughputs_np, marker='o', label="Throughput")

for abort in data_experiment["aborts"]:
    ts_p = abort["timestampProcessed"]-timestamp_start_global
    ts_a = abort["timestampAcknowledged"]-timestamp_start_global
    tid = abort["abortedTid"]

    plt.axvline(x=ts_p, color="r", linestyle="--", label=f"abort init")
    plt.axvline(x=ts_a, color="r", linestyle="--", label=f"abort ACK")


for crash in data_experiment["crashes"]:
    ts_p = crash["timestampProcessed"]-timestamp_start_global
    ts_a = crash["timestampAcknowledged"]-timestamp_start_global
    vms = crash["crashedVms"]

    plt.axvline(x=ts_p, color="b", linestyle="--", label=f"{vms} crash")
    plt.axvline(x=ts_a, color="b", linestyle="-", label=f"{vms} crash ACK")

plt.title("Running Throughput of Committed Transactions")
plt.xlabel("Time (ms since start)")
plt.ylabel("Throughput (Committed TXs / s)")

plt.ylim(0, 50000)
plt.yticks(range(0, 50001, 4000))

plt.xlim(0, 20000)
plt.xticks(range(0, 20001, 5000))

plt.tight_layout()
plt.legend()
plt.savefig(f"test_results/{experiment}.png")

plt.show()
