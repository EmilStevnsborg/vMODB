import json
import matplotlib.pyplot as plt
import numpy as np


baseline_result = "result_baseline"
abort_result = "result_abort"
vms_recovery_result = "result_vms_recovery"

# experiment = abort_result
# experiment = vms_recovery_result
experiment = baseline_result

with open(f"test_results_plot_final/{experiment}.json") as f:
    data_experiment = json.load(f)


timestamp_start_global = min(info["timestampStart"] for info in data_experiment["throughputInfo"])
timestamp_first_end_global = min(info["timestampEnd"] for info in data_experiment["throughputInfo"])

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

plt.figure(figsize=(10, 6))
plt.plot(timestamps_np/1000, throughputs_np, marker='o', label="Throughput")

for abort in data_experiment["aborts"]:
    ts_p = abort["timestampProcessed"]-timestamp_start_global
    ts_a = abort["timestampAcknowledged"]-timestamp_start_global
    tid = abort["abortedTid"]

    plt.axvline(x=ts_p/1000, color="r", linestyle="--", label=f"abort init")
    plt.axvline(x=ts_a/1000, color="r", linestyle="-", label=f"abort ACK")


for crash in data_experiment["crashes"]:
    ts_p = crash["timestampProcessed"]-timestamp_start_global
    ts_a = crash["timestampAcknowledged"]-timestamp_start_global
    vms = crash["crashedVms"]

    plt.axvline(x=ts_p/1000, color="b", linestyle="--", label=f"{vms} crash")
    plt.axvline(x=ts_a/1000, color="b", linestyle="-", label=f"{vms} crash ACK")


for reconnection in data_experiment["reconnections"]:
    ts_p = reconnection["timestampProcessed"]-timestamp_start_global
    ts_a = reconnection["timestampAcknowledged"]-timestamp_start_global
    vms = reconnection["restartedVms"]

    plt.axvline(x=ts_p/1000, color="g", linestyle="--", label=f"{vms} attempts reconnection")
    plt.axvline(x=ts_a/1000, color="g", linestyle="-", label=f"{vms} reconnection ACK")

if experiment == baseline_result:
    #plot constant line
    plt.title("Baseline Running Throughput")
    avg_throughput = np.mean(throughputs_np)
    plt.axhline(y=avg_throughput, color="k", linestyle="--", label=f"Average Throughput ~{('%.0f'%avg_throughput)} TXs / s")
elif experiment == abort_result:
    plt.title("Abort Effects on Running Throughput")
else:
    plt.title("Crash Effects on Running Throughput")

plt.xlabel("Time in seconds (s)")
plt.ylabel("Throughput (Committed TXs / s)")

plt.ylim(32000, 37000)
plt.yticks(range(32000, 37001, 1000))

xmin = max(1, int(np.floor(timestamps_np.min()/1000)))
xmax = int(np.ceil(timestamps_np.max()/1000))

plt.xlim(xmin, xmax)
plt.xticks(range(xmin, xmax, 2))


plt.tight_layout()
plt.legend()
plt.savefig(f"test_results/{experiment}.png")

plt.show()
