import json
import matplotlib.pyplot as plt
import numpy as np

# ----------------------------
# Load data
# ----------------------------
with open("tpcc_result.json") as f:
    data = json.load(f)

throughput = data["throughputInfo"]

timestamp_start_global = min(info["timestampStart"] for info in throughput)

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




aborts = data["aborts"]

aborts_np = np.array([
    [
        a["timestampProcessed"] - timestamp_start_global,
        a["timestampAcknowledged"] - timestamp_start_global,
        a["abortedTid"]
    ]
    for a in aborts
])




plt.figure(figsize=(12, 6))
plt.plot(timestamps_np, throughputs_np, marker='o', label="Throughput")

for abort in aborts_np:
    ts_p = abort[0]
    ts_a = abort[1]
    tid = abort[2]

    plt.axvline(x=ts_p, color='r', linestyle='--')
    plt.axvline(x=ts_a, color='r', linestyle='--')

plt.title("Running Throughput of Committed Transactions")
plt.xlabel("Time (ms since start)")
plt.ylabel("Throughput (Committed TXs / s)")

plt.tight_layout()
plt.show()
