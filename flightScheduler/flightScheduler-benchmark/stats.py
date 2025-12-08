import json
import matplotlib.pyplot as plt
import numpy as np


baseline_result = "result_baseline"
abort_result = "result_abort"
vms_recovery_result = "result_vms_recovery"

with open(f"test_results_stats_final/{abort_result}.json") as f:
    data_abort = json.load(f)

with open(f"test_results_stats_final/{baseline_result}_1.json") as f:
    data_baseline = json.load(f)

with open(f"test_results_stats_final/{vms_recovery_result}.json") as f:
    data_recovery = json.load(f)

def load_and_normalize_throughput(data):
    throughput = data["throughputInfo"]
    timestamp_start_global = min(info["timestampStart"] for info in data["throughputInfo"])
    timestamp_first_end_global = min(info["timestampEnd"] for info in data["throughputInfo"])

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

throughputs_np_abort, timestamps_np_abort = load_and_normalize_throughput(data_abort)
throughputs_np_baseline, timestamps_np_baseline = load_and_normalize_throughput(data_baseline)
throughputs_np_recovery, timestamps_np_recovery = load_and_normalize_throughput(data_recovery)

def batch_latencies(timestamps):
    latencies = []
    for i in range(1, len(timestamps)):
        latency = timestamps[i] - timestamps[i - 1]
        latencies.append(latency)

    return latencies

# use right baseline i.e. the 35k
abort_batch_latencies = batch_latencies(timestamps_np_abort)
baseline_batch_latencies = batch_latencies(timestamps_np_baseline)

# the acknowledgment latency
def protocol_ack_latency(protocol_timestamps):
    # acknowledgment latency
    latencies = []
    for protocol_timestamp in protocol_timestamps:
        latency = protocol_timestamp["timestampAcknowledged"] - protocol_timestamp["timestampProcessed"]
        latencies.append(latency)

    return latencies

abort_protocol_ack_latencies = protocol_ack_latency(data_abort["aborts"])
crash_protocol_ack_latencies = protocol_ack_latency(data_recovery["crashes"])
reconnection_protocol_ack_latencies = protocol_ack_latency(data_recovery["reconnections"])

# LATENCY
print(f"BASELINE: there are {len(baseline_batch_latencies)} batches, with an average latency of {np.mean(baseline_batch_latencies)} ms")
print(f"ABORT: there are {len(abort_batch_latencies)} batches, with an average latency of {np.mean(abort_batch_latencies)} ms")

# THROUGHPUTS
print(f"BASELINE: there are {len(throughputs_np_recovery)} batches, with an average throughput of {np.mean(throughputs_np_baseline)} ms")
print(f"ABORT: there are {len(throughputs_np_abort)} batches, with an average throughput of {np.mean(throughputs_np_abort)} ms")

# ACKS
print(f"ABORT: there are {len(abort_protocol_ack_latencies)} aborts initiated and acknowledged "
      f"on average taking {np.mean(abort_protocol_ack_latencies)} ms")
print(f"RECOVERY: there are {len(crash_protocol_ack_latencies)} crashes initiated and acknowledged "
      f"on average taking {np.mean(crash_protocol_ack_latencies)} ms")
print(f"RECOVERY: there are {len(reconnection_protocol_ack_latencies)} reconnections initiated and acknowledged "
      f"on average taking {np.mean(reconnection_protocol_ack_latencies)} ms")