import json
import matplotlib.pyplot as plt
import numpy as np


baseline_result = "result_baseline"
abort_result = "result_many_abort"
vms_recovery_result = "result_many_order_recovery"

with open(f"{abort_result}_25k.json") as f:
    data_abort = json.load(f)

with open(f"{baseline_result}_25k.json") as f:
    data_baseline = json.load(f)

with open(f"{vms_recovery_result}_25k.json") as f:
    data_recovery = json.load(f)

def compute_throughput_and_normalize_timestamps(data):
    timestamp_start_global = min(info["timestampStart"] for info in data["throughputInfo"])
    timestamp_first_end_global = max(info["timestampEnd"] for info in data["throughputInfo"])

    total_committed = 0
    timestamps = []

    for info in data["throughputInfo"]:
        total_committed += info["numCommittedTiDs"]
        timestamps.append(info["timestampStart"])
        timestamps.append(info["timestampEnd"])

    throughput = total_committed/((timestamp_first_end_global-timestamp_start_global)/1000)
    timestamps_np = np.array(timestamps)
    timestamps_np = timestamps_np - timestamp_start_global

    return throughput, timestamps_np

throughput_abort, timestamps_np_abort = compute_throughput_and_normalize_timestamps(data_abort)
throughput_baseline, timestamps_np_baseline = compute_throughput_and_normalize_timestamps(data_baseline)
throughput_recovery, timestamps_np_recovery = compute_throughput_and_normalize_timestamps(data_recovery)

def batch_latencies(timestamps):
    latencies = []
    for i in range(0, len(timestamps), 2):
        latency = timestamps[i+1] - timestamps[i]
        latencies.append(latency)

    return np.array(latencies)

# use right baseline i.e. the 35k
abort_batch_latencies = batch_latencies(timestamps_np_abort)
baseline_batch_latencies = batch_latencies(timestamps_np_baseline)

print(24999*len(abort_batch_latencies)/np.sum(abort_batch_latencies))
# the acknowledgment latency
def protocol_ack_latency(protocol_timestamps, start, end):
    # acknowledgment latency
    latencies = []
    for protocol_timestamp in protocol_timestamps:
        if protocol_timestamp["timestampProcessed"] > start and protocol_timestamp["timestampAcknowledged"] < end:
            latency = protocol_timestamp["timestampAcknowledged"] - protocol_timestamp["timestampProcessed"]
            latencies.append(latency)

    return latencies


abort_exp_start = min(info["timestampStart"] for info in data_abort["throughputInfo"])
abort_exp_end = max(info["timestampEnd"] for info in data_abort["throughputInfo"])
abort_protocol_ack_latencies = protocol_ack_latency(data_abort["aborts"], abort_exp_start, abort_exp_end)

crash_protocol_ack_latencies = protocol_ack_latency(data_recovery["crashes"], 0, np.infty)

reconnection_protocol_ack_latencies = protocol_ack_latency(data_recovery["reconnections"], 0, np.infty)

# LATENCY
print(f"BASELINE: there are {len(baseline_batch_latencies)} batches, with an average latency of {np.mean(baseline_batch_latencies)} ms")
print(f"ABORT: there are {len(abort_batch_latencies)} batches, with an average latency of {np.mean(abort_batch_latencies)} ms")
print()

# THROUGHPUTS
print(f"BASELINE: average throughput of {throughput_baseline} TXs/s")
print(f"ABORT: there are {throughput_abort} TXs/s")
print()

# ACKS
print(f"ABORT: there are {len(abort_protocol_ack_latencies)} aborts initiated and acknowledged "
      f"on average taking {np.mean(abort_protocol_ack_latencies)} ms")
print(f"RECOVERY: there are {len(crash_protocol_ack_latencies)} crashes initiated and acknowledged "
      f"on average taking {np.mean(crash_protocol_ack_latencies)} ms")
print(f"RECOVERY: there are {len(reconnection_protocol_ack_latencies)} reconnections initiated and acknowledged "
      f"on average taking {np.mean(reconnection_protocol_ack_latencies)} ms")