# Copyright (c) 2023 Arista Networks, Inc.
# Use of this source code is governed by the Apache License 2.0
# that can be found in the COPYING file.

import math
import statistics

from google.protobuf.timestamp_pb2 import Timestamp
from google.protobuf import wrappers_pb2 as wrapperpb

from cloudvision.cvlib import ActionFailed, TimeoutExpiry

from arista.connectivitymonitor.v1.services import (
    ProbeStatsServiceStub,
    ProbeStatsStreamRequest,
    ProbeStatsRequest
)

from arista.connectivitymonitor.v1 import models
from arista.subscriptions.subscriptions_pb2 import Operation


ONE_HOUR_NS = 3600000000000

stub = ctx.getApiClient(ProbeStatsServiceStub)

monitorTimeout = ctx.action.args.get("monitorTimeout")
timeout = int(monitorTimeout) if monitorTimeout else 300

anomaly_score_threshold = ctx.action.args.get("anomaly_score_threshold")
anomaly_threshold = int(anomaly_score_threshold) if anomaly_score_threshold else 100

critical_level = ctx.action.args.get("critical_level")
critical_lvl = int(critical_level) if critical_level else 3

device_id = ctx.action.args.get("DeviceID")
host = ctx.action.args.get("host")
vrf = ctx.action.args.get("vrf")
source_intf = ctx.action.args.get("source_intf")
stat = ctx.action.args.get("stat").casefold()

if not(device_id and host and stat):
    raise ActionFailed("Required arguments not found")

if not(stat == "latency" or stat == "jitter" or stat == "http_response" or stat == "packet_loss"):
    raise ActionFailed("Invalid statistic name given")

if timeout < 1 or anomaly_threshold < 0 or critical_lvl < 0:
    raise ActionFailed("'timeout', 'anomaly_score_threshold', 'critical_level' must all have positive values")

probeKey = f"Device = {device_id}, Host = {host}, VRF = {vrf}, Source interface = {source_intf}"

ctx.info(f"Monitoring the Connectivity Monitor probe ({probeKey}) for anomalies in {stat} for {timeout} seconds")

with ctx.getCvClient() as client:
    ccStartTs = ctx.action.getCCStartTime(client)
    if not ccStartTs:
        raise ActionFailed("No change control ID present")
    ccStart = Timestamp()
    ccStart.FromNanoseconds(int(ccStartTs))

key = models.ProbeKey(
    device_id=wrapperpb.StringValue(value=device_id),
    host=wrapperpb.StringValue(value=host),
    vrf=wrapperpb.StringValue(value=vrf),
    source_intf=wrapperpb.StringValue(value=source_intf)
)

connectivity_filter = models.ProbeStats(
    key=key
)

prev_hr = int(ccStartTs) - ONE_HOUR_NS

get = ProbeStatsRequest(
    key=key,
    time=ccStart
)

get_range = ProbeStatsStreamRequest()

get_range.time.start.FromNanoseconds(prev_hr)
get_range.time.end.FromNanoseconds(int(ccStartTs))

get_range.partial_eq_filter.append(connectivity_filter)

baseline_stats = []

baseline_value = 0

for resp in stub.GetAll(get_range, timeout=timeout):
    device_stats = resp.value

    match stat:
        case "latency":
            baseline_value = device_stats.latency_millis.value
        case "jitter":
            baseline_value = device_stats.jitter_millis.value
        case "http_response":
            baseline_value = device_stats.http_response_time_millis.value
        case "packet_loss":
            baseline_value = device_stats.packet_loss_percent.value

    if math.isnan(baseline_value):
        ctx.warning("Connectivity Monitor stat NaN, dropping data point")
        continue

    baseline_stats.append(baseline_value)

if len(baseline_stats) == 0:
    raise ActionFailed(f"No valid data received for the probe ({probeKey})")

baseline_stats_mean = statistics.mean(baseline_stats)

baseline_stats_sd = statistics.stdev(baseline_stats)

updates_received = False

valid_stats = True


def monitor():
    subscribe = ProbeStatsStreamRequest()

    subscribe.partial_eq_filter.append(connectivity_filter)

    cusum_hi = 0
    cusum_lo = 0

    latest_stat = 0

    global updates_received
    global valid_stats

    for resp in stub.Subscribe(subscribe, timeout=timeout):
        # Discard anything that is not the initial and update phase of subscribe
        if resp.type not in {Operation.INITIAL, Operation.UPDATED}:
            continue

        if resp.type is Operation.UPDATED:
            updates_received = True

        device_stats = resp.value

        match stat:
            case "latency":
                latest_stat = device_stats.latency_millis.value
            case "jitter":
                latest_stat = device_stats.jitter_millis.value
            case "http_response":
                latest_stat = device_stats.http_response_time_millis.value
            case "packet_loss":
                latest_stat = device_stats.packet_loss_percent.value

        if math.isnan(latest_stat):
            ctx.warning("Connectivity Monitor stat NaN, dropping data point")
            # Set valid_stats to false so action will fail if last data point is NaN
            valid_stats = False
            continue
        else:
            valid_stats = True

        # when there is no deviation in baseline stats, any value greater than 0 is infinite deviation
        if baseline_stats_sd == 0:
            if latest_stat == 0:
                continue
            raise ActionFailed(f"Connectivity monitor probe '{probeKey}' detected anomaly"
                               f" for {stat} statistic")

        else:
            normalised_stat = (latest_stat - baseline_stats_mean) / baseline_stats_sd

            # calculate the upper and lower CUSUM values
            cusum_hi = max(0, cusum_hi + normalised_stat - critical_lvl)
            cusum_lo = min(0, cusum_lo + normalised_stat + critical_lvl)

            # fail the action if any of the CUSUM values exceed the threshold value
            if(cusum_hi > anomaly_threshold or abs(cusum_lo) > anomaly_threshold):
                raise ActionFailed(f"Connectivity monitor probe '{probeKey}' detected anomaly"
                                   f" for {stat} statistic for a prolonged period of time")


try:
    ctx.doWithTimeout(monitor, timeout)
# ctx.doWithTimeout raises a TimeoutExpiry when the timeout is exceeded. Handle this here
# All other exceptions raised will fail the script
except TimeoutExpiry:
    # On timeout expiry, if no stat changes have occurred, fail the action
    if not updates_received:
        raise ActionFailed(f"No updates received from probe ({probeKey})")
    # If the last data point is NaN, fail the action
    if not valid_stats:
        raise ActionFailed(f"Invalid stats received for probe ({probeKey}),"
                           f" it is possible that the probe is currently down")
