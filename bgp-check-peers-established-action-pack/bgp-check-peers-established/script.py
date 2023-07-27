# Copyright (c) 2022 Arista Networks, Inc.
# Use of this source code is governed by the Apache License 2.0
# that can be found in the COPYING file.

from cloudvision.cvlib import ActionFailed

from cloudvision.Connector.grpc_client import create_query
from cloudvision.Connector.codec import Wildcard, Path

ESTABLISHED = "Established"


def extractBGPStats(batch, statsDict, useVrfs=True):
    for notif in batch["notifications"]:
        for stat in notif["updates"]:
            count = notif["updates"][stat]
            # Skip over any path pointers, such as to the vrf counts
            if isinstance(count, Path):
                continue
            # If using vrfs, append the vrf name to the stat to stop overlap
            # The vrf name is the last path element of the notification
            if useVrfs:
                stat = notif['path_elements'][-1] + "_" + stat
            statsDict[stat] = count


with ctx.getCvClient() as client:
    device = ctx.getDevice()
    if device is None or device.id is None:
        err = "Missing change control device" if device is None \
            else f"device {device} is missing 'id'"
        raise ActionFailed(err)
    pathElts = [
        "Devices",
        device.id,
        "versioned-data",
        "routing",
        "bgp",
        "status",
        "vrf",
        Wildcard(),
        "bgpPeerInfoStatusEntry",
        Wildcard()
    ]
    query = [
        create_query([(pathElts, [])], "analytics")
    ]
    failedPeers = []
    # Get current bgp stats
    currBGPStats = {}
    for batch in client.get(query):
        extractBGPStats(batch, currBGPStats)
    # Get the bgpState and fail action if ANY BGP peer is not 'Established'
    for peer in currBGPStats:
        if "_bgpState" in peer:
            if currBGPStats[peer]["Name"] != ESTABLISHED:
                failedPeers += [{peer[:-9]: currBGPStats[peer]["Name"]}]

    if len(failedPeers) > 0:
        failedPeers = [i for n, i in enumerate(failedPeers) if i not in failedPeers[n + 1:]]
        err = f"One or more BGP peers are not in Established state: {failedPeers}"
        raise ActionFailed(err)

ctx.info("BGP all peers are in Established state")
