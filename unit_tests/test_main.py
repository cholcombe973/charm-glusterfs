# Copyright 2017 Canonical Ltd
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import sys
import unittest
from unittest.mock import MagicMock

from result import Ok

from lib.gluster.peer import Peer, State
from lib.gluster.volume import Brick, Transport, Volume, VolumeType
from reactive import main

mock_apt = MagicMock()
sys.modules['apt'] = mock_apt
mock_apt.apt_pkg = MagicMock()

import uuid


class Test(unittest.TestCase):
    def testPeersAreNotReady(self):
        peer_list = [
            Peer(uuid=uuid.UUID('3da2c343-7c67-499d-a6bb-68591cc72bc1'),
                 hostname="host-{}".format(
                     uuid.UUID('8fd64553-8925-41f5-b64a-1ba4d359c73b')),
                 status=State.PeerInCluster),
            Peer(uuid=uuid.UUID('3da2c343-7c67-499d-a6bb-68591cc72bc2'),
                 hostname="host-{}".format(
                     uuid.UUID('8fd64553-8925-41f5-b64a-1ba4d359c73c')),
                 status=State.AcceptedPeerRequest),
        ]
        result = main.peers_are_ready(Ok(peer_list))
        self.assertFalse(result)

    def testPeersAreReady(self):
        peer_list = [
            Peer(uuid=uuid.UUID('3da2c343-7c67-499d-a6bb-68591cc72bc1'),
                 hostname="host-{}".format(
                     uuid.UUID('8fd64553-8925-41f5-b64a-1ba4d359c73b')),
                 status=State.PeerInCluster),
            Peer(uuid=uuid.UUID('3da2c343-7c67-499d-a6bb-68591cc72bc2'),
                 hostname="host-{}".format(
                     uuid.UUID('8fd64553-8925-41f5-b64a-1ba4d359c73c')),
                 status=State.PeerInCluster),
        ]
        result = main.peers_are_ready(Ok(peer_list))
        self.assertTrue(result)

    def testFindNewPeers(self):
        peer1 = Peer(uuid=uuid.UUID('3da2c343-7c67-499d-a6bb-68591cc72bc1'),
                     hostname="host-{}".format(
                         uuid.UUID('8fd64553-8925-41f5-b64a-1ba4d359c73b')),
                     status=State.PeerInCluster)
        peer2 = Peer(uuid=uuid.UUID('3da2c343-7c67-499d-a6bb-68591cc72bc2'),
                     hostname="host-{}".format(
                         uuid.UUID('8fd64553-8925-41f5-b64a-1ba4d359c73c')),
                     status=State.AcceptedPeerRequest)

        # peer1 and peer2 are in the cluster but only peer1 is actually
        # serving a brick. find_new_peers should return peer2 as a new peer
        peers = [peer1, peer2]
        existing_brick = Brick(peer=peer1,
                               brick_uuid=uuid.UUID(
                                   '3da2c343-7c67-499d-a6bb-68591cc72bc1'),
                               path="/mnt/brick1",
                               is_arbiter=False)
        volume_info = Volume(name="test",
                             vol_type=VolumeType.Replicate,
                             vol_id=uuid.uuid4(),
                             status="online", bricks=[existing_brick],
                             arbiter_count=0, disperse_count=0, dist_count=0,
                             replica_count=3, redundancy_count=0,
                             stripe_count=0, transport=Transport.Tcp,
                             snapshot_count=0, options={})
        new_peers = main.find_new_peers(peers=peers, volume_info=volume_info)
        self.assertListEqual(new_peers, [peer2])

    def testCartesianProduct(self):
        peer1 = Peer(uuid=uuid.UUID('3da2c343-7c67-499d-a6bb-68591cc72bc1'),
                     hostname="server1",
                     status=State.PeerInCluster)
        peer2 = Peer(uuid=uuid.UUID('3da2c343-7c67-499d-a6bb-68591cc72bc2'),
                     hostname="server2",
                     status=State.AcceptedPeerRequest)
        expected = [
            Brick(peer=peer1,
                  brick_uuid=None,
                  path="/mnt/brick1",
                  is_arbiter=False),
            Brick(peer=peer2,
                  brick_uuid=None,
                  path="/mnt/brick1",
                  is_arbiter=False),
            Brick(peer=peer1,
                  brick_uuid=None,
                  path="/mnt/brick2",
                  is_arbiter=False),
            Brick(peer=peer2,
                  brick_uuid=None,
                  path="/mnt/brick2",
                  is_arbiter=False)
        ]
        peers = [peer1, peer2]
        paths = ["/mnt/brick1", "/mnt/brick2"]
        result = main.brick_and_server_cartesian_product(peers=peers,
                                                         paths=paths)
        for brick in result:
            print("Cartesian: {}".format(brick.uuid))
        self.assertListEqual(result, expected)


if __name__ == "__main__":
    unittest.main()
