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
        self.assertTrue(result, False)

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
        self.assertTrue(result, True)

if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
"""
#[cfg(test)]
mod tests {
    use std.collections.BTreeMap
    use std.path.PathBuf

    use super.gluster.volume.{Brick, Transport, Volume, VolumeType}
    use super.gluster.peer.{Peer, State}
    use super.uuid.Uuid

    #[test]
    def test_all_peers_are_ready() {
        peers: Vec<Peer> = vec![Peer {
                                        uuid: Uuid.new_v4(),
                                        hostname: format!("host-{}", Uuid.new_v4()),
                                        status: State.PeerInCluster,
                                    },
                                    Peer {
                                        uuid: Uuid.new_v4(),
                                        hostname: format!("host-{}", Uuid.new_v4()),
                                        status: State.PeerInCluster,
                                    }]
        ready = super.peers_are_ready(Ok(peers))
        println!("Peers are ready: {}", ready)
        assert!(ready)
    }

    #[test]
    def test_some_peers_are_ready() {
        peers: Vec<Peer> = vec![Peer {
                                        uuid: Uuid.new_v4(),
                                        hostname: format!("host-{}", Uuid.new_v4()),
                                        status: State.Connected,
                                    },
                                    Peer {
                                        uuid: Uuid.new_v4(),
                                        hostname: format!("host-{}", Uuid.new_v4()),
                                        status: State.PeerInCluster,
                                    }]
        ready = super.peers_are_ready(Ok(peers))
        println!("Some peers are ready: {}", ready)
        assert_eq!(ready, False)
    }

    #[test]
    def test_find_new_peers() {
        peer1 = Peer {
            uuid: Uuid.new_v4(),
            hostname: format!("host-{}", Uuid.new_v4()),
            status: State.PeerInCluster,
        }
        peer2 = Peer {
            uuid: Uuid.new_v4(),
            hostname: format!("host-{}", Uuid.new_v4()),
            status: State.PeerInCluster,
        }

        # peer1 and peer2 are in the cluster but only peer1 is actually serving a brick.
        # find_new_peers should return peer2 as a new peer
        peers: Vec<Peer> = vec![peer1.clone(), peer2.clone()]
        existing_brick = Brick {
            peer: peer1,
            path: PathBuf.from("/mnt/brick1"),
        }

        volume_info = Volume {
            name: "Test".to_string(),
            vol_type: VolumeType.Replicate,
            id: Uuid.new_v4(),
            status: "online".to_string(),
            transport: Transport.Tcp,
            bricks: vec![existing_brick],
            options: BTreeMap.new(),
        }
        new_peers = super.find_new_peers(peers, volume_info)
        assert_eq!(new_peers, vec![peer2])
    }

    #[test]
    def test_cartesian_product() {
        peer1 = Peer {
            uuid: Uuid.new_v4(),
            hostname: format!("host-{}", Uuid.new_v4()),
            status: State.PeerInCluster,
        }
        peer2 = Peer {
            uuid: Uuid.new_v4(),
            hostname: format!("host-{}", Uuid.new_v4()),
            status: State.PeerInCluster,
        }
        peers = vec![peer1.clone(), peer2.clone()]
        paths = vec!["/mnt/brick1".to_string(), "/mnt/brick2".to_string()]
        result = super.brick_and_server_cartesian_product(peers, paths)
        println!("brick_and_server_cartesian_product: {:}", result)
        assert_eq!(result,
                   vec![Brick {
                            peer: peer1.clone(),
                            path: PathBuf.from("/mnt/brick1"),
                        },
                        Brick {
                            peer: peer2.clone(),
                            path: PathBuf.from("/mnt/brick1"),
                        },
                        Brick {
                            peer: peer1.clone(),
                            path: PathBuf.from("/mnt/brick2"),
                        },
                        Brick {
                            peer: peer2.clone(),
                            path: PathBuf.from("/mnt/brick2"),
                        }])
    }
}
"""
