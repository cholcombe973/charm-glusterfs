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
import uuid
from ipaddress import ip_address

import mock
from result import Ok

from lib.gluster import peer

mock_apt = mock.MagicMock()
sys.modules['apt'] = mock_apt
mock_apt.apt_pkg = mock.MagicMock()


class Test(unittest.TestCase):
    def testParsePeerStatus(self):
        expected_peers = [
            peer.Peer(
                uuid=uuid.UUID("663bbc5b-c9b4-4a02-8b56-85e05e1b01c8"),
                hostname=ip_address("172.31.12.7"),
                status=peer.State.PeerInCluster),
            peer.Peer(
                uuid=uuid.UUID("15af92ad-ae64-4aba-89db-73730f2ca6ec"),
                hostname=ip_address("172.31.21.242"),
                status=peer.State.PeerInCluster)
        ]
        with open('unit_tests/peer_status.xml', 'r') as xml_output:
            lines = xml_output.readlines()
            result_list = peer.parse_peer_status("".join(lines))
            self.assertTrue(result_list.is_ok())
            peer_list = result_list.value
            for status in peer_list:
                self.assertIn(member=status, container=expected_peers)
            self.assertEqual(2, len(result_list.value),
                             "Expected 2 peer objects")

    @mock.patch('lib.gluster.peer.resolve_to_ip')
    def testParsePeerList(self, _resolve_to_ip):
        _resolve_to_ip.return_value = Ok("172.31.21.243")
        expected_peers = [
            peer.Peer(
                uuid=uuid.UUID("663bbc5b-c9b4-4a02-8b56-85e05e1b01c8"),
                hostname=ip_address("172.31.12.7"),
                status=peer.State.PeerInCluster),
            peer.Peer(
                uuid=uuid.UUID("15af92ad-ae64-4aba-89db-73730f2ca6ec"),
                hostname=ip_address("172.31.21.242"),
                status=peer.State.PeerInCluster),
            peer.Peer(
                uuid=uuid.UUID("cebf02bb-a304-4058-986e-375e2e1e5313"),
                hostname=ip_address("172.31.21.243"),
                status=None)
        ]
        with open('unit_tests/pool_list.xml', 'r') as xml_output:
            lines = xml_output.readlines()
            result_list = peer.parse_peer_list("".join(lines))
            self.assertTrue(result_list.is_ok())
            peer_list = result_list.value
            for peer_object in peer_list:
                self.assertIn(member=peer_object, container=expected_peers)

    @mock.patch('lib.gluster.peer.peer_list')
    def testGetPeer(self, _peer_list):
        existing_peers = [
            peer.Peer(
                uuid=uuid.UUID("663bbc5b-c9b4-4a02-8b56-85e05e1b01c8"),
                hostname=ip_address("172.31.12.7"),
                status=peer.State.PeerInCluster),
            peer.Peer(
                uuid=uuid.UUID("15af92ad-ae64-4aba-89db-73730f2ca6ec"),
                hostname=ip_address("172.31.21.242"),
                status=peer.State.PeerInCluster)
        ]
        _peer_list.return_value = Ok(existing_peers)
        result = peer.get_peer(hostname=ip_address('172.31.21.242'))
        self.assertIs(result, existing_peers[1])

    @mock.patch('lib.gluster.peer.parse_peer_list')
    @mock.patch('lib.gluster.peer.run_command')
    def testPeerList(self, _run_command, _parse_peer_list):
        # Ignore parse_peer_list.  We test that above
        _parse_peer_list.return_value = Ok("")
        _run_command.return_value = Ok("")
        peer.peer_list()
        _run_command.assert_called_with("gluster",
                                        ["pool", "list", "--xml"],
                                        True,
                                        False)

    @mock.patch('lib.gluster.peer.peer_list')
    @mock.patch('lib.gluster.peer.run_command')
    def testPeerProbe(self, _run_command, _peer_list):
        existing_peers = [
            peer.Peer(
                uuid=uuid.UUID("663bbc5b-c9b4-4a02-8b56-85e05e1b01c8"),
                hostname=ip_address("172.31.12.7"),
                status=peer.State.PeerInCluster),
            peer.Peer(
                uuid=uuid.UUID("15af92ad-ae64-4aba-89db-73730f2ca6ec"),
                hostname=ip_address("172.31.21.242"),
                status=peer.State.PeerInCluster)
        ]
        _peer_list.return_value = Ok(existing_peers)
        peer.peer_probe(hostname='172.31.21.243')
        # print("Peer probe result: {}".format(result.value))
        _run_command.assert_called_with("gluster",
                                        ["peer", "probe", "172.31.21.243"],
                                        True,
                                        False)

    @mock.patch('lib.gluster.peer.run_command')
    def testPeerRemove(self, _run_command):
        _run_command.return_value = Ok("")
        result = peer.peer_remove(hostname='172.31.21.242', force=True)
        self.assertTrue(result.is_ok())
        _run_command.assert_called_with("gluster",
                                        ["peer", "detach", "172.31.21.242",
                                         "force"], True, False)

    @mock.patch('lib.gluster.peer.parse_peer_status')
    @mock.patch('lib.gluster.peer.run_command')
    def testPeerStatus(self, _run_command, _parse_peer_status):
        # Ignore parse_peer_status because we test that above
        _parse_peer_status.return_value = Ok("")
        _run_command.return_value = Ok("")
        result = peer.peer_status()
        self.assertTrue(result.is_ok())
        _run_command.assert_called_with("gluster",
                                        ["peer", "status", "--xml"], True,
                                        False)


if __name__ == "__main__":
    unittest.main()
