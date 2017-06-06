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

import mock
from result import Ok

from lib.gluster import peer, volume

mock_apt = mock.MagicMock()
sys.modules['apt'] = mock_apt
mock_apt.apt_pkg = mock.MagicMock()

peer_1 = peer.Peer(
    uuid=uuid.UUID("39bdbbd6-5271-4c23-b405-cc0b67741ebc"),
    hostname="172.20.21.231", status=None)
peer_2 = peer.Peer(
    uuid=uuid.UUID("a51b28e8-6f06-4563-9a5f-48f3f31a6713"),
    hostname="172.20.21.232", status=None)
peer_3 = peer.Peer(
    uuid=uuid.UUID("57dd0230-50d9-452a-be8b-8f9dd9fe0264"),
    hostname="172.20.21.233", status=None)

brick_list = [
    volume.Brick(
        brick_uuid=uuid.UUID("12d4bd98-e102-4174-b99a-ef76f849474e"),
        peer=peer_1,
        path="/mnt/sdb",
        is_arbiter=False),
    volume.Brick(
        brick_uuid=uuid.UUID("a563d73c-ef3c-47c6-b50d-ddc800ef5dae"),
        peer=peer_2,
        path="/mnt/sdb",
        is_arbiter=False),
    volume.Brick(
        brick_uuid=uuid.UUID("cc4a3f0a-f152-4e40-ab01-598f53eb83f9"),
        peer=peer_3,
        path="/mnt/sdb", is_arbiter=False)
]


class Test(unittest.TestCase):
    def testGetLocalBricks(self):
        pass

    def testOkToRemove(self):
        pass

    def testParseQuotaList(self):
        expected_quotas = [
            volume.Quota(path="/", hard_limit=10240, soft_limit=8192,
                         soft_limit_percentage="80%", used=0, avail=10240,
                         soft_limit_exceeded=False, hard_limit_exceeded=False),
            volume.Quota(path="/test2", hard_limit=10240, soft_limit=8192,
                         soft_limit_percentage="80%", used=0, avail=10240,
                         soft_limit_exceeded=False, hard_limit_exceeded=False),
        ]
        with open('unit_tests/quota_list.xml', 'r') as xml_output:
            lines = xml_output.readlines()
            result = volume.parse_quota_list("".join(lines))
            self.assertTrue(result.is_ok())
            self.assertTrue(len(result.value) == 2)
            self.assertListEqual(expected_quotas, result.value)

    def testParseVolumeList(self):
        with open('unit_tests/vol_list.xml', 'r') as xml_output:
            lines = xml_output.readlines()
            result_list = volume.parse_volume_list("".join(lines))
            self.assertTrue(result_list.is_ok())
            self.assertEqual(1, len(result_list.value), "Expected 1 volume")
            self.assertEqual("chris", result_list.value[0],
                             "Expected 1 volume called chris")

    def testParseVolumeStatus(self):
        with open('unit_tests/vol_status.xml', 'r') as xml_output:
            lines = xml_output.readlines()
            result = volume.parse_volume_status("".join(lines))
            self.assertTrue(result.is_ok())
            # for status_item in result.value:
            # print("volume status item: {}".format(status_item))

    def testVolumeAddBrick(self):
        pass

    @mock.patch('lib.gluster.volume.run_command')
    def testVolumeAddQuota(self, _run_command):
        _run_command.return_value = Ok("")
        volume.volume_add_quota("test", "/", 10240)
        _run_command.assert_called_with("gluster",
                                        ["volume", "quota", "test",
                                         "limit-usage", "/", "10240"],
                                        True, False)

    @mock.patch('lib.gluster.volume.volume_create')
    def testVolumeCreateArbiter(self, _volume_create):
        volume.volume_create_arbiter(volume="test", replica_count=3,
                                     arbiter_count=1,
                                     transport=volume.Transport.Tcp,
                                     bricks=brick_list, force=False)
        _volume_create.assert_called_with(
            'test',
            {
                volume.VolumeTranslator.Replica: '3',
                volume.VolumeTranslator.Arbiter: '1'
            }, volume.Transport.Tcp,
            brick_list, False)

    def testVolumeCreate(self):
        pass

    @mock.patch('lib.gluster.volume.volume_create')
    def testVolumeCreateDistributed(self, _volume_create):
        volume.volume_create_distributed(volume="test",
                                         transport=volume.Transport.Tcp,
                                         bricks=brick_list, force=False)
        _volume_create.assert_called_with("test", {}, volume.Transport.Tcp,
                                          brick_list, False)

    @mock.patch('lib.gluster.volume.volume_create')
    def testVolumeCreateErasure(self, _volume_create):
        volume.volume_create_erasure(volume="test", disperse_count=1,
                                     redundancy_count=3,
                                     transport=volume.Transport.Tcp,
                                     bricks=brick_list, force=False)
        _volume_create.assert_called_with(
            'test',
            {
                volume.VolumeTranslator.Disperse: '1',
                volume.VolumeTranslator.Redundancy: '3'
            }, volume.Transport.Tcp,
            brick_list, False)

    @mock.patch('lib.gluster.volume.volume_create')
    def testVolumeCreateReplicated(self, _volume_create):
        volume.volume_create_replicated(volume="test", replica_count=3,
                                        transport=volume.Transport.Tcp,
                                        bricks=brick_list, force=False)
        _volume_create.assert_called_with(
            'test',
            {
                volume.VolumeTranslator.Replica: '3'
            }, volume.Transport.Tcp,
            brick_list, False)

    @mock.patch('lib.gluster.volume.volume_create')
    def testVolumeCreateStriped(self, _volume_create):
        volume.volume_create_striped(volume="test", stripe_count=3,
                                     transport=volume.Transport.Tcp,
                                     bricks=brick_list, force=False)
        _volume_create.assert_called_with(
            'test',
            {
                volume.VolumeTranslator.Stripe: '3'
            }, volume.Transport.Tcp,
            brick_list, False)

    @mock.patch('lib.gluster.volume.volume_create')
    def testVolumeCreateStripedReplicated(self, _volume_create):
        volume.volume_create_striped_replicated(volume="test", stripe_count=1,
                                                replica_count=3,
                                                transport=volume.Transport.Tcp,
                                                bricks=brick_list, force=False)
        _volume_create.assert_called_with(
            'test',
            {
                volume.VolumeTranslator.Stripe: '1',
                volume.VolumeTranslator.Replica: '3'
            }, volume.Transport.Tcp,
            brick_list, False)

    @mock.patch('lib.gluster.volume.run_command')
    def testVolumeDelete(self, _run_command):
        _run_command.return_value = Ok("")
        volume.volume_delete("test")
        _run_command.assert_called_with("gluster",
                                        ["volume", "delete", "test"],
                                        True, True)

    @mock.patch('lib.gluster.volume.run_command')
    def testVolumeDisableBitrot(self, _run_command):
        _run_command.return_value = Ok("")
        volume.volume_disable_bitrot("test")
        _run_command.assert_called_with("gluster",
                                        ["volume", "bitrot", "test",
                                         "disable"],
                                        True, False)

    @mock.patch('lib.gluster.volume.run_command')
    def testVolumeDisableQuotas(self, _run_command):
        _run_command.return_value = Ok("")
        volume.volume_disable_quotas("test")
        _run_command.assert_called_with("gluster",
                                        ["volume", "quota", "test",
                                         "disable"],
                                        True, False)

    @mock.patch('lib.gluster.volume.run_command')
    def testVolumeEnableBitrot(self, _run_command):
        _run_command.return_value = Ok("")
        volume.volume_enable_bitrot("test")
        _run_command.assert_called_with("gluster",
                                        ["volume", "bitrot", "test",
                                         "enable"],
                                        True, False)

    def testVolumeSetBitrotOption(self):
        pass

    @mock.patch('lib.gluster.volume.run_command')
    def testVolumeEnableQuotas(self, _run_command):
        _run_command.return_value = Ok("")
        volume.volume_enable_quotas("test")
        _run_command.assert_called_with("gluster",
                                        ["volume", "quota", "test",
                                         "enable"],
                                        True, False)

    @mock.patch('lib.gluster.volume.run_command')
    def testVolumeRebalance(self, _run_command):
        _run_command.return_value = Ok("")
        volume.volume_rebalance("test")
        _run_command.assert_called_with("gluster",
                                        ["volume", "rebalance", "test",
                                         "start"],
                                        True, True)

    def testVolumeRemoveBrick(self):
        pass

    @mock.patch('lib.gluster.volume.run_command')
    def testVolumeRemoveQuota(self, _run_command):
        _run_command.return_value = Ok("")
        volume.volume_remove_quota("test", "/path1")
        _run_command.assert_called_with("gluster",
                                        ["volume", "quota", "test",
                                         "remove", "/path1"],
                                        True, False)

    @mock.patch('lib.gluster.volume.run_command')
    def testVolSet(self, _run_command):
        volume.vol_set("test",
                       volume.GlusterOption(
                           option=volume.GlusterOption.AuthAllow,
                           value="*"))
        _run_command.assert_called_with("gluster",
                                        ["volume", "set", "test", "auth.allow",
                                         "*"], True, True)

    def testVolumeSetOptions(self):
        pass

    @mock.patch('lib.gluster.volume.run_command')
    def testVolumeStart(self, _run_command):
        _run_command.return_value = Ok("")
        volume.volume_start("test", True)
        _run_command.assert_called_with("gluster",
                                        ["volume", "start", "test", "force"],
                                        True, True)

    @mock.patch('lib.gluster.volume.run_command')
    def testVolumeStop(self, _run_command):
        _run_command.return_value = Ok("")
        volume.volume_stop("test", True)
        _run_command.assert_called_with("gluster",
                                        ["volume", "stop", "test", "force"],
                                        True, True)

    @mock.patch('lib.gluster.volume.parse_volume_status')
    @mock.patch('lib.gluster.volume.run_command')
    def testVolumeStatus(self, _run_command, _parse_volume_status):
        _run_command.return_value = Ok("")
        _parse_volume_status.return_value = Ok("")
        volume.volume_status("test")
        _run_command.assert_called_with("gluster", ["vol", "status", "test",
                                                    "--xml"], True, False)

    def testParseVolumeInfo(self):
        with open('unit_tests/vol_info.xml', 'r') as xml_output:
            lines = xml_output.readlines()
            results = volume.parse_volume_info("".join(lines))
            self.assertTrue(results.is_ok())
            # for vol in results.value:
            # print("parse volume info: {}".format(vol))
            # for brick in vol.bricks:
            # print("brick: {}".format(brick))


if __name__ == "__main__":
    unittest.main()
