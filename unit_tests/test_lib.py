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

import mock
import unittest
from lib.gluster import lib
from ipaddress import ip_address
from result import Ok


class TestResolveToIp(unittest.TestCase):
    @mock.patch("lib.gluster.lib.run_command")
    @mock.patch("lib.gluster.lib.ip_address")
    def test(self, _ip_address, _run_command):
        _run_command.return_value = Ok("172.217.3.206\n")
        _ip_address.return_value = ip_address("172.217.3.206")
        result = lib.resolve_to_ip("google.com")
        _ip_address.assert_called_with("172.217.3.206")
        self.assertTrue(result.is_ok())
        self.assertTrue(result.value == ip_address("172.217.3.206"))

    @mock.patch("lib.gluster.lib.get_local_ip")
    def testLocalhost(self, _get_local_ip):
        _get_local_ip.return_value = Ok("192.168.1.2")
        result = lib.resolve_to_ip("localhost")
        self.assertTrue(result.is_ok())
        self.assertTrue(result.value == ip_address("192.168.1.2"))


class TestGetLocalIp(unittest.TestCase):
    @staticmethod
    def run_command_side_effect(*args):
        print("args: {}".format(args))
        if args[1][1] == "show":
            return Ok(
                "default via 192.168.1.1 dev eth1  proto static  metric 100")
        elif args[1][1] == "get":
            return Ok("192.168.1.1 dev eth1  src 192.168.1.6 \n    cache \n")

    @mock.patch("lib.gluster.lib.run_command")
    def testGetLocalIp(self, _run_command):
        _run_command.side_effect = self.run_command_side_effect
        result = lib.get_local_ip()
        self.assertTrue(result.is_ok())
        self.assertTrue(result.value == ip_address("192.168.1.6"))


class TestGetLocalHostName(unittest.TestCase):
    def setUp(self):
        pass

    @mock.patch('builtins.open', mock.mock_open(read_data='1'))
    def test(self, _open):
        # Mock the /etc/hostnames file and assert return.
        _open.return_value = "server001"
        return_value = lib.get_local_hostname()
        self.assertTrue(return_value == "server001")

    def tearDown(self):
        pass


class TestTranslateToBytes(unittest.TestCase):
    def setUp(self):
        self.tests = {
            "1TB": 1099511627776.0,
            "8.2KB": 8396.8,
            "2Bytes": 2.0
        }

    def test(self):
        for test, correct in self.tests.items():
            self.assertEqual(lib.translate_to_bytes(test), correct)

    def tearDown(self):
        pass


if __name__ == "__main__":
    unittest.main()
