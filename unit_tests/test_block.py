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

from os import path
from reactive import block


class Test(unittest.TestCase):
    def testGetDeviceInfo(self):
        pass

    def testGetJujuBricks(self):
        pass

    def testGetManualBricks(self):
        pass

    @mock.patch('reactive.block.Context')
    def testIsBlockDevice(self, _context):
        device = mock.Mock()
        device.sys_name.return_value = "sda"
        _context.list_devices.return_value = device
        block.is_block_device(path("/dev/sda"))

    def testSetElevator(self):
        pass

    def testScanDevices(self):
        pass

    def testWeeklyDefrag(self):
        pass


if __name__ == "__main__":
    unittest.main()
