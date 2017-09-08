"""
unittest.TestCase for JavaScript tests.
"""

from __future__ import absolute_import

import os
import os.path
import shutil
import sys
import threading

from . import interface
from ... import config
from ... import core
from ... import utils
from ...utils import registry


class SingleJSTestCase(interface.TestCase):
    """
    A single jstest to execute.
    """

    REGISTERED_NAME = registry.LEAVE_UNREGISTERED

    DEFAULT_CLIENT_NUM = 1

    def __init__(self,
                 logger,
                 js_filename,
                 shell_executable=None,
                 shell_options=None,
                 test_kind="SingleJSTest"):
        """Initializes the JSSingleTestCase with the JS file to run."""

        interface.TestCase.__init__(self, logger, test_kind, js_filename)

        # Command line options override the YAML configuration.
        self.shell_executable = utils.default_if_none(config.MONGO_EXECUTABLE, shell_executable)

        self.js_filename = js_filename
        self.shell_options = utils.default_if_none(shell_options, {}).copy()
        self.num_clients = SingleJSTestCase.DEFAULT_CLIENT_NUM

    def configure(self, fixture, shell_executable, shell_options,
                  num_clients=DEFAULT_CLIENT_NUM,
                  *args, **kwargs):
        interface.TestCase.configure(self, fixture, *args, **kwargs)
        self.shell_executable = shell_executable
        self.shell_options = shell_options
        self.num_clients = num_clients

    def run_test(self):
        shell = self._make_process()
        self._execute(shell)

    def _make_process(self):
        return core.programs.mongo_shell_program(
            self.logger,
            executable=self.shell_executable,
            filename=self.js_filename,
            connection_string=self.fixture.get_driver_connection_url(),
            **self.shell_options)
