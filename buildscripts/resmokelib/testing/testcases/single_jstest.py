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
    A jstest to execute.
    """

    REGISTERED_NAME = registry.LEAVE_UNREGISTERED

    DEFAULT_CLIENT_NUM = 1

    def __init__(self,
                 logger,
                 js_filename,
                 shell_executable=None,
                 shell_options=None,
                 test_kind="SingleJSTest"):
        """Initializes the JSTestCase with the JS file to run."""

        interface.TestCase.__init__(self, logger, test_kind, js_filename)

        # Command line options override the YAML configuration.
        self.shell_executable = utils.default_if_none(config.MONGO_EXECUTABLE, shell_executable)

        self.js_filename = js_filename
        self.shell_options = utils.default_if_none(shell_options, {}).copy()
        self.num_clients = SingleJSTestCase.DEFAULT_CLIENT_NUM

    def configure(self, fixture, shell_executable, shell_options, num_clients,
                  *args, **kwargs):
        interface.TestCase.configure(self, fixture, *args, **kwargs)
        self.shell_executable = shell_executable
        self.shell_options = shell_options
        self.num_clients = num_clients

    def run_test_as_thread(self, thread_id, logger, retcode_queue):
        shell = self._make_process(logger, thread_id)
        self._execute(shell)
        retcode_queue.put(self.return_code)

    def _make_process(self, logger=None, thread_id=0):
        # Since _make_process() is called by each thread, we make a shallow copy of the mongo shell
        # options to avoid modifying the shared options for the JSTestCase.
        shell_options = self.shell_options.copy()
        global_vars = shell_options["global_vars"].copy()
        test_data = global_vars["TestData"].copy()

        # We set a property on TestData to mark the main test when multiple clients are going to run
        # concurrently in case there is logic within the test that must execute only once. We also
        # set a property on TestData to indicate how many clients are going to run the test so they
        # can avoid executing certain logic when there may be other operations running concurrently.
        is_main_test = thread_id == 0
        test_data["isMainTest"] = is_main_test
        test_data["numTestClients"] = self.num_clients
        test_data["threadID"] = thread_id

        global_vars["TestData"] = test_data
        shell_options["global_vars"] = global_vars

        # If logger is none, it means that it's not running in a thread and thus logger should be
        # set to self.logger.
        logger = utils.default_if_none(logger, self.logger)

        return core.programs.mongo_shell_program(
            logger,
            executable=self.shell_executable,
            filename=self.js_filename,
            connection_string=self.fixture.get_driver_connection_url(),
            **shell_options)
