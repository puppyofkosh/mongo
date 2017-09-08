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
from . import single_jstest
from ... import config
from ... import core
from ... import utils
from ...utils import queue as _queue
from ...utils import registry


class MultipleCopyJSTestCase(interface.TestCase):
    """
    A wrapper for several jstests to execute.
    """

    REGISTERED_NAME = "js_test"
    DEFAULT_CLIENT_NUM = 1

    # A wrapper for the thread class that lets us propagate exceptions.
    class ExceptionThread(threading.Thread):
        def __init__(self, my_target, my_args):
            threading.Thread.__init__(self, target=my_target, args=my_args)
            self.err = None

        def run(self):
            try:
                threading.Thread.run(self)
            except:
                self.err = sys.exc_info()[1]
            else:
                self.err = None

        def _get_exception(self):
            return self.err

    def __init__(self,
                 logger,
                 js_filename,
                 shell_executable=None,
                 shell_options=None,
                 test_kind="JSTest"):
        """Initializes the MultipleCopyJSTestCase with the JS file to run."""

        interface.TestCase.__init__(self, logger, test_kind, js_filename)

        # Command line options override the YAML configuration.
        self.shell_executable = utils.default_if_none(config.MONGO_EXECUTABLE, shell_executable)

        self.js_filename = js_filename
        self.shell_options = utils.default_if_none(shell_options, {}).copy()
        self.num_clients = MultipleCopyJSTestCase.DEFAULT_CLIENT_NUM

        self.single_test_case = single_jstest.SingleJSTestCase(self.logger, self.js_filename,
                                                               self.shell_executable,
                                                               self.shell_options)

    def configure(self, fixture, num_clients=DEFAULT_CLIENT_NUM, *args, **kwargs):
        interface.TestCase.configure(self, fixture, *args, **kwargs)

        global_vars = self.shell_options.get("global_vars", {}).copy()
        data_dir = self._get_data_dir(global_vars)

        # Set MongoRunner.dataPath if overridden at command line or not specified in YAML.
        if config.DBPATH_PREFIX is not None or "MongoRunner.dataPath" not in global_vars:
            # dataPath property is the dataDir property with a trailing slash.
            data_path = os.path.join(data_dir, "")
        else:
            data_path = global_vars["MongoRunner.dataPath"]

        global_vars["MongoRunner.dataDir"] = data_dir
        global_vars["MongoRunner.dataPath"] = data_path

        # Don't set the path to the executables when the user didn't specify them via the command
        # line. The functions in the mongo shell for spawning processes have their own logic for
        # determining the default path to use.
        if config.MONGOD_EXECUTABLE is not None:
            global_vars["MongoRunner.mongodPath"] = config.MONGOD_EXECUTABLE
        if config.MONGOS_EXECUTABLE is not None:
            global_vars["MongoRunner.mongosPath"] = config.MONGOS_EXECUTABLE
        if self.shell_executable is not None:
            global_vars["MongoRunner.mongoShellPath"] = self.shell_executable

        test_data = global_vars.get("TestData", {}).copy()
        test_data["minPort"] = core.network.PortAllocator.min_test_port(fixture.job_num)
        test_data["maxPort"] = core.network.PortAllocator.max_test_port(fixture.job_num)

        global_vars["TestData"] = test_data
        self.shell_options["global_vars"] = global_vars

        shutil.rmtree(data_dir, ignore_errors=True)

        if num_clients < 1:
            raise RuntimeError("Must have at least 1 client")
        self.num_clients = num_clients

        try:
            os.makedirs(data_dir)
        except os.error:
            # Directory already exists.
            pass

        process_kwargs = self.shell_options.get("process_kwargs", {}).copy()

        if "KRB5_CONFIG" in process_kwargs and "KRB5CCNAME" not in process_kwargs:
            # Use a job-specific credential cache for JavaScript tests involving Kerberos.
            krb5_dir = os.path.join(data_dir, "krb5")
            try:
                os.makedirs(krb5_dir)
            except os.error:
                pass
            process_kwargs["KRB5CCNAME"] = "DIR:" + os.path.join(krb5_dir, ".")

        self.shell_options["process_kwargs"] = process_kwargs

        single_test_case_shell_options = self._get_shell_options_for_thread(0)
        self.single_test_case.configure(self.fixture, self.shell_executable,
                                        single_test_case_shell_options,
                                        num_clients)

    def _get_data_dir(self, global_vars):
        """
        Returns the value that the mongo shell should set for the
        MongoRunner.dataDir property.
        """

        # Command line options override the YAML configuration.
        data_dir_prefix = utils.default_if_none(config.DBPATH_PREFIX,
                                                global_vars.get("MongoRunner.dataDir"))
        data_dir_prefix = utils.default_if_none(data_dir_prefix, config.DEFAULT_DBPATH_PREFIX)
        return os.path.join(data_dir_prefix,
                            "job%d" % (self.fixture.job_num),
                            config.MONGO_RUNNER_SUBDIR)

    def _make_process(self):
        # This function should only be called by interface.py's as_command().
        return self.single_test_case._make_process()

    def _get_shell_options_for_thread(self, thread_id):
        """Get shell_options with an initialized TestData object for given thread"""

        # We give each JSSingleTestCase its own copy of the shell_options.
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

        return shell_options

    def _create_single_test_case_for_thread(self, thread_id):
        """Create and configure a SingleJSTestCase to be run in a separate thread"""

        logger = self.logger.new_test_thread_logger(self.test_kind, str(thread_id))
        shell_options = self._get_shell_options_for_thread(thread_id)

        test_case = single_jstest.SingleJSTestCase(logger, self.js_filename, self.shell_executable,
                                                   shell_options)
        test_case.configure(self.fixture, self.shell_executable, shell_options,
                            self.num_clients, thread_id)

        return test_case

    def run_test(self):
        threads = []
        test_cases_run = []
        try:
            # Don't thread if there is only one client.
            if self.num_clients == 1:
                # Just run the one test case in this thread.
                test_cases_run.append(self.single_test_case)
                self.single_test_case.run_test()
            else:
                # If there are multiple clients, make a new thread for each client.
                for thread_id in xrange(self.num_clients):
                    test_case = self._create_single_test_case_for_thread(thread_id)
                    test_cases_run.append(test_case)

                    t = self.ExceptionThread(my_target=test_case.run_test, my_args=[])
                    t.start()
                    threads.append(t)
        except self.failureException:
            raise
        except:
            self.logger.exception("Encountered an error running jstest %s.", self.basename())
            raise
        finally:
            for t in threads:
                t.join()

            # Go through each test's return code and store the first nonzero one if it exists.
            self.return_code = 0
            for test_case in test_cases_run:
                if test_case.return_code != 0:
                    self.return_code = test_case.return_code
                    break

            for t in threads:
                if t._get_exception() is not None:
                    raise t._get_exception()
