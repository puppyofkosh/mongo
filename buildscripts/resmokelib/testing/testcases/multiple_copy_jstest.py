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
from ...utils import registry


class MultipleCopyJSTestCase(interface.TestCase):
    """
    A jstest to execute.
    """

    REGISTERED_NAME = "js_test"

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

    DEFAULT_CLIENT_NUM = 1

    def __init__(self,
                 logger,
                 js_filename,
                 shell_executable=None,
                 shell_options=None,
                 test_kind="JSTest"):
        """Initializes the MultipleCopyJSTestCase with the JS file to run."""

        interface.TestCase.__init__(self, logger, test_kind, js_filename)

        # FIXME: Remove
        print("Creating multi copy JS Test case")

        # Command line options override the YAML configuration.
        self.shell_executable = utils.default_if_none(config.MONGO_EXECUTABLE, shell_executable)

        self.js_filename = js_filename
        self.shell_options = utils.default_if_none(shell_options, {}).copy()
        self.num_clients = MultipleCopyJSTestCase.DEFAULT_CLIENT_NUM
        self.test_cases = []

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

        if num_clients < 1:
            raise RuntimeError("Must have at least 1 client")

        # Initialize the individual test cases
        for _ in xrange(num_clients):
            # TODO: logger should depend on num_clients.
            # If num_clients is 1 use self.logger otherwise use the other one.
            
            test = single_jstest.SingleJSTestCase(self.logger, self.js_filename,
                                              self.shell_executable, self.shell_options)
            test.configure(self.fixture, self.shell_executable, self.shell_options)
            self.test_cases.append(test)

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
        if len(self.test_cases) > 0:
            return self.test_cases[0]._make_process()
        else:
            return None

    def run_test(self):
        print("Running multi copy jstest")
        threads = []
        try:
            # Don't thread if there is only one client.
            if len(self.test_cases) == 1:
                # Just run the one test case in this thread.
                self.test_cases[0].run_test()

                # FIXME Must do this for both cases
                self.return_code = self.test_cases[0]
            else:
                raise RuntimeError("nyi")
                # If there are multiple clients, make a new thread for each client.
                for i in xrange(self.num_clients):
                    t = self.ExceptionThread(my_target=self._run_test_in_thread, my_args=[i])
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
            for t in threads:
                if t._get_exception() is not None:
                    raise t._get_exception()
            print("run_test finished")
