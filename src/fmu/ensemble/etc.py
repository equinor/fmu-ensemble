"""This module is deprecated and will be removed in fmu-ensemble v2.0.0"""

import os
import sys
import warnings
import inspect
import logging
import timeit

# pylint: disable=protected-access

warnings.filterwarnings(
    action="always",
    category=DeprecationWarning,
    module=r"etc|ensemble.etc|fmu.ensemble.etc",
)


class _BColors(object):
    # local class for ANSI term color commands
    # bgcolors:
    # 40=black, 41=red, 42=green, 43=yellow, 44=blue, 45=pink, 46 cyan

    # pylint: disable=too-few-public-methods
    HEADER = "\033[1;96m"
    OKBLUE = "\033[94m"
    OKGREEN = "\033[92m"
    WARN = "\033[93;43m"
    ERROR = "\033[93;41m"
    CRITICAL = "\033[1;91m"
    ENDC = "\033[0m"
    BOLD = "\033[1m"
    UNDERLINE = "\033[4m"

    def __init__(self):
        pass


class Interaction(object):
    """System for handling interaction; dialogues and messages in FMU.

    This module cooperates with the standard Python logging module.
    """

    def __init__(self):
        warnings.warn(
            "fmu.ensemble.etc is deprecated and will be removed in later versions.",
            DeprecationWarning,
        )
        self._callclass = None
        self._caller = None
        self._lformat = None
        self._lformatlevel = 1
        self._logginglevel = "WARNING"
        self._loggingname = ""
        self._syslevel = 1
        self._test_env = True
        self._tmpdir = "TMP"
        self._testpath = None

        # a string, for Python logging:
        logginglevel = os.environ.get("FMU_LOGGING_LEVEL")

        # a number, for format, 1 is simple, 2 is more info etc
        loggingformat = os.environ.get("FMU_LOGGING_FORMAT")

        if logginglevel is not None:
            self.logginglevel = logginglevel

        if loggingformat is not None:
            self._lformatlevel = int(loggingformat)

    @property
    def logginglevel(self):
        """Set or return a logging level property, e.g. logging.CRITICAL"""

        return self._logginglevel

    @logginglevel.setter
    def logginglevel(self, level):
        # pylint: disable=pointless-statement

        validlevels = ("INFO", "WARNING", "DEBUG", "ERROR", "CRITICAL")
        if level in validlevels:
            self._logginglevel = level
        else:
            raise ValueError(
                "Invalid level given, must be " "in {}".format(validlevels)
            )

    @property
    def numericallogginglevel(self):
        """Return a numerical logging level (read only)"""
        llo = logging.CRITICAL
        if self._logginglevel == "INFO":
            llo = logging.INFO
        elif self._logginglevel == "WARNING":
            llo = logging.WARNING
        elif self._logginglevel == "ERROR":
            llo = logging.ERROR
        elif self._logginglevel == "DEBUG":
            llo = logging.DEBUG

        return llo

    @property
    def loggingformatlevel(self):
        """Set logging format (for future use)"""
        return self._lformatlevel

    @property
    def loggingformat(self):
        """Returns the format string to be used in logging"""

        if self._lformatlevel <= 1:
            self._lformat = "%(levelname)8s: \t%(message)s"
        else:
            self._lformat = (
                "%(asctime)s Line: %(lineno)4d %(name)44s "
                + "[%(funcName)40s()]"
                + "%(levelname)8s:"
                + "\t%(message)s"
            )

        return self._lformat

    @property
    def tmpdir(self):
        """Get and set tmpdir for testing"""
        return self._tmpdir

    @tmpdir.setter
    def tmpdir(self, value):
        self._tmpdir = value

    @staticmethod
    def print_fmu_header(appname, appversion, info=None):
        """Prints a banner for a FMU app to STDOUT.

        Args:
            appname (str): Name of application.
            appversion (str): Version of application on form '3.2.1'
            info (str, optional): More info, e.g. if beta release

        Example::

            fmux.print_fmu_header('fmu.ensemble, '0.2.1', info='Beta release!')
        """
        cur_version = "Python " + str(sys.version_info[0]) + "."
        cur_version += str(sys.version_info[1]) + "." + str(sys.version_info[2])

        app = "This is " + appname + ", v. " + str(appversion)
        if info:
            app = app + " (" + info + ")"

        print("")
        print(_BColors.HEADER)
        print("#" * 79)
        print("#{}#".format(app.center(77)))
        print("#{}#".format(cur_version.center(77)))
        print("#" * 79)
        print(_BColors.ENDC)
        print("")

    def basiclogger(self, name, level=None):
        """Initiate the logger by some default settings."""

        if level is not None:
            self.logginglevel = level

        fmt = self.loggingformat
        self._loggingname = name
        logging.basicConfig(format=fmt, stream=sys.stdout)
        logging.getLogger().setLevel(self.numericallogginglevel)  # root logger
        logging.captureWarnings(True)

        return logging.getLogger(name)

    @staticmethod
    def functionlogger(name):
        """Get the logger for functions (not top level)."""

        logger = logging.getLogger(name)
        logger.addHandler(logging.NullHandler())
        return logger

    def testsetup(self, path="TMP"):
        """Basic setup for FMU testing (developer only; relevant for tests)"""

        try:
            os.makedirs(path)
        except OSError:
            if not os.path.isdir(path):
                raise

        self._test_env = True
        self._tmpdir = path
        self._testpath = None

        return True

    @staticmethod
    def timer(*args):
        """Without args; return the time, with a time as arg return the
        difference.

        Example::

            time1 = timer()
            for i in range(10000):
                i = i + 1
            time2 = timer(time1)
            print('Execution took {} seconds'.format(time2)

        """
        time1 = timeit.default_timer()

        if args:
            return time1 - args[0]

        return time1

    def echo(self, string):
        """Show info at runtime (for user scripts)"""
        level = -5
        idx = 3

        caller = sys._getframe(1).f_code.co_name
        frame = inspect.stack()[1][0]
        self.get_callerinfo(caller, frame)

        self._output(idx, level, string)

    def warn(self, string):
        """Show warnings at Runtime (pure user info/warns)."""
        level = 0
        idx = 6

        caller = sys._getframe(1).f_code.co_name
        frame = inspect.stack()[1][0]
        self.get_callerinfo(caller, frame)

        self._output(idx, level, string)

    warning = warn

    def error(self, string):
        """Issue an error, will not exit system by default"""
        level = -8
        idx = 8

        caller = sys._getframe(1).f_code.co_name
        frame = inspect.stack()[1][0]
        self.get_callerinfo(caller, frame)

        self._output(idx, level, string)

    def critical(self, string, sysexit=True):
        """Issue a critical error, default is SystemExit."""
        level = -9
        idx = 9

        caller = sys._getframe(1).f_code.co_name
        frame = inspect.stack()[1][0]
        self.get_callerinfo(caller, frame)

        self._output(idx, level, string)
        if sysexit:
            raise SystemExit("STOP!")

    def get_callerinfo(self, caller, frame):
        """Get caller info for logging (developer stuff)"""
        the_class = self._get_class_from_frame(frame)

        # just keep the last class element
        xname = str(the_class)
        xname = xname.split(".")
        the_class = xname[-1]

        self._caller = caller
        self._callclass = the_class

        return (self._caller, self._callclass)

    # =========================================================================
    # Private routines
    # =========================================================================

    @staticmethod
    def _get_class_from_frame(frame):
        # Incomplete (need more coffee)
        current = inspect.currentframe()
        outer = inspect.getouterframes(current)
        return outer[0]

    def _output(self, idx, level, string):

        # pylint: disable=too-many-branches

        prefix = ""
        endfix = ""

        if idx == 0:
            prefix = "++"
        elif idx == 1:
            prefix = "**"
        elif idx == 3:
            prefix = ">>"
        elif idx == 6:
            prefix = _BColors.WARN + "##"
            endfix = _BColors.ENDC
        elif idx == 8:
            prefix = _BColors.ERROR + "!#"
            endfix = _BColors.ENDC
        elif idx == 9:
            prefix = _BColors.CRITICAL + "!!"
            endfix = _BColors.ENDC

        prompt = False
        if level <= self._syslevel:
            prompt = True

        if prompt:
            if self._syslevel <= 1:
                print("{} {}{}".format(prefix, string, endfix))
            else:
                ulevel = str(level)
                if level == -5:
                    ulevel = "M"
                if level == -8:
                    ulevel = "E"
                if level == -9:
                    ulevel = "W"
                print(
                    "{0} <{1}> [{2:23s}->{3:>33s}] {4}{5}".format(
                        prefix, ulevel, self._callclass, self._caller, string, endfix
                    )
                )
