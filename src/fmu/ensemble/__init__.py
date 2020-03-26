"""Top-level package for fmu.ensemble"""

try:
    from .version import version

    __version__ = version
except ImportError:
    __version__ = "0.0.0"

from .ensemble import ScratchEnsemble  # noqa
from .realization import ScratchRealization  # noqa
from .ensembleset import EnsembleSet  # noqa
from .virtualrealization import VirtualRealization  # noqa
from .virtualensemble import VirtualEnsemble  # noqa
from .ensemblecombination import EnsembleCombination  # noqa
from .realizationcombination import RealizationCombination  # noqa
from .observations import Observations  # noqa
