"""Top-level package for fmu.ensemble"""

from ._theversion import theversion

__version__ = theversion()

del theversion

from .ensemble import use_concurrent  # noqa
from .ensemble import ScratchEnsemble  # noqa
from .realization import ScratchRealization  # noqa
from .ensembleset import EnsembleSet  # noqa
from .virtualrealization import VirtualRealization  # noqa
from .virtualensemble import VirtualEnsemble  # noqa
from .ensemblecombination import EnsembleCombination  # noqa
from .realizationcombination import RealizationCombination  # noqa
from .observations import Observations  # noqa
