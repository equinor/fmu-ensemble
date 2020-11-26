"""Testing incorporation of ecl2df in fmu-ensemble."""

import os
import logging

import pytest

from fmu.ensemble import ScratchEnsemble, ScratchRealization

HAVE_ECL2DF = True
try:
    import ecl2df
except ImportError:
    HAVE_ECL2DF = False

logger = logging.getLogger(__name__)


def test_ecl2df_real():
    """Check that we can utilize ecl2df on single realizations"""

    if not HAVE_ECL2DF:
        pytest.skip()

    if "__file__" in globals():
        # Easen up copying test code into interactive sessions
        testdir = os.path.dirname(os.path.abspath(__file__))
    else:
        testdir = os.path.abspath(".")
    realdir = os.path.join(testdir, "data/testensemble-reek001", "realization-0/iter-0")
    real = ScratchRealization(realdir)

    eclfiles = real.get_eclfiles()
    assert isinstance(eclfiles, ecl2df.EclFiles)
    compdat_df = ecl2df.compdat.df(eclfiles)
    assert not compdat_df.empty
    assert "KH" in compdat_df


def test_reek():
    """Import the reek ensemble and apply ecl2df functions on
    the realizations"""

    if "__file__" in globals():
        testdir = os.path.dirname(os.path.abspath(__file__))
    else:
        testdir = os.path.abspath(".")

    reekens = ScratchEnsemble(
        "reektest", testdir + "/data/testensemble-reek001/" + "realization-*/iter-0"
    )
    if not HAVE_ECL2DF:
        pytest.skip()

    def extract_compdat(kwargs):
        """Callback fnction to extract compdata data using ecl2df
        on a ScratchRealization"""
        eclfiles = kwargs["realization"].get_eclfiles()
        if not eclfiles:
            print(
                "Could not obtain EclFiles object for realization "
                + str(kwargs["realization"].index)
            )
        return ecl2df.compdat.deck2dfs(eclfiles.get_ecldeck())["COMPDAT"]

    allcompdats = reekens.apply(extract_compdat)
    assert not allcompdats.empty
    assert 0 in allcompdats["REAL"]
    assert "KH" in allcompdats
    # Pr. now, only realization-0 has eclipse/include in git


def test_smry_via_ecl2df():
    """Test that we could use ecl2df for smry extraction instead
    of the native code inside fmu-ensemble"""

    def get_smry(kwargs):
        """Callback function to extract smry data using ecl2df on a
        ScratchRealization"""
        eclfiles = kwargs["realization"].get_eclfiles()
        return ecl2df.summary.df(
            eclfiles, time_index=kwargs["time_index"], column_keys=kwargs["column_keys"]
        )

    if "__file__" in globals():
        testdir = os.path.dirname(os.path.abspath(__file__))
    else:
        testdir = os.path.abspath(".")

    reekens = ScratchEnsemble(
        "reektest", testdir + "/data/testensemble-reek001/" + "realization-*/iter-0"
    )
    if not HAVE_ECL2DF:
        pytest.skip()

    callback_smry = reekens.apply(get_smry, column_keys="FOPT", time_index="yearly")
    direct_smry = reekens.get_smry(column_keys="FOPT", time_index="yearly")

    assert callback_smry["FOPT"].sum() == direct_smry["FOPT"].sum()
    assert callback_smry["REAL"].sum() == direct_smry["REAL"].sum()
    # BUG in ecl2df, dates are missing!!
