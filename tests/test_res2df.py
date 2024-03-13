"""Testing incorporation of res2df in fmu-ensemble."""

import os
import logging

import pytest

from fmu.ensemble import ScratchEnsemble, ScratchRealization

HAVE_RES2DF = True
try:
    import res2df
except ImportError:
    HAVE_RES2DF = False

logger = logging.getLogger(__name__)


def test_res2df_real():
    """Check that we can utilize res2df on single realizations"""

    if not HAVE_RES2DF:
        pytest.skip()

    if "__file__" in globals():
        # Easen up copying test code into interactive sessions
        testdir = os.path.dirname(os.path.abspath(__file__))
    else:
        testdir = os.path.abspath(".")
    realdir = os.path.join(testdir, "data/testensemble-reek001", "realization-0/iter-0")
    real = ScratchRealization(realdir)

    resdatafiles = real.get_resdatafiles()
    assert isinstance(resdatafiles, res2df.ResdataFiles)
    compdat_df = res2df.compdat.df(resdatafiles)
    assert not compdat_df.empty
    assert "KH" in compdat_df


def test_reek():
    """Import the reek ensemble and apply res2df functions on
    the realizations"""

    if "__file__" in globals():
        testdir = os.path.dirname(os.path.abspath(__file__))
    else:
        testdir = os.path.abspath(".")

    reekens = ScratchEnsemble(
        "reektest", testdir + "/data/testensemble-reek001/" + "realization-*/iter-0"
    )
    if not HAVE_RES2DF:
        pytest.skip()

    def extract_compdat(kwargs):
        """Callback fnction to extract compdata data using res2df
        on a ScratchRealization"""
        resdatafiles = kwargs["realization"].get_resdatafiles()
        if not resdatafiles:
            print(
                "Could not obtain ResdataFiles object for realization "
                + str(kwargs["realization"].index)
            )
        return res2df.compdat.deck2dfs(resdatafiles.get_deck())["COMPDAT"]

    allcompdats = reekens.apply(extract_compdat)
    assert not allcompdats.empty
    assert 0 in allcompdats["REAL"]
    assert "KH" in allcompdats
    # Pr. now, only realization-0 has eclipse/include in git


def test_smry_via_res2df():
    """Test that we could use res2df for smry extraction instead
    of the native code inside fmu-ensemble"""

    def get_smry(kwargs):
        """Callback function to extract smry data using res2df on a
        ScratchRealization"""
        resdatafiles = kwargs["realization"].get_resdatafiles()
        return res2df.summary.df(
            resdatafiles,
            time_index=kwargs["time_index"],
            column_keys=kwargs["column_keys"],
        )

    if "__file__" in globals():
        testdir = os.path.dirname(os.path.abspath(__file__))
    else:
        testdir = os.path.abspath(".")

    reekens = ScratchEnsemble(
        "reektest", testdir + "/data/testensemble-reek001/" + "realization-*/iter-0"
    )
    if not HAVE_RES2DF:
        pytest.skip()

    callback_smry = reekens.apply(get_smry, column_keys="FOPT", time_index="yearly")
    direct_smry = reekens.get_smry(column_keys="FOPT", time_index="yearly")

    assert callback_smry["FOPT"].sum() == direct_smry["FOPT"].sum()
    assert callback_smry["REAL"].sum() == direct_smry["REAL"].sum()
    # BUG in res2df, dates are missing!!
