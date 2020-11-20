"""Module for book-keeping and aggregation of ensembles"""

import re
import os
import glob
import logging

import numpy as np
import pandas as pd

from .ensemble import ScratchEnsemble, VirtualEnsemble

logger = logging.getLogger(__name__)


class EnsembleSet(object):
    """An ensemble set is any collection of ensemble objects

    Ensemble objects are ScratchEnsembles or VirtualEnsembles.

    There is support for initializing from a filstructure with both
    iterations and batches, but the concept of iterations and batches
    are not kept in an EnsembleSet, there each ensemble is uniquely
    identified by the ensemble name. To keep the iteration (and batch)
    concept, that must be embedded into the ensemble name.

    The init method will make an ensemble set, either as empty, or from a
    list of already initialized ensembles, or directly from the
    filesystem, or from an ERT runpath file. Only one of these
    initialization modes can be used.

    Args:
        name: Chosen name for the ensemble set. Can be used if aggregated at a
            higher level.
        ensembles: list of Ensemble objects. Can be omitted.
        frompath: string or list of strings with filesystem path.
            Will be globbed by default. If no realizations or iterations
            are detected after globbing, the standard glob
            'realization-*/iter-*/ will be used.
        runpathfile: string with path to an ert runpath file which will
            be used to lookup realizations and iterations.
        realidxregexp: regular expression object that will be used to
            determine the realization index (must be integer) from a path
            component (split by /). The default fits realization-*
        iterregexp: similar to realidxregexp, and result will always be
            treated as a string.
        batchregexp: similar ot iterregexp, for future support of an extra
            level similar to iterations
        autodiscovery: boolean, sent to initializing Realization objects,
            instructing them on whether certain files should be
            auto-discovered.
        batch (dict): List of functions (load_*) that
            should be run at time of initialization for each realization.
            Each element is a length 1 dictionary with the function name to run as
            the key and each keys value should be the function arguments as a dict.
    """

    def __init__(
        self,
        name=None,
        ensembles=None,
        frompath=None,
        runpathfile=None,
        realidxregexp=None,
        iterregexp=None,
        batchregexp=None,
        autodiscovery=True,
        batch=None,
    ):
        self._name = name
        self._ensembles = {}  # Dictionary indexed by each ensemble's name.

        if (ensembles and frompath) or (ensembles and runpathfile):
            logger.error(
                (
                    "EnsembleSet only supports one initialization mode,"
                    "from list of ensembles\n, list of paths or "
                    "an ert runpath file"
                )
            )
            raise ValueError

        # Check consistency in arguments.
        if not name:
            logger.warning("EnsembleSet name defaulted to 'ensembleset'")
            name = "ensembleset"
            self._name = name
        if name and not isinstance(name, str):
            logger.error("Name of EnsembleSet must be a string")
            return
        if frompath and not isinstance(frompath, str):
            logger.error("frompath arg given to EnsembleSet must be a string")
            return
        if ensembles and not isinstance(ensembles, list):
            logger.error("Ensembles supplied to EnsembleSet must be a list")
            return

        if ensembles and isinstance(ensembles, list):
            if batch:
                logger.warning(
                    "Batch commands not procesed when loading finished ensembles"
                )
            for ensemble in ensembles:
                if isinstance(ensemble, (ScratchEnsemble, VirtualEnsemble)):
                    self._ensembles[ensemble.name] = ensemble
                else:
                    logger.warning("Supplied object was not an ensemble")
            if not self._ensembles:
                logger.warning("No ensembles added to EnsembleSet")
        if frompath:
            self.add_ensembles_frompath(
                frompath,
                realidxregexp,
                iterregexp,
                batchregexp,
                autodiscovery=autodiscovery,
                batch=batch,
            )
            if not self._ensembles:
                logger.warning("No ensembles added to EnsembleSet")

        if runpathfile:
            if not os.path.exists(runpathfile):
                logger.error("Could not open runpath file %s", runpathfile)
                raise IOError
            self.add_ensembles_fromrunpath(runpathfile, batch=batch)
            if not self._ensembles:
                logger.warning("No ensembles added to EnsembleSet")

    @property
    def name(self):
        """Return the name of the ensembleset,
        as initialized"""
        return self._name

    def __len__(self):
        return len(self._ensembles)

    def __getitem__(self, name):
        return self._ensembles[name]

    def __repr__(self):
        return "<EnsembleSet {}, {} ensembles:\n{}>".format(
            self.name, len(self), self._ensembles
        )

    @property
    def ensemblenames(self):
        """
        Return a list of named ensembles in this set
        """
        return list(self._ensembles.keys())

    def keys(self):
        """
        Return the union of all keys available in the ensembles.

        Keys refer to the realization datastore, a dictionary
        of dataframes or dicts.
        """
        allkeys = set()
        for ensemble in self._ensembles.values():
            allkeys = allkeys.union(ensemble.keys())
        return allkeys

    def add_ensembles_frompath(
        self,
        paths,
        realidxregexp=None,
        iterregexp=None,
        batchregexp=None,
        autodiscovery=True,
        batch=None,
    ):
        """Convenience function for adding multiple ensembles.

        Args:
            paths: str or list of strings with path to the
                directory containing the realization-*/iter-*
                structure
            realidxregexp: Supply a regexp that can extract the realization
                index as an *integer* from path components.
                The expression will be tested on individual path
                components from right to left.
            iterregexp: Similar to real_regexp, but is allowed to
                match strings.
            batchregexp: Similar to real_regexp, but is allowed to
                match strings.
            autodiscovery: boolean, sent to initializing Realization objects,
                instructing them on whether certain files should be
                auto-discovered.
            batch (dict): List of functions (load_*) that
                should be run at time of initialization for each realization.
                Each element is a length 1 dictionary with the function name to run as
                the key and each keys value should be the function arguments as a dict.
        """
        # Try to catch the most common use case and make that easy:
        if isinstance(paths, str):
            if (
                "realization" not in paths
                and not realidxregexp
                and not iterregexp
                and not batchregexp
            ):
                logger.info(
                    "Adding realization-*/iter-* path pattern to case directory"
                )
                paths = paths + "/realization-*/iter-*"
            paths = [paths]

        if not realidxregexp:
            realidxregexp = re.compile(r"realization-(\d+)")
        if isinstance(realidxregexp, str):
            realidxregexp = re.compile(realidxregexp)
        if not iterregexp:
            # Alternative regexp that extracts iteration
            # as an integer
            # iterregexp = re.compile(r'iter-(\d+)')
            # Default regexp that will add 'iter-' to the
            # ensemble name
            iterregexp = re.compile(r"(iter-\d+)")
        if isinstance(iterregexp, str):
            iterregexp = re.compile(iterregexp)
        if not batchregexp:
            batchregexp = re.compile(r"batch-(\d+)")
        if isinstance(batchregexp, str):
            batchregexp = re.compile(batchregexp)

        # Check that the regexpes actually can return something
        if realidxregexp.groups != 1:
            logger.critical("Invalid regular expression for realization")
            return
        if iterregexp.groups != 1:
            logger.critical("Invalid regular expression for iter")
            return
        if batchregexp.groups != 1:
            logger.critical("Invalid regular expression for batch")
            return

        globbedpaths = [glob.glob(path) for path in paths]
        globbedpaths = list({item for sublist in globbedpaths for item in sublist})

        # Build a temporary dataframe of globbed paths, and columns with
        # the realization index and the iter we found
        # (extented to a third level called 'batch')
        paths_df = pd.DataFrame(columns=["path", "real", "iter", "batch"])
        for path in globbedpaths:
            real = None
            iterr = None  # 'iter' is a builtin..
            batchname = None
            for path_comp in reversed(path.split(os.path.sep)):
                realmatch = re.match(realidxregexp, path_comp)
                if realmatch:
                    real = int(realmatch.group(1))
                    break
            for path_comp in reversed(path.split(os.path.sep)):
                itermatch = re.match(iterregexp, path_comp)
                if itermatch:
                    iterr = str(itermatch.group(1))
                    break
            for path_comp in reversed(path.split(os.path.sep)):
                batchmatch = re.match(batchregexp, path_comp)
                if batchmatch:
                    batchname = str(itermatch.group(1))
                    break
            df_row = {"path": path, "real": real, "iter": iterr, "batch": batchname}
            paths_df = paths_df.append(df_row, ignore_index=True)
        paths_df.fillna(value="Unknown", inplace=True)
        # Initialize ensemble objects for each iter found:
        iters = sorted(paths_df["iter"].unique())
        logger.info("Identified %s iterations, %s", len(iters), iters)
        for iterr in iters:
            # The realization indices *must* be unique for these
            # chosen paths, otherwise we are most likely in
            # trouble
            iterslice = paths_df[paths_df["iter"] == iterr]
            if len(iterslice["real"].unique()) != len(iterslice):
                logger.error("Repeated realization indices for iter %s", iterr)
                logger.error("Some realizations will be ignored")
            pathsforiter = sorted(paths_df[paths_df["iter"] == iterr]["path"].values)
            # iterr might contain the 'iter-' prefix,
            # depending on chosen regexpx
            ens = ScratchEnsemble(
                str(iterr),
                pathsforiter,
                realidxregexp=realidxregexp,
                autodiscovery=autodiscovery,
                batch=batch,
            )
            self._ensembles[ens.name] = ens

    def add_ensembles_fromrunpath(self, runpathfile, batch=None):
        """Add one or many ensembles from an ERT runpath file.

        autodiscovery is not an argument, it is by default set to False
        for runpath-files, since the location of the UNSMRY-file is given in
        the runpath file.
        """
        runpath_df = pd.read_csv(
            runpathfile,
            sep=r"\s+",
            engine="python",
            names=["index", "runpath", "eclbase", "iter"],
        )
        # If index and iter columns are all integers (typically zero padded),
        # Pandas has converted them to int64. If not, they will be
        # strings (objects)
        for iterr in runpath_df["iter"].unique():
            # Make a runpath slice, and initialize from that:
            ens_runpath = runpath_df[runpath_df["iter"] == iterr]
            ens = ScratchEnsemble(
                "iter-" + str(iterr),
                runpathfile=ens_runpath,
                autodiscovery=False,
                batch=batch,
            )
            self._ensembles[ens.name] = ens

    def add_ensemble(self, ensembleobject):
        """Add a single ensemble to the ensemble set

        Name is taken from the ensembleobject.
        """
        if ensembleobject.name in self._ensembles:
            raise ValueError(
                "The name {} already exists in the EnsembleSet".format(
                    ensembleobject.name
                )
            )
        self._ensembles[ensembleobject.name] = ensembleobject

    @property
    def parameters(self):
        """Build a dataframe of the information in each
        realizations parameters.txt.

        If no realizations have the file, an empty dataframe is returned.

        Returns:
            pd.DataFrame
        """
        try:
            return self.get_df("parameters.txt")
        except KeyError:
            return pd.DataFrame()

    def load_scalar(self, localpath, convert_numeric=False, force_reread=False):
        """Parse a single value from a file

        The value can be a string or a number. Empty files
        are treated as existing, with an empty string as
        the value, different from non-existing files.

        Parsing is performed individually in each ensemble
        and realization"""
        for ensname, ensemble in self._ensembles.items():
            try:
                ensemble.load_scalar(localpath, convert_numeric, force_reread)
            except ValueError:
                # This will occur if an ensemble is missing the file.
                # At ensemble level that is an Error, but at EnsembleSet level
                # it is only a warning.
                logger.warning(
                    "Ensemble %s did not contain the data %s", ensname, localpath
                )

    def load_txt(self, localpath, convert_numeric=True, force_reread=False):
        """Parse and internalize a txt-file from disk

        Parses text files on the form
        <key> <value>
        in each line."""
        return self.load_file(localpath, "txt", convert_numeric, force_reread)

    def load_csv(self, localpath, convert_numeric=True, force_reread=False):
        """Parse and internalize a CSV file from disk"""
        return self.load_file(localpath, "csv", convert_numeric, force_reread)

    def load_file(self, localpath, fformat, convert_numeric=True, force_reread=False):
        """Internal function for load_*()"""
        for ensname, ensemble in self._ensembles.items():
            try:
                ensemble.load_file(localpath, fformat, convert_numeric, force_reread)
            except (KeyError, ValueError):
                # This will occur if an ensemble is missing the file.
                # At ensemble level that is an Error, but at EnsembleSet level
                # it is only a warning.
                logger.warning(
                    "Ensemble %s did not contain the data %s", ensname, localpath
                )
        return self.get_df(localpath)

    def get_df(self, localpath, merge=None):
        """Collect contents of dataframes from each ensemble

        Args:
            localpath (str): path to the text file, relative to each realization
            merge (list or str): refer to additional localpath(s) which will
                be merged into the dataframe for every ensemble/realization.
                Merging happens before aggregation.
        """
        ensdflist = []
        for _, ensemble in self._ensembles.items():
            try:
                ensdf = ensemble.get_df(localpath, merge=merge)
                ensdf.insert(0, "ENSEMBLE", ensemble.name)
                ensdflist.append(ensdf)
            except (KeyError, ValueError):
                # Happens if an ensemble is missing some data
                # Warning has already been issued at initialization
                pass
        if ensdflist:
            return pd.concat(ensdflist, sort=False)
        raise KeyError("No data found for {} or merge failed".format(localpath))

    def drop(self, localpath, **kwargs):
        """Delete elements from internalized data.

        Shortcuts are allowed for localpath. If the data pointed to is
        a DataFrame, you can delete columns, or rows containing certain
        elements

        If the data pointed to is a dictionary, keys can be deleted.

        Args:
            localpath: string, path to internalized data. If no other options
                are supplied, that dataset is deleted in its entirety
            column: string with a column name to drop. Only for dataframes
            columns: list of strings with column names to delete
            rowcontains: rows where one column contains this string will be
                dropped. The comparison is on strings only, and all cells in
                the dataframe is converted to strings for the comparison.
                Thus it might work on dates, but be careful with numbers.
            key: string with a keyname in a dictionary. Will not work for
                dataframes
            keys: list of strings of keys to delete from a dictionary
        """
        if self.shortcut2path(localpath) not in self.keys():
            raise ValueError("%s not found" % localpath)
        for _, ensemble in self._ensembles.items():
            try:
                ensemble.drop(localpath, **kwargs)
            except ValueError:
                pass  # Allow localpath to be missing in some ensembles.

    def remove_data(self, localpaths):
        """Remove certain datatypes from each ensembles/realizations
        datastores. This modifies the underlying realization
        objects, and is equivalent to

            >>> del realization[localpath]

        on each realization in each ensemble.

        Args:
            localpaths (string): Full localpath to
                the data, or list of strings.
        """
        for _, ensemble in self._ensembles.items():
            ensemble.remove_data(localpaths)

    def process_batch(self, batch=None):
        """Process a list of functions to run/apply

        This is equivalent to calling each function individually
        but this enables more efficient concurrency. It is meant
        to be used for functions that modifies the realization
        object, not for functions that returns a dataframe already.

        Args:
            batch (list): Each list element is a dictionary with one key,
                being a function names, value pr key is a dict with keyword
                arguments to be supplied to each function.
        """
        for ensemble in self._ensembles.values():
            if isinstance(ensemble, ScratchEnsemble):
                ensemble.process_batch(batch)

    def apply(self, callback, **kwargs):
        """Callback functionalty, apply a function to every realization

        The supplied function handle will be handed over to each
        underlying ScratchEnsemble object, which in turn will hand it
        over to its realization objects. The function supplied must
        return a Pandas DataFrame. The function can obtain the
        realization object in the kwargs dictionary through the key
        'realization'.

        Any VirtualEnsembles are ignored. Operations on dataframes in
        VirtualEnsembles can be done using the apply() functionality
        in pd.DataFrame

        Args:
            callback: function handle
            kwargs: dictionary where 'realization' and
                'localpath' is reserved, will be forwarded
                to the callbacked function
            localpath: str, optional if the data is to be internalized
                in each realization object.

        Returns:
            pd.DataFrame, aggregated result of the supplied function
                on each realization.

        """
        results = []
        for ens_name, ensemble in self._ensembles.items():
            if isinstance(ensemble, ScratchEnsemble):
                result = ensemble.apply(callback, **kwargs)
                result["ENSEMBLE"] = ens_name
                results.append(result)
        return pd.concat(results, sort=False, ignore_index=True)

    def shortcut2path(self, shortpath):
        """
        Convert short pathnames to fully qualified pathnames
        within the datastore.

        If the fully qualified localpath is
            'share/results/volumes/simulator_volume_fipnum.csv'
        then you can also access this with these alternatives:
         * simulator_volume_fipnum
         * simulator_volume_fipnum.csv
         * share/results/volumes/simulator_volume_fipnum

        but only as long as there is no ambiguity. In case
        of ambiguity, the shortpath will be returned.

        CODE DUPLICATION from realization.py
        """
        basenames = [os.path.basename(key) for key in self.keys()]
        if basenames.count(shortpath) == 1:
            short2path = {os.path.basename(x): x for x in self.keys()}
            return short2path[shortpath]
        noexts = ["".join(x.split(".")[:-1]) for x in self.keys()]
        if noexts.count(shortpath) == 1:
            short2path = {"".join(x.split(".")[:-1]): x for x in self.keys()}
            return short2path[shortpath]
        basenamenoexts = [
            "".join(os.path.basename(x).split(".")[:-1]) for x in self.keys()
        ]
        if basenamenoexts.count(shortpath) == 1:
            short2path = {
                "".join(os.path.basename(x).split(".")[:-1]): x for x in self.keys()
            }
            return short2path[shortpath]
        # If we get here, we did not find anything that
        # this shorthand could point to. Return as is, and let the
        # calling function handle further errors.
        return shortpath

    def get_csv_deprecated(self, filename):
        """Load CSV data from each realization in each
        ensemble, and aggregate.

        Args:
            filename: string, filename local to realization
        Returns:
            dataframe: Merged CSV from each realization.
                Realizations with missing data are ignored.
                Empty dataframe if no data is found
        """
        dflist = []
        for _, ensemble in self._ensembles.items():
            dframe = ensemble.get_csv(filename)
            dframe["ENSEMBLE"] = ensemble.name
            dflist.append(dframe)
        return pd.concat(dflist, sort=False)

    def load_smry(
        self,
        time_index="raw",
        column_keys=None,
        cache_eclsum=True,
        start_date=None,
        end_date=None,
    ):
        """
        Fetch summary data from all ensembles

        Wraps around Ensemble.load_smry() which wraps
        Realization.load_smry(), which wraps ecl.summary.EclSum.pandas_frame()

        The time index is determined at realization level. If you
        ask for 'monthly', you will from each realization get its
        months. At ensemble or ensembleset-level, the number of
        monthly report dates between realization can vary

        The pr. realization results will be cached by each
        realization object, and can be retrieved through get_df().

        Args:
            time_index: list of DateTime if interpolation is wanted
               default is raw, which returns the raw Eclipse report times
               If a string is supplied, that string is attempted used
               via get_smry_dates() in order to obtain a time index.
            column_keys: list of column key wildcards
            cache_eclsum: Boolean for whether we should cache the EclSum
                objects. Set to False if you cannot keep all EclSum files in
                memory simultaneously
            start_date: str or date with first date to include.
                Dates prior to this date will be dropped, supplied
                start_date will always be included. Overridden if time_index
                is 'first' or 'last'.
            end_date: str or date with last date to be included.
                Dates past this date will be dropped, supplied
                end_date will always be included. Overridden if time_index
                is 'first' or 'last'.

        Returns:
            A DataFame of summary vectors for the ensembleset.
            The column 'ENSEMBLE' will denote each ensemble's name
        """
        # Future: Multithread this:
        for _, ensemble in self._ensembles.items():
            ensemble.load_smry(
                time_index=time_index,
                column_keys=column_keys,
                cache_eclsum=cache_eclsum,
                start_date=start_date,
                end_date=end_date,
            )
        if isinstance(time_index, (list, np.ndarray)):
            time_index = "custom"
        elif time_index is None:
            time_index = "raw"
        return self.get_df("share/results/tables/unsmry--" + time_index + ".csv")

    def get_smry(
        self,
        time_index=None,
        column_keys=None,
        cache_eclsum=False,
        start_date=None,
        end_date=None,
    ):
        """Aggregates summary data from all ensembles

        Wraps around Ensemble.get_smry(), which wraps around
        Realization.get_smry() which wraps around
        ecl.summary.EclSum.pandas_frame()

        Args:
            time_index: list of DateTime if interpolation is wanted
               default is None, which returns the raw Eclipse report times
               If a string is supplied, that string is attempted used
               via get_smry_dates() in order to obtain a time index.
            column_keys: list of column key wildcards
            cache_eclsum: boolean for whether to cache the EclSum
                objects. Defaults to False. Set to True if
                there is enough memory to keep all realizations summary
                files in memory at once. This will speed up subsequent
                operations
            start_date: str or date with first date to include.
                Dates prior to this date will be dropped, supplied
                start_date will always be included. Overridden if time_index
                is 'first' or 'last'.
            end_date: str or date with last date to be included.
                Dates past this date will be dropped, supplied
                end_date will always be included. Overridden if time_index
                is 'first' or 'last'.
        Returns:
            A DataFame of summary vectors for the EnsembleSet. The column
            ENSEMBLE will distinguish the different ensembles by their
            respective names.
        """
        smrylist = []
        for _, ensemble in self._ensembles.items():
            smry = ensemble.get_smry(
                time_index, column_keys, cache_eclsum, start_date, end_date
            )
            smry.insert(0, "ENSEMBLE", ensemble.name)
            smrylist.append(smry)
        if smrylist:
            return pd.concat(smrylist, sort=False)
        return pd.DataFrame()

    def get_smry_dates(
        self, freq="monthly", cache_eclsum=True, start_date=None, end_date=None
    ):
        """Return list of datetimes from an ensembleset

        Datetimes from each realization in each ensemble can
        be returned raw, or be resampled.

        Args:
           freq: string denoting requested frequency for
               the returned list of datetime. 'report' will
               yield the sorted union of all valid timesteps for
               all realizations. Other valid options are
               'daily', 'monthly' and 'yearly'.
            cache_eclsum: Boolean for whether we should cache the EclSum
                objects. Set to False if you cannot keep all EclSum files in
                memory simultaneously
            start_date: str or date with first date to include.
                Dates prior to this date will be dropped, supplied
                start_date will always be included. Overridden if time_index
                is 'first' or 'last'.
            end_date: str or date with last date to be included.
                Dates past this date will be dropped, supplied
                end_date will always be included. Overridden if time_index
                is 'first' or 'last'.
        Returns:
            list of datetime.date.
        """

        rawdates = set()
        for _, ensemble in self._ensembles.items():
            rawdates = rawdates.union(
                ensemble.get_smry_dates(
                    freq="report",
                    cache_eclsum=cache_eclsum,
                    start_date=start_date,
                    end_date=end_date,
                )
            )
        rawdates = list(rawdates)
        rawdates.sort()
        if freq == "report":
            return rawdates
        # Later optimization: Wrap eclsum.start_date in the
        # ensemble object.
        start_date = min(rawdates)
        end_date = max(rawdates)
        pd_freq_mnenomics = {"monthly": "MS", "yearly": "YS", "daily": "D"}
        if freq not in pd_freq_mnenomics:
            raise ValueError("Requested frequency %s not supported" % freq)
        datetimes = pd.date_range(start_date, end_date, freq=pd_freq_mnenomics[freq])
        # Convert from Pandas' datetime64 to datetime.date:
        return [x.date() for x in datetimes]

    def get_wellnames(self, well_match=None):
        """Return a union of all Eclipse summary well names in all ensembles
        realizations (union).

        Optionally, the well names can be filtered.

        Args:
            well_match: `Optional`. String (or list of strings)
               with wildcard filter (globbing). If None, all wells ar
               returned. Empty string will not match anything.
        Returns:
            list of strings with eclipse well names. Empty list if no
            summary file or no matched well names.

        """
        result = set()
        for _, ensemble in self._ensembles.items():
            result = result.union(ensemble.get_wellnames(well_match))
        return sorted(list(result))
