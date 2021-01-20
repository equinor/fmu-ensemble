"""Module containing the ScratchEnsemble class"""

import re
import os
import glob
import logging

import dateutil
import pandas as pd
import numpy as np
import yaml
from ecl import EclDataType
from ecl.eclfile import EclKW

from .etc import Interaction  # noqa
from .realization import ScratchRealization
from .virtualrealization import VirtualRealization
from .virtualensemble import VirtualEnsemble
from .ensemblecombination import EnsembleCombination
from .realization import parse_number
from .util import shortcut2path
from .util.dates import unionize_smry_dates

logger = logging.getLogger(__name__)


class ScratchEnsemble(object):
    """An ensemble is a collection of Realizations.

    Ensembles are initialized from path(s) pointing to
    filesystem locations containing realizations.

    Ensemble objects can be grouped into EnsembleSet.

    Realizations in an ensembles are uniquely determined
    by their realization index (integer).

    Example for initialization:

        >>> from fmu import ensemble
        >>> ens = ensemble.ScratchEnsemble('ensemblename',
                    '/scratch/fmu/foobert/r089/casename/realization-*/iter-0')

    Upon initialization, only a subset of the files on
    disk will be discovered. More files must be expliclitly
    discovered and/or loaded.

    Args:
        ensemble_name (str): Name identifier for the ensemble.
            Optional to have it consistent with f.ex. iter-0 in the path.
        paths (list/str): String or list of strings with wildcards
            to file system. Absolute or relative paths.
            If omitted, ensemble will be empty unless runpathfile
            is used.
        realidxregexp (str or regexp): used to deduce the realization index
            from the file path. Default tailored for realization-X
        runpathfile (str): Filename (absolute or relative) of an ERT
            runpath file, consisting of four space separated text fields,
            first column is realization index, second column is absolute
            or relative path to a realization RUNPATH, third column is
            the basename of the Eclipse simulation, relative to RUNPATH.
            Fourth column is not used.
        runpathfilter (str): If supplied, the only the runpaths in
            the runpathfile which contains this string will be included
            Use to select only a specific realization f.ex.
        autodiscovery (boolean): True by default, means that the class
            can try to autodiscover data in the realization. Turn
            off to gain more fined tuned control.
        manifest: dict or filename to use for manifest. If filename,
            it must be a yaml-file that will be parsed to a single dict.
        batch (dict): List of functions (load_*) that
            should be run at time of initialization for each realization.
            Each element is a length 1 dictionary with the function name to run as
            the key and each keys value should be the function arguments as a dict.

    """

    def __init__(
        self,
        ensemble_name,
        paths=None,
        realidxregexp=None,
        runpathfile=None,
        runpathfilter=None,
        autodiscovery=True,
        manifest=None,
        batch=None,
    ):
        self._name = ensemble_name  # ensemble name
        self.realizations = {}  # dict of ScratchRealization objects,
        # indexed by realization indices as integers.
        self._ens_df = pd.DataFrame()
        self._manifest = {}

        self._global_active = None
        self._global_size = None
        self._global_grid = None

        self.obs = None

        if isinstance(paths, str):
            paths = [paths]

        if paths and runpathfile:
            logger.error("Cannot initialize from both path and runpathfile")
            return

        globbedpaths = None
        if isinstance(paths, list):
            # Glob incoming paths to determine
            # paths for each realization (flatten and uniqify)
            globbedpaths = [glob.glob(path) for path in paths]
            globbedpaths = list({item for sublist in globbedpaths for item in sublist})
        if not globbedpaths:
            if isinstance(runpathfile, str):
                if not runpathfile:
                    logger.warning("Initialized empty ScratchEnsemble")
                    return
            if isinstance(runpathfile, pd.DataFrame):
                if runpathfile.empty:
                    logger.warning("Initialized empty ScratchEnsemble")
                    return

        count = None
        if globbedpaths:
            logger.info("Loading ensemble from dirs: %s", " ".join(globbedpaths))

            # Search and locate minimal set of files
            # representing the realizations.
            count = self.add_realizations(
                paths, realidxregexp, autodiscovery=autodiscovery, batch=batch
            )

        if isinstance(runpathfile, str) and runpathfile:
            count = self.add_from_runpathfile(runpathfile, runpathfilter, batch=batch)
        if isinstance(runpathfile, pd.DataFrame) and not runpathfile.empty:
            count = self.add_from_runpathfile(runpathfile, runpathfilter, batch=batch)

        if manifest:
            # The _manifest variable is set using a property decorator
            self.manifest = manifest

        if count:
            logger.info("ScratchEnsemble initialized with %d realizations", count)
        else:
            logger.warning("ScratchEnsemble empty")

    def __getitem__(self, realizationindex):
        """Get one of the ScratchRealization objects.

        Indexed by integers."""
        return self.realizations[realizationindex]

    def keys(self):
        """
        Return the union of all keys available in realizations.

        Keys refer to the realization datastore of internalized
        data. The datastore is a dictionary
        of dataframes or dicts. Examples would be `parameters.txt`,
        `STATUS`, `share/results/tables/unsmry--monthly.csv`
        """
        allkeys = set()
        for realization in self.realizations.values():
            allkeys = allkeys.union(realization.keys())
        return list(allkeys)

    def add_realizations(
        self, paths, realidxregexp=None, autodiscovery=True, batch=None
    ):
        """Utility function to add realizations to the ensemble.

        Realizations are identified by their integer index.
        If the realization index already exists, it will be replaced
        when calling this function.

        This function passes on initialization to ScratchRealization
        and stores a reference to those generated objects.

        Args:
            paths (list/str): String or list of strings with wildcards
                to file system. Absolute or relative paths.
            autodiscovery (boolean): whether files can be attempted
                auto-discovered
            batch (list): Batch commands sent to each realization.

        Returns:
            count (int): Number of realizations successfully added.
        """
        if isinstance(paths, list):
            globbedpaths = [glob.glob(path) for path in paths]
            # Flatten list and uniquify:
            globbedpaths = list({item for sublist in globbedpaths for item in sublist})
        else:
            globbedpaths = glob.glob(paths)

        count = 0
        for realdir in globbedpaths:
            realization = ScratchRealization(
                realdir,
                realidxregexp=realidxregexp,
                autodiscovery=autodiscovery,
                batch=batch,
            )
            if realization.index is None:
                logger.critical(
                    "Could not determine realization index for path %s", realdir
                )
                if not realidxregexp:
                    logger.critical("Maybe you need to supply a regexp.")
                else:
                    logger.critical("Your regular expression is maybe wrong.")
            else:
                count += 1
                self.realizations[realization.index] = realization
        logger.info("add_realizations() found %d realizations", len(self.realizations))
        return count

    def add_from_runpathfile(self, runpath, runpathfilter=None, batch=None):
        """Add realizations from a runpath file typically
        coming from ERT.

        The runpath file is a space separated table with the columns:

          * index - integer with realization index
          * runpath - string with the full path to the realization
          * eclbase - ECLBASE within the runpath (location of DATA file
            minus the trailing '.DATA')
          * iter - integer with the iteration number.

        Args:
            runpath (str): Filename, absolute or relative, or
                a Pandas DataFrame parsed from a runpath file
            runpathfilter (str). A filter which each filepath has to match
                in order to be included. Default None which means not filter
            batch (list): Batch commands to be sent to each realization.

        Returns:
            int: Number of successfully added realizations.
        """
        prelength = len(self)
        if isinstance(runpath, str):
            runpath_df = pd.read_csv(
                runpath,
                sep=r"\s+",
                engine="python",
                names=["index", "runpath", "eclbase", "iter"],
            )
        elif isinstance(runpath, pd.DataFrame):
            # We got a readymade dataframe. Perhaps a slice.
            # Most likely we are getting the slice from an EnsembleSet
            # initialization.
            runpath_df = runpath
            if (
                "index" not in runpath_df
                or "runpath" not in runpath_df
                or "eclbase" not in runpath_df
                or "iter" not in runpath_df
            ):
                raise ValueError("runpath dataframe not correct")

        for _, row in runpath_df.iterrows():
            if runpathfilter and runpathfilter not in row["runpath"]:
                continue
            logger.info("Adding realization from %s", row["runpath"])
            realization = ScratchRealization(
                row["runpath"],
                index=int(row["index"]),
                autodiscovery=False,
                batch=batch,
            )
            # Use the ECLBASE from the runpath file to
            # ensure we recognize the correct UNSMRY file
            realization.find_files(row["eclbase"] + ".DATA")
            realization.find_files(row["eclbase"] + ".UNSMRY")
            self.realizations[int(row["index"])] = realization

        return len(self) - prelength

    def remove_data(self, localpaths):
        """Remove certain datatypes from each realizations
        datastores. This modifies the underlying realization
        objects, and is equivalent to

            >>> del realization[localpath]

        on each realization in the ensemble.

        Args:
            localpaths (string): Full localpaths to
                the data, or list of strings.
        """
        if isinstance(localpaths, str):
            localpaths = [localpaths]
        for localpath in localpaths:
            for _, real in self.realizations.items():
                del real[localpath]

    def remove_realizations(self, realindices):
        """Remove specific realizations from the ensemble

        Args:
            realindices (int or list of ints): The realization
                indices to be removed
        """
        if isinstance(realindices, int):
            realindices = [realindices]
        popped = 0
        for index in realindices:
            self.realizations.pop(index, None)
            popped += 1
        logger.info("removed %d realization(s)", popped)

    def to_virtual(self, name=None):
        """Convert the ScratchEnsemble to a VirtualEnsemble.

        This means that all imported data in each realization is
        aggregated and stored as dataframes in the returned
        VirtualEnsemble

        Unless specified, the VirtualEnsemble object wil
        have the same 'name' as the ScratchEnsemble.

        Args:
            name (str): Name of the ensemble as virtualized.
        """
        if not name:
            name = self._name
        logger.info("Creating virtual ensemble named %s", str(name))
        vens = VirtualEnsemble(name=name, manifest=self.manifest)

        for key in self.keys():
            vens.append(key, self.get_df(key))
        vens.update_realindices()

        # __files is the magic name for the dataframe of
        # loaded files.
        vens.append("__files", self.files)

        # Conserve metadata for smry vectors. Build metadata dict for all
        # loaded summary vectors.
        smrycolumns = [
            vens.get_df(key).columns for key in self.keys() if "unsmry" in key
        ]
        smrycolumns = {smrykey for sublist in smrycolumns for smrykey in sublist}
        # flatten
        meta = self.get_smry_meta(smrycolumns)
        if meta:
            meta_df = pd.DataFrame.from_dict(meta, orient="index")
            meta_df.index.name = "SMRYCOLUMN"
            vens.append("__smry_metadata", meta_df.reset_index())
            # The metadata dictionary is stored as a Dataframe, with one row pr
            # summary key (the index is reset due to code simplifications
            # in to/from_disk)
        return vens

    def to_disk(self, filesystempath, delete=False, dumpcsv=True, dumpparquet=True):
        """Dump ensemble data to a directory on disk.

        The ScratchEnsemble is first converted to a VirtualEnsemble,
        which is then dumped to disk. This function is a
        convenience wrapper for to_disk() in VirtualEnsemble.
        """
        self.to_virtual().to_disk(filesystempath, delete, dumpcsv, dumpparquet)

    @property
    def manifest(self):
        """Get the manifest of the ensemble. The manifest is
        nothing but a Python dictionary with unspecified content

        Returns:
            dict
        """
        return self._manifest

    @manifest.setter
    def manifest(self, manifest):
        """Set the manifest of the ensemble. The manifest
        is nothing but a Python dictionary with unspecified
        content

        Args:
            manifest: dict or str. If dict, it is used as is, if str it
                is assumed to be a filename with YAML syntax which is
                parsed into a dict and stored as dict
        """
        if isinstance(manifest, dict):
            if not manifest:
                logger.warning("Empty manifest")
                self._manifest = {}
            else:
                self._manifest = manifest
        elif isinstance(manifest, str):
            if os.path.exists(manifest):
                with open(manifest) as file_handle:
                    manifest_fromyaml = yaml.safe_load(file_handle)
                if not manifest_fromyaml:
                    logger.warning("Empty manifest")
                    self._manifest = {}
                else:
                    self._manifest = manifest_fromyaml
            else:
                logger.error("Manifest file %s not found", manifest)
        else:
            # NoneType will also end here.
            logger.error("Wrong manifest type supplied")

    @property
    def parameters(self):
        """Build a dataframe of the information in each
        realizations parameters.txt.

        If no realizations have the file, an empty dataframe is returned.

        Returns:
            pd.DataFrame
        """
        try:
            return self.load_txt("parameters.txt")
        except KeyError:
            return pd.DataFrame()

    def load_scalar(self, localpath, convert_numeric=False, force_reread=False):
        """Parse a single value from a file for each realization.

        The value can be a string or a number.

        Empty files are treated as existing, with an empty string as
        the value, different from non-existing files.

        Parsing is performed individually in each realization

        Args:
            localpath (str): path to the text file, relative to each realization
            convert_numeric (boolean): If set to True, assume that
                the value is numerical, and treat strings as
                errors.
            force_reread (boolean): Force reread from file system. If
                False, repeated calls to this function will
                returned cached results.
        Returns:
            pd.DataFrame: Aggregated data over the ensemble. The column 'REAL'
            signifies the realization indices, and a column with the same
            name as the localpath filename contains the data.

        """
        return self.load_file(localpath, "scalar", convert_numeric, force_reread)

    def load_txt(self, localpath, convert_numeric=True, force_reread=False):
        """Parse a key-value text file from disk and internalize data

        Parses text files on the form

            <key> <value>

        in each line.

        Parsing is performed individually in each realization
        """
        return self.load_file(localpath, "txt", convert_numeric, force_reread)

    def load_csv(self, localpath, convert_numeric=True, force_reread=False):
        """For each realization, load a CSV.

        The CSV file must be present in at least one realization.
        The parsing is done individually for each realization, and
        aggregation is on demand (through `get_df()`) and when
        this function returns.

        Args:
            localpath (str): path to the text file, relative to each realization
            convert_numeric (boolean): If set to True, numerical columns
                will be searched for and have their dtype set
                to integers or floats. If scalars, only numerical
                data will be loaded.
            force_reread (boolean): Force reread from file system. If
                False, repeated calls to this function will
                returned cached results.
        Returns:
            pd.Dataframe: aggregation of the loaded CSV files. Column 'REAL'
            distuinguishes each realizations data.
        """
        return self.load_file(localpath, "csv", convert_numeric, force_reread)

    def load_file(self, localpath, fformat, convert_numeric=False, force_reread=False):
        """Function for calling load_file() in every realization

        This function may utilize multithreading.

        Args:
            localpath (str): path to the text file, relative to each realization
            fformat (str): string identifying the file format. Supports 'txt'
                and 'csv'.
            convert_numeric (boolean): If set to True, numerical columns
                will be searched for and have their dtype set
                to integers or floats. If scalars, only numerical
                data will be loaded.
            force_reread (boolean): Force reread from file system. If
                False, repeated calls to this function will
                returned cached results.
        Returns:
            pd.Dataframe: with loaded data aggregated. Column 'REAL'
            distuinguishes each realizations data.
        """
        for index, realization in self.realizations.items():
            try:
                realization.load_file(localpath, fformat, convert_numeric, force_reread)
            except ValueError as exc:
                # This would at least occur for unsupported fileformat,
                # and that we should not skip.
                logger.critical("load_file() failed in realization %d", index)
                raise ValueError from exc
            except IOError:
                # At ensemble level, we allow files to be missing in
                # some realizations
                logger.warning("Could not read %s for realization %d", localpath, index)
        if self.get_df(localpath).empty:
            raise ValueError("No ensemble data found for {}".format(localpath))
        return self.get_df(localpath)

    def find_files(self, paths, metadata=None, metayaml=False):
        """Discover realization files. The files dataframes
        for each realization will be updated.

        Certain functionality requires up-front file discovery,
        e.g. ensemble archiving and ensemble arithmetic.

        CSV files for single use do not have to be discovered.

        Files containing double-dashes '--' indicate that the double
        dashes separate different component with meaning in the
        filename.  The components are extracted and put into
        additional columns "COMP1", "COMP2", etc..
        Filetype extension (after the last dot) will be removed
        from the last component.

        Args:
            paths (str or list of str): Filenames (will be globbed)
                that are relative to the realization directory.
            metadata (dict): metadata to assign for the discovered
                files. The keys will be columns, and its values will be
                assigned as column values for the discovered files.
            metayaml (boolean): Additional possibility of adding metadata from
                associated yaml files. Yaml files to be associated to
                a specific discovered file can have an optional dot in
                front, and must end in .yml, added to the discovered filename.
                The yaml file will be loaded as a dict, and have its keys
                flattened using the separator '--'. Flattened keys are
                then used as column headers in the returned dataframe.

        Returns:
            pd.DataFrame: with the slice of discovered files in each
                realization, tagged with realization index in the column REAL.
                Empty dataframe if no files found.
        """
        df_list = {}
        for index, realization in self.realizations.items():
            df_list[index] = realization.find_files(
                paths, metadata=metadata, metayaml=metayaml
            )
        if df_list:
            return (
                pd.concat(df_list, sort=False)
                .reset_index()
                .rename(columns={"level_0": "REAL"})
                .drop("level_1", axis="columns")
            )
        return pd.DataFrame()

    def __repr__(self):
        return "<ScratchEnsemble {}, {} realizations>".format(self.name, len(self))

    def __len__(self):
        return len(self.realizations)

    def get_smrykeys(self, vector_match=None):
        """
        Return a union of all Eclipse Summary vector names
        in all realizations (union).

        If any requested key/pattern does not match anything, it is
        silently ignored.

        Args:
            vector_match (str or list of str): Wildcards for vectors
               to obtain. If None, all vectors are returned
        Returns:
            list of str: Matched summary vectors. Empty list if no
                summary file or no matched summary file vectors
        """
        if isinstance(vector_match, str):
            vector_match = [vector_match]
        result = set()
        for index, realization in self.realizations.items():
            eclsum = realization.get_eclsum()
            if eclsum:
                if vector_match is None:
                    result = result.union(set(eclsum.keys()))
                else:
                    for vector in vector_match:
                        result = result.union(set(eclsum.keys(vector)))
            else:
                logger.warning("No EclSum available for realization %d", index)
        return list(result)

    def get_smry_meta(self, column_keys=None):
        """
        Provide metadata for summary data vectors.

        A dictionary indexed by summary vector names is returned, and each
        value is another dictionary with potentially the following metadata types:
        * unit (string)
        * is_total (bool)
        * is_rate (bool)
        * is_historical (bool)
        * get_num (int) (only provided if not None)
        * keyword (str)
        * wgname (str or None)

        The requested columns are asked for over the entire ensemble, and if necessary
        all realizations will be checked to obtain the metadata for a specific key.
        If metadata differ between realization, behaviour is *undefined*.

        Args:
            column_keys (list or str): Column key wildcards.

        Returns:
            dict of dict with metadata information
        """
        ensemble_smry_keys = self.get_smrykeys(vector_match=column_keys)
        meta = {}
        needed_reals = 0
        # Loop over realizations until all requested keys are accounted for
        for _, realization in self.realizations.items():
            needed_reals += 1
            real_meta = realization.get_smry_meta(column_keys=ensemble_smry_keys)
            meta.update(real_meta)
            missing_keys = set(ensemble_smry_keys) - set(meta.keys())
            if not missing_keys:
                break
        if needed_reals:
            logger.info(
                "Searched %s realization(s) to get summary metadata", str(needed_reals)
            )
        return meta

    def get_df(self, localpath, merge=None):
        """Load data from each realization and aggregate (vertically)

        Data must be already have been internalized using
        a load_*() function.

        Each row is tagged by the realization index in the column 'REAL'

        The localpath argument can be shortened, as it will be
        looked up using the function shortcut2path()

        Args:
            localpath (str): refers to the internalized name.
            merge (list or str): refer to additional localpath which
                will be merged into the dataframe for every realization

        Returns:
           pd.dataframe: Merged data from each realization.
               Realizations with missing data are ignored.

        Raises:
            KeyError if no data is found in no realizations.
        """
        dflist = {}
        for index, realization in self.realizations.items():
            try:
                data = realization.get_df(localpath, merge=merge)
                if isinstance(data, dict):
                    data = pd.DataFrame(index=[1], data=data)
                elif isinstance(data, (str, int, float, np.number)):
                    data = pd.DataFrame(index=[1], columns=[localpath], data=data)
                if isinstance(data, pd.DataFrame):
                    dflist[index] = data
                else:
                    raise ValueError("Unkown datatype returned " + "from realization")
            except (KeyError, ValueError):
                # No logging here, those error messages
                # should have appeared at construction using load_*()
                pass
        if dflist:
            # Merge a dictionary of dataframes. The dict key is
            # the realization index, and end up in a MultiIndex
            dframe = pd.concat(dflist, sort=False).reset_index()
            dframe.rename(columns={"level_0": "REAL"}, inplace=True)
            del dframe["level_1"]  # This is the indices from each real
            return dframe
        raise KeyError("No data found for " + localpath)

    def load_smry(
        self,
        time_index="raw",
        column_keys=None,
        stacked=True,
        cache_eclsum=True,
        start_date=None,
        end_date=None,
        include_restart=True,
    ):
        """
        Fetch and internalize summary data from all realizations.

        The fetched summary data will be cached/internalized by each
        realization object, and can be retrieved through get_df().

        The name of the internalized dataframe is "unsmry--" + a string
        for the time index, 'monthly', 'yearly', 'daily' or 'raw'.

        Multiple calls to this function with differnent time indices
        will lead to multiple storage of internalized dataframes, so
        your ensemble can both contain a yearly and a monthly dataset.
        There is no requirement for the column_keys to be consistent, but
        care should be taken if they differ.

        If you create a virtual ensemble of this ensemble object, all
        internalized summary data will be kept, as opposed to if
        you have retrieved it through get_smry()

        Wraps around Realization.load_smry() which wraps around
        ecl.summary.EclSum.pandas_frame()

        Beware that the default time_index for ensembles is 'monthly',
        differing from realizations which use raw dates by default.

        Args:
            time_index (str or list of DateTime):
                If defaulted, the raw Eclipse report times will be used.
                If a string is supplied, that string is attempted used
                via get_smry_dates() in order to obtain a time index,
                typically 'monthly', 'daily' or 'yearly'.
            column_keys (str or list of str): column key wildcards. Default is '*'
                which will match all vectors in the Eclipse output.
            stacked (boolean): determining the dataframe layout. If
                true, the realization index is a column, and dates are repeated
                for each realization in the DATES column.
                If false, a dictionary of dataframes is returned, indexed
                by vector name, and with realization index as columns.
                This only works when time_index is the same for all
                realizations. Not implemented yet!
            cache_eclsum (boolean): Boolean for whether we should cache the EclSum
                objects. Set to False if you cannot keep all EclSum files in
                memory simultaneously
            start_date (str or date): First date to include.
                Dates prior to this date will be dropped, supplied
                start_date will always be included. Overridden if time_index
                is 'first' or 'last'. If string, use ISO-format, YYYY-MM-DD.
                ISO-format, YYYY-MM-DD.
            end_date (str or date): Last date to be included.
                Dates past this date will be dropped, supplied
                end_date will always be included. Overridden if time_index
                is 'first' or 'last'. If string, use ISO-format, YYYY-MM-DD.
            include_restart (boolean): boolean sent to libecl for whether restart
                files should be traversed.
        Returns:
            pd.DataFame: Summary vectors for the ensemble, or
            a dict of dataframes if stacked=False.
        """
        if not stacked:
            raise NotImplementedError
        # Future: Multithread this!
        for realidx, realization in self.realizations.items():
            # We do not store the returned DataFrames here,
            # instead we look them up afterwards using get_df()
            # Downside is that we have to compute the name of the
            # cached object as it is not returned.
            logger.info("Loading smry from realization %s", realidx)
            realization.load_smry(
                time_index=time_index,
                column_keys=column_keys,
                cache_eclsum=cache_eclsum,
                start_date=start_date,
                end_date=end_date,
                include_restart=include_restart,
            )
        if isinstance(time_index, (list, np.ndarray)):
            time_index = "custom"
        elif time_index is None:
            time_index = "raw"
        return self.get_df("share/results/tables/unsmry--" + time_index + ".csv")

    def get_volumetric_rates(self, column_keys=None, time_index=None):
        """Compute volumetric rates from cumulative summary vectors

        Column names that are not referring to cumulative summary
        vectors are silently ignored.

        A Dataframe is returned with volumetric rates, that is rate
        values that can be summed up to the cumulative version. The
        'T' in the column name is switched with 'R'. If you ask for
        FOPT, you will get FOPR in the returned dataframe.

        Rates in the returned dataframe are valid **forwards** in time,
        opposed to rates coming directly from the Eclipse simulator which
        are valid backwards in time.

        Args:
            column_keys (str or list of str): cumulative summary vectors
            time_index (str or list of datetimes):

        Returns:
            pd.DataFrame: analoguous to the dataframe returned by get_smry().
            Empty dataframe if no data found.
        """
        vol_dfs = []
        for realidx, real in self.realizations.items():
            vol_real = real.get_volumetric_rates(
                column_keys=column_keys, time_index=time_index
            )
            if "DATE" not in vol_real.columns and vol_real.index.name == "DATE":
                # This should be true, if not we might be in trouble.
                vol_real.reset_index(inplace=True)
            vol_real.insert(0, "REAL", realidx)
            vol_dfs.append(vol_real)

        if not vol_dfs:
            return pd.DataFrame()
        return pd.concat(vol_dfs, ignore_index=True, sort=False)

    def filter(self, localpath, inplace=True, **kwargs):
        """Filter realizations or data within realizations

        Calling this function can return a copy with fewer
        realizations, or remove realizations from the current object.

        Typical usage is to require that parameters.txt is present, or
        that the OK file is present.

        It is also possible to require a certain scalar to have a specific
        value, for example filtering on a specific sensitivity case.

        Args:
            localpath (string): pointing to the data for which the filtering
                applies. If no other arguments, only realizations containing
                this data key is kept.
            key (str): A certain key within a realization dictionary that is
                required to be present. If a value is also provided, this
                key must be equal to this value
            value (str, int or float): The value a certain key must equal. Floating
                point comparisons are not robust.
            column (str): Name of a column in tabular data. If columncontains is
                not specified, this means that this column must be present
            columncontains (str, int or float):
                A value that the specific column must include.
            inplace (boolean): Indicating if the current object should have its
                realizations stripped, or if a copy should be returned.
                Default true.

         Return:
            If inplace=True, then nothing will be returned.
            If inplace=False, a VirtualEnsemble fulfilling the filter
            will be returned.
        """
        deletethese = []
        keepthese = []
        for realidx, realization in self.realizations.items():
            if inplace:
                if not realization.contains(localpath, **kwargs):
                    deletethese.append(realidx)
            else:
                if realization.contains(localpath, **kwargs):
                    keepthese.append(realidx)

        if inplace:
            logger.info("Removing realizations %s", deletethese)
            if deletethese:
                self.remove_realizations(deletethese)
            return self
        filtered = VirtualEnsemble(self.name + " filtered")
        for realidx in keepthese:
            filtered.add_realization(self.realizations[realidx])
        return filtered

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
        if shortcut2path(self.keys(), localpath) not in self.keys():
            raise ValueError("%s not found" % localpath)
        for _, realization in self.realizations.items():
            try:
                realization.drop(localpath, **kwargs)
            except ValueError:
                pass  # Allow localpath to be missing in some realizations

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
        Returns:
            ScratchEnsemble: This ensemble object (self), for it
                to be picked up by ProcessPoolExecutor and pickling.
        """
        for realization in self.realizations.values():
            realization.process_batch(batch)
        return self

    def apply(self, callback, **kwargs):
        """Callback functionalty, apply a function to every realization

        The supplied function handle will be handed over to
        each underlying realization object. The function supplied
        must return a Pandas DataFrame. The function can obtain
        the realization object in the kwargs dictionary through
        the key 'realization'.

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
        logger.info("Ensemble %s is running callback %s", self.name, str(callback))
        for realidx, realization in self.realizations.items():
            result = realization.apply(callback, **kwargs).copy()
            # (we took a copy since we are modifying it here:)
            # Todo: Avoid copy by concatenatint a dict of dataframes
            # where realization index is the dict keys.
            result["REAL"] = realidx
            results.append(result)
        return pd.concat(results, sort=False, ignore_index=True)

    def get_smry_dates(
        self,
        freq="monthly",
        normalize=True,
        start_date=None,
        end_date=None,
        cache_eclsum=True,
        include_restart=True,
    ):
        """Return list of datetimes for an ensemble according to frequency

        Args:
           freq: string denoting requested frequency for
               the returned list of datetime. 'report' or 'raw' will
               yield the sorted union of all valid timesteps for
               all realizations. Other valid options are
               'daily', 'monthly' and 'yearly'.
               'first' will give out the first date (minimum).
               'last' will give out the last date (maximum).
            normalize:  Whether to normalize backwards at the start
                and forwards at the end to ensure the raw
                date range is covered.
            start_date: str or date with first date to include.
                Dates prior to this date will be dropped, supplied
                start_date will always be included. Overrides
                normalized dates. Overridden if freq is 'first' or 'last'.
                If string, use ISO-format, YYYY-MM-DD.
            end_date: str or date with last date to be included.
                Dates past this date will be dropped, supplied
                end_date will always be included. Overrides
                normalized dates. Overridden if freq is 'first' or 'last'.
                If string, use ISO-format, YYYY-MM-DD.
            include_restart: boolean sent to libecl for whether restart
                files should be traversed.

        Returns:
            list of datetimes. Empty list if no data found.
        """

        # Build list of list of eclsum dates
        eclsumsdates = []
        for _, realization in self.realizations.items():
            if realization.get_eclsum(
                cache=cache_eclsum, include_restart=include_restart
            ):
                eclsumsdates.append(
                    realization.get_eclsum(
                        cache=cache_eclsum, include_restart=include_restart
                    ).dates
                )
        return unionize_smry_dates(eclsumsdates, freq, normalize, start_date, end_date)

    def get_smry_stats(
        self,
        column_keys=None,
        time_index="monthly",
        quantiles=None,
        cache_eclsum=True,
        start_date=None,
        end_date=None,
    ):
        """
        Function to extract the ensemble statistics (Mean, Min, Max, P10, P90)
        for a set of simulation summary vectors (column key).

        Compared to the agg() function, this function only works on summary
        data (time series), and will only operate on actually requested data,
        independent of what is internalized. It accesses the summary files
        directly and can thus obtain data at any time frequency.

        Args:
            column_keys: list of column key wildcards
            time_index: list of DateTime if interpolation is wanted
               default is None, which returns the raw Eclipse report times
               If a string is supplied, that string is attempted used
               via get_smry_dates() in order to obtain a time index.
            quantiles: list of ints between 0 and 100 for which quantiles
               to compute. Quantiles refer to scientific standard, which
               is opposite to the oil industry convention.
               Ask for p10 if you need the oil industry p90.
            cache_eclsum: boolean for whether to keep the loaded EclSum
                object in memory after data has been loaded.
            start_date: str or date with first date to include.
                Dates prior to this date will be dropped, supplied
                start_date will always be included. Overridden if time_index
                is 'first' or 'last'. If string, use ISO-format, YYYY-MM-DD.
            end_date: str or date with last date to be included.
                Dates past this date will be dropped, supplied
                end_date will always be included. Overridden if time_index
                is 'first' or 'last'. If string, use ISO-format, YYYY-MM-DD.
        Returns:
            A MultiIndex dataframe. Outer index is 'minimum', 'maximum',
            'mean', 'p10', 'p90', inner index are the dates. Column names
            are the different vectors. Quantiles refer to the scientific
            standard, opposite to the oil industry convention.
            If quantiles are explicitly supplied, the 'pXX'
            strings in the outer index are changed accordingly. If no
            data is found, return empty DataFrame.
        """
        if quantiles is None:
            quantiles = [10, 90]

        # Check validity of quantiles to compute:
        quantiles = list(map(int, quantiles))  # Potentially raise ValueError
        for quantile in quantiles:
            if quantile < 0 or quantile > 100:
                raise ValueError("Quantiles must be integers " + "between 0 and 100")

        # Obtain an aggregated dataframe for only the needed columns over
        # the entire ensemble.
        dframe = self.get_smry(
            time_index=time_index,
            column_keys=column_keys,
            cache_eclsum=cache_eclsum,
            start_date=start_date,
            end_date=end_date,
        )
        if "REAL" in dframe:
            dframe = dframe.drop(columns="REAL").groupby("DATE")
        else:
            logger.warning("No data found for get_smry_stats")
            return pd.DataFrame()

        # Build a dictionary of dataframes to be concatenated
        dframes = {}
        dframes["mean"] = dframe.mean()
        for quantile in quantiles:
            quantile_str = "p" + str(quantile)
            dframes[quantile_str] = dframe.quantile(q=quantile / 100.0)
        dframes["maximum"] = dframe.max()
        dframes["minimum"] = dframe.min()

        return pd.concat(dframes, names=["STATISTIC"], sort=False)

    def get_wellnames(self, well_match=None):
        """
        Return a union of all Eclipse Summary well names
        in all realizations (union). In addition, can return a list
        based on matches to an input string pattern.

        Args:
            well_match: `Optional`. String (or list of strings)
               with wildcard filter. If None, all wells are returned
        Returns:
            list of strings with eclipse well names. Empty list if no
            summary file or no matched well names.

        """
        if isinstance(well_match, str):
            well_match = [well_match]
        result = set()
        for _, realization in self.realizations.items():
            eclsum = realization.get_eclsum()
            if eclsum:
                if well_match is None:
                    result = result.union(set(eclsum.wells()))
                else:
                    for well in well_match:
                        result = result.union(set(eclsum.wells(well)))

        return sorted(list(result))

    def get_groupnames(self, group_match=None):
        """
        Return a union of all Eclipse Summary group names
        in all realizations (union).

        Optionally, the well names can be filtered.

        Args:
            well_match: `Optional`. String (or list of strings)
               with wildcard filter (globbing). If None, all
               wells are returned. Empty string does not match anything.
        Returns:
            list of strings with eclipse well names. Empty list if no
            summary file or no matched well names.

        """

        if isinstance(group_match, str):
            group_match = [group_match]
        result = set()
        for _, realization in self.realizations.items():
            eclsum = realization.get_eclsum()
            if eclsum:
                if group_match is None:
                    result = result.union(set(eclsum.groups()))
                else:
                    for group in group_match:
                        result = result.union(set(eclsum.groups(group)))

        return sorted(list(result))

    def agg(self, aggregation, keylist=None, excludekeys=None):
        """Aggregate the ensemble data into one VirtualRealization

        All data will be attempted aggregated. String data will typically
        be dropped in the result.

        Arguments:
            aggregation: string, supported modes are
                'mean', 'median', 'p10', 'p90', 'min',
                'max', 'std, 'var', 'pXX' where X is a number
            keylist: list of strings, indicating which keys
                in the internal datastore to include. If list is empty
                (default), all data will be attempted included.
            excludekeys: list of strings that should be excluded if
                keylist is empty, otherwise ignored
        Returns:
            VirtualRealization. Its name will include the aggregation operator

        WARNING: This code is duplicated in virtualensemble.py
        """
        quantilematcher = re.compile(r"p(\d\d)")
        supported_aggs = ["mean", "median", "min", "max", "std", "var"]
        if aggregation not in supported_aggs and not quantilematcher.match(aggregation):
            raise ValueError(
                "{arg} is not a".format(arg=aggregation)
                + "supported ensemble aggregation"
            )

        # Generate a new empty object:
        vreal = VirtualRealization(self.name + " " + aggregation)

        # Determine keys to use
        if isinstance(keylist, str):
            keylist = [keylist]
        if not keylist:  # Empty list means all keys.
            if not isinstance(excludekeys, list):
                excludekeys = [excludekeys]
            keys = set(self.keys()) - set(excludekeys)
        else:
            keys = keylist

        for key in keys:
            # Aggregate over this ensemble:
            # Ensure we operate on fully qualified localpath's
            key = shortcut2path(self.keys(), key)
            data = self.get_df(key)

            # This column should never appear in aggregated data
            del data["REAL"]

            # Look for data we should group by. This would be beneficial
            # to get from a metadata file, and not by pure guesswork.
            groupbycolumncandidates = [
                "DATE",
                "FIPNUM",
                "ZONE",
                "REGION",
                "JOBINDEX",
                "Zone",
                "Region_index",
            ]

            # Pick up string columns (or non-numeric values)
            # (when strings are used as values, this breaks, but it is also
            # meaningless to aggregate them. Most likely, strings in columns
            # is a label we should group over)
            stringcolumns = [x for x in data.columns if data.dtypes[x] == "object"]

            groupby = [x for x in groupbycolumncandidates if x in data.columns]

            # Add remainding string columns to columns to group by unless
            # we are working with the STATUS dataframe, which has too many strings..
            if key != "STATUS":
                groupby = list(set(groupby + stringcolumns))

            # Filter to only numerical columns and groupby columns:
            numerical_and_groupby_cols = list(
                set(list(groupby) + list(data.select_dtypes(include="number").columns))
            )
            data = data[numerical_and_groupby_cols]

            dtypes = data.dtypes.unique()
            if not (int in dtypes or float in dtypes):
                logger.info("No numerical data to aggregate in %s", key)
                continue
            if groupby:
                logger.info("Grouping %s by %s", key, groupby)
                aggobject = data.groupby(groupby)
            else:
                aggobject = data

            if quantilematcher.match(aggregation):
                quantile = int(quantilematcher.match(aggregation).group(1))
                aggregated = aggobject.quantile(quantile / 100.0)
            else:
                # Passing through the variable 'aggregation' to
                # Pandas, thus supporting more than we have listed in
                # the docstring.
                aggregated = aggobject.agg(aggregation)

            if groupby:
                aggregated.reset_index(inplace=True)

            # We have to recognize scalars.
            if len(aggregated) == 1 and aggregated.index.values[0] == key:
                aggregated = parse_number(aggregated.values[0])
            vreal.append(key, aggregated)
        return vreal

    @property
    def files(self):
        """Return a concatenation of files in each realization"""
        filedflist = []
        for realidx, realization in self.realizations.items():
            realfiles = realization.files.copy()
            realfiles.insert(0, "REAL", realidx)
            filedflist.append(realfiles)
        return pd.concat(filedflist, ignore_index=True, sort=False)

    @property
    def name(self):
        """The ensemble name."""
        return self._name

    @name.setter
    def name(self, newname):
        if isinstance(newname, str):
            self._name = newname
        else:
            raise ValueError("Name input is not a string")

    def __sub__(self, other):
        """Substract another ensemble from this"""
        result = EnsembleCombination(ref=self, sub=other)
        return result

    def __add__(self, other):
        """Add another ensemble to this"""
        result = EnsembleCombination(ref=self, add=other)
        return result

    def __mul__(self, other):
        """Scale this ensemble with a scalar value"""
        result = EnsembleCombination(ref=self, scale=float(other))
        return result

    def __rsub__(self, other):
        """Substract another ensemble from this"""
        result = EnsembleCombination(ref=self, sub=other)
        return result

    def __radd__(self, other):
        """Add another ensemble to this"""
        result = EnsembleCombination(ref=self, add=other)
        return result

    def __rmul__(self, other):
        """Scale this ensemble with a scalar value"""
        result = EnsembleCombination(ref=self, scale=float(other))
        return result

    def get_realindices(self):
        """Return the integer indices for realizations in this ensemble

        Returns:
            list of integers
        """
        return self.realizations.keys()

    def get_smry(
        self,
        time_index=None,
        column_keys=None,
        cache_eclsum=True,
        start_date=None,
        end_date=None,
        include_restart=True,
    ):
        """
        Aggregates summary data from all realizations.

        Wraps around Realization.get_smry() which wraps around
        ecl.summary.EclSum.pandas_frame()

        Args:
            time_index: list of DateTime if interpolation is wanted
               default is None, which returns the raw Eclipse report times
               If a string with an ISO-8601 date is supplied, that date
               is used directly, otherwise the string is assumed to indicate
               a wanted frequencey for dates, daily, weekly, monthly, yearly,
               that will be send to get_smry_dates()
            column_keys: list of column key wildcards
            cache_eclsum: boolean for whether to cache the EclSum
                objects. Defaults to True. Set to False if
                not enough memory to keep all summary files in memory.
            start_date: str or date with first date to include.
                Dates prior to this date will be dropped, supplied
                start_date will always be included. Overridden if time_index
                is 'first' or 'last'.
            end_date: str or date with last date to be included.
                Dates past this date will be dropped, supplied
                end_date will always be included. Overridden if time_index
                is 'first' or 'last'.
            include_restart: boolean sent to libecl for whether restart
                files should be traversed.

        Returns:
            A DataFame of summary vectors for the ensemble. The column
            REAL with integers is added to distinguish realizations. If
            no realizations, empty DataFrame is returned.
        """
        if isinstance(time_index, str):
            # Try interpreting as ISO-date:
            try:
                parseddate = dateutil.parser.isoparse(time_index)
                time_index = [parseddate]
            # But this should fail when a frequency string is supplied:
            except ValueError:
                time_index = self.get_smry_dates(
                    time_index,
                    start_date=start_date,
                    end_date=end_date,
                    include_restart=include_restart,
                )
        dflist = []
        for index, realization in self.realizations.items():
            dframe = realization.get_smry(
                time_index=time_index,
                column_keys=column_keys,
                cache_eclsum=cache_eclsum,
                include_restart=include_restart,
            )
            dframe.insert(0, "REAL", index)
            dframe.index.name = "DATE"
            dflist.append(dframe)
        if dflist:
            return pd.concat(dflist, sort=False).reset_index()
        return pd.DataFrame()

    def get_eclgrid(self, props, report=0, agg="mean", active_only=False):
        """
        Returns the grid (i,j,k) and (x,y), and any requested init
        and/or unrst property. The values are aggregated over the
        ensemble (mean/ std currently supported).

        Args:
            props: list of column key wildcards
            report: int. for unrst props only. Report step for given date.
                    Use the function get_unrst_report_dates to get an overview
                    of the report steps availible.
            agg: String. "mean" or "std".
            active_only: bool. True if activate cells only.
        Returns:
            A dictionary. Index by grid attribute, and contains a list
            corresponding to a set of values for each grid cells.
        """
        egrid_reals = [
            real for real in self.realizations.values() if real.get_grid() is not None
        ]
        # Pick a "random" reference realization for grids.
        ref = egrid_reals[0]
        grid_index = ref.get_grid_index(active_only=active_only)
        corners = ref.get_grid_corners(grid_index)
        centre = ref.get_grid_centre(grid_index)
        dframe = grid_index.reset_index().join(corners).join(centre)
        dframe["realizations_active"] = self.global_active.numpy_copy()
        for prop in props:
            logger.info("Reading the grid property: %s", prop)
            if prop in self.init_keys:
                dframe[prop] = self.get_init(prop, agg=agg)
            if prop in self.unrst_keys:
                dframe[prop] = self.get_unrst(prop, agg=agg, report=report)
        dframe.drop("index", axis=1, inplace=True)
        dframe.set_index(["i", "j", "k", "active"])
        return dframe

    @property
    def global_active(self):
        """
        :returns: An EclKw with, for each cell,
            the number of realizations where the cell is active.
        """
        if not self._global_active:
            self._global_active = EclKW(
                "eactive", self.global_size, EclDataType.ECL_INT
            )
            for realization in self.realizations.values():
                if realization.get_grid() is not None:
                    self._global_active += realization.actnum

        return self._global_active

    @property
    def global_size(self):
        """
        :returns: global size of the realizations in the Ensemble.  see
            :func:`fmu_postprocessing.modelling.Realization.global_size()`.
        """
        if not self.realizations:
            return 0
        if self._global_size is None:
            egrid_reals = [
                real
                for real in self.realizations.values()
                if real.get_grid() is not None
            ]
            ref = egrid_reals[0]
            self._global_size = ref.global_size
        return self._global_size

    def _get_grid_index(self, active=True):
        """
        :returns: The grid of the ensemble, see
            :func:`fmu.ensemble.Realization.get_grid()`.
        """
        if not self.realizations:
            return None
        else:
            logger.warning("No GRID file in realization %s", self)
            return None
        return list(self.realizations.values())[0].get_grid_index(active=active)

    @property
    def init_keys(self):
        """ Keys availible in the eclipse init file """
        if not self.realizations:
            return None
        all_keys = set.union(
            *[
                set(realization.get_init().keys())
                for _, realization in self.realizations.items()
                if realization.get_grid() is not None
            ]
        )
        return all_keys

    @property
    def unrst_keys(self):
        """ Keys availaible in the eclipse unrst file """
        if not self.realizations:
            return None
        all_keys = set.union(
            *[
                set(realization.get_unrst().keys())
                for _, realization in self.realizations.items()
                if realization.get_unrst() is not None
            ]
        )
        return all_keys

    def get_unrst_report_dates(self):
        """ returns unrst report step and the corresponding date """
        if not self.realizations:
            return None
        all_report_dates = set.union(
            *[
                set(realization.report_dates)
                for _, realization in self.realizations.items()
                if realization.get_unrst() is not None
            ]
        )
        all_report_dates = list(all_report_dates)
        all_report_dates.sort()
        dframe = pd.DataFrame(all_report_dates, columns=["Dates"])
        dframe.index.names = ["Report"]
        return dframe

    def get_init(self, prop, agg):
        """
        :param prop: A time independent property,
        :returns: Dictionary with ``mean`` or ``std_dev`` as keys,
            and corresponding values for given property as values.
        :raises ValueError: If prop is not found.
        """
        if agg == "mean":
            mean = self._keyword_mean(prop, self.global_active)
            return pd.Series(mean.numpy_copy(), name=prop)
        if agg == "std":
            std_dev = self._keyword_std_dev(prop, self.global_active, mean)
            return pd.Series(std_dev.numpy_copy(), name=prop)
        return pd.Series()

    def get_unrst(self, prop, report, agg):
        """
        :param prop: A time dependent property, see
            `fmu_postprocessing.modelling.SimulationGrid.TIME_DEPENDENT`.
        :returns: Dictionary with ``mean`` and ``std_dev`` as keys,
            and corresponding values for given property as values.
        :raises ValueError: If prop is not in `TIME_DEPENDENT`.
        """

        if agg == "mean":
            mean = self._keyword_mean(prop, self.global_active, report=report)
            return pd.Series(mean.numpy_copy(), name=prop)
        if agg == "std":
            std_dev = self._keyword_std_dev(
                prop, self.global_active, mean, report=report
            )
            return pd.Series(std_dev.numpy_copy(), name=prop)
        return pd.Series()

    def _keyword_mean(self, prop, global_active, report=None):
        """
        :returns: Mean values of keywords.
        :param prop: Name of resulting Keyword.
        :param global_active: A EclKW with, for each cell, The number of
            realizations where the cell is active.
        :param report: Report step for unrst keywords
        """
        mean = EclKW(prop, len(global_active), EclDataType.ECL_FLOAT)
        if report:
            for _, realization in self.realizations.items():
                if realization.get_unrst() is not None:
                    mean += realization.get_global_unrst_keyword(prop, report)
            mean.safe_div(global_active)
            return mean
        for _, realization in self.realizations.items():
            if realization.get_grid() is not None:
                mean += realization.get_global_init_keyword(prop)
        mean.safe_div(global_active)
        return mean

    def _keyword_std_dev(self, prop, global_active, mean, report=0):
        """
        :returns: Standard deviation of keywords.
        :param name: Name of resulting Keyword.
        :param keywords: List of pairs of keywords and list of active cell
        :param global_active: A EclKW with, for each cell, The number of
            realizations where the cell is active.
        :param mean: Mean of keywords.
        """
        std_dev = EclKW(prop, len(global_active), EclDataType.ECL_FLOAT)
        if report:
            for _, realization in self.realizations.items():
                real_prop = realization.get_global_unrst_keyword(prop, report)
                std_dev.add_squared(real_prop - mean)
            std_dev.safe_div(global_active)
            return std_dev.isqrt()
        for _, realization in self.realizations.items():
            if realization.get_grid() is not None:
                real_prop = realization.get_global_init_keyword(prop)
                std_dev.add_squared(real_prop - mean)
        std_dev.safe_div(global_active)
        return std_dev.isqrt()
