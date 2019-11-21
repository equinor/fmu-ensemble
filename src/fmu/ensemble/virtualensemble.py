# -*- coding: utf-8 -*-
"""Module containing a VirtualEnsemble class"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import re
import shutil
import datetime

import six
import yaml
import numpy as np
import pandas as pd

from .etc import Interaction
from .virtualrealization import VirtualRealization

fmux = Interaction()
logger = fmux.basiclogger(__name__, level="INFO")


class VirtualEnsemble(object):
    """A computed or archived ensemble

    Computed or archived, there is no link to the original dataset(s)
    that once was on a file system.

    Contrary to a ScratchEnsemble which contains realization objects
    with individual data, a VirtualEnsemble stores aggregrated
    dataframes for its data. The column REAL will in all dataframes
    signify the realization index.

    Initialization of VirtualEnsembles is typically done by other code, as
    to_virtual() in a ScratchEnsemble.

    Args:
        name: string, can be chosen freely
        data: dict with data to initialize with. Defaults to empty
        longdescription: string, free form multiline description.
        fromdisk: string with filesystem path, from which we will
            try to initialize the ensemble from files on disk.
        lazy_load (boolean): If true, it will be used if loaded from disk
            to be lazy in actually loading dataframes from disk
        manifest: dict with any information about the ensemble
    """

    def __init__(
        self,
        name=None,
        data=None,
        longdescription=None,
        fromdisk=None,
        lazy_load=False,
        manifest=None,
    ):

        if name:
            self._name = name
        else:
            self._name = "VirtualEnsemble"

        self._longdescription = longdescription
        self._manifest = {}

        if data and fromdisk:
            raise ValueError(
                "Can't initialize from both data and " + "disk at the same time"
            )

        self.realindices = []

        if manifest and not fromdisk:
            self.manifest = manifest

        # At ensemble level, this dictionary has dataframes only.
        # All dataframes have the column REAL.
        if data:
            self.data = data
        else:
            self.data = {}

        # We support having some dataframes only on disk, for faster
        # initialization of the VirtualEnsemble object. This
        # dictionary have the same keys as self.data and the value is
        # a full path to a filename on disk. There should never be
        # overlap of keys in self.data and self.lazy_frames.
        self.lazy_frames = {}

        if fromdisk:
            self.from_disk(fromdisk, lazy_load=lazy_load)

    def __len__(self):
        """Return the number of realizations (integer) included in the
        ensemble"""
        return len(self.realindices)

    def update_realindices(self):
        """Update the internal list of known realization indices

        Anything that adds or removes realizations must
        take responsibility for having that list consistent.

        If there is a dataframe missing the REAL column, this
        will intentionally error.
        """

        # Check all dataframes:
        idxset = set()
        for key in self.data.keys():
            idxset = idxset | set(self.data[key]["REAL"].unique())
        self.realindices = list(idxset)

    def keys(self):
        """Return all keys in the internal datastore

        The keys are also called localpaths, and resemble the the
        filenames they would be written to if dumped to disk, and also
        resemble the filenames from which they were originally
        loaded in a ScratchEnsemble.
        """
        return list(self.data.keys()) + list(self.lazy_frames.keys())

    def lazy_keys(self):
        """Return keys that are not yet loaded, but will
        be loaded on demand"""
        return list(self.lazy_frames.keys())

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

        """
        from fmu.ensemble import ScratchEnsemble

        return ScratchEnsemble._shortcut2path(self.keys(), shortpath)

    def __getitem__(self, localpath):
        """Shorthand for .get_df()

        Args:
            localpath: string with the name of the data requested.
                shortcuts allowed
        """
        return self.get_df(localpath)

    def get_realization(self, realindex):
        """
        Return a virtual realization object, with data
        taken from the virtual ensemble. Each dataframe
        in the ensemble will by sliced by the REAL column.

        Args:
            realindex: integer for the realization.

        Returns:
            VirtualRealization, populated with data.
        """
        vreal = VirtualRealization(
            description="Realization %d from %s" % (realindex, self._name)
        )
        for key in self.data.keys():
            data = self.get_df(key)
            realizationdata = data[data["REAL"] == realindex]
            if len(realizationdata) == 1:
                # Convert scalar values to dictionaries, avoiding
                # getting length-one-series returned later on access.
                realizationdata = realizationdata.iloc[0].to_dict()
            elif len(realizationdata) > 1:
                realizationdata.reset_index(inplace=True, drop=True)
            else:
                continue
            del realizationdata["REAL"]
            vreal.append(key, realizationdata)
        if vreal.keys():
            return vreal
        else:
            raise ValueError("No data for realization %d" % realindex)

    def add_realization(self, realization, realidx=None, overwrite=False):
        """Add a realization. A ScratchRealization will be effectively
        converted to a virtual realization.

        A ScratchRealization knows its realization index, and that index
        will be used unless realidx is not None. A VirtualRealization does
        not always have a index, so then it must be supplied.

        Unless overwrite is True, a ValueError will be raised
        if the realization index already exists.

        Args:
            overwrite: boolean whether an existing realization with the same
                index should be removed prior to adding
            realidx: Override the realization index for incoming realization.
                Necessary for VirtualRealization.
        """
        if realidx is None and isinstance(realization, VirtualRealization):
            raise ValueError(
                "Can't add virtual realizations " + "without specifying index"
            )
        if not realidx:
            realidx = realization.index

        if not overwrite and realidx in self.realindices:
            raise ValueError("Error, realization index already present")
        if overwrite and realidx in self.realindices:
            self.remove_realizations(realidx)

        # Add the data from the incoming realization key by key
        for key in realization.keys():
            dframe = realization.get_df(key)
            if isinstance(dframe, dict):  # dicts to go to one-row dataframes
                dframe = pd.DataFrame(index=[1], data=dframe)
            if isinstance(dframe, (str, int, float)):
                dframe = pd.DataFrame(index=[1], columns=[key], data=dframe)
            dframe["REAL"] = realidx
            if key not in self.data and key in self.lazy_frames:
                self.get_df(key)  # Trigger load from disk.
            if key not in self.data.keys():
                self.data[key] = dframe
            else:
                self.data[key] = self.data[key].append(
                    dframe, ignore_index=True, sort=True
                )
        self.update_realindices()

    def remove_realizations(self, deleteindices):
        """Remove realizations from internal data

        This will remove all rows in all internalized data belonging
        to the set of supplied indices.

        Args:
            deleteindices: int or list of ints, realization indices to remove
        """
        if not isinstance(deleteindices, list):
            deleteindices = [deleteindices]

        indicesknown = self.realindices
        indicestodelete = list(set(deleteindices) & set(indicesknown))
        indicesnotknown = list(set(deleteindices) - set(indicestodelete))
        if indicesnotknown:
            logger.warning(
                "Skipping undefined realization indices %s", str(indicesnotknown)
            )

        # Trigger load of any lazy frames:
        for key in list(self.lazy_frames.keys()):
            self.get_df(key)
        # There might be Pandas tricks to avoid this outer loop.
        for realindex in indicestodelete:
            for key in self.data:
                self.data[key] = self.data[key][self.data[key]["REAL"] != realindex]
        self.update_realindices()
        logger.info(
            "Removed %s realization(s) from VirtualEnsemble", len(indicestodelete)
        )

    def remove_data(self, localpaths):
        """Remove a certain datatype from the internal datastore

        Args:
            localpaths: string or list of strings, fully qualified localpath
                (no shorthand allowed)
        """
        if not isinstance(localpaths, list):
            localpaths = [localpaths]
        for localpath in localpaths:
            if localpath in self.data:
                del self.data[localpath]
                logger.info("Deleted %s from ensemble", localpath)
            elif localpath in self.lazy_frames:
                del self.lazy_frames[localpath]
                logger.info("Deleted %s from ensemble", localpath)
            else:
                logger.warning("Ensemble did not contain %s", localpath)

    def agg(self, aggregation, keylist=None, excludekeys=None):
        """Aggregate the ensemble data into a VirtualRealization

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

        WARNING: CODE DUPLICATION from ensemble.py
        """
        quantilematcher = re.compile(r"p(\d\d)")
        supported_aggs = ["mean", "median", "min", "max", "std", "var"]
        if aggregation not in supported_aggs and not quantilematcher.match(aggregation):
            raise ValueError(
                "{arg} is not a".format(arg=aggregation)
                + "supported ensemble aggregation"
            )

        # Generate a new empty object:
        vreal = VirtualRealization(self._name + " " + aggregation)

        # Determine keys to use
        if isinstance(keylist, str):
            keylist = [keylist]
        if not keylist:  # Empty list means all keys.
            if not isinstance(excludekeys, list):
                excludekeys = [excludekeys]
            keys = set(self.data.keys()).union(set(self.lazy_frames.keys())) - set(
                excludekeys
            )
        else:
            keys = keylist

        # Trigger loading of lazy and needed frames:
        for key in list(self.lazy_frames.keys()):
            if key in keys:
                self.get_df(key)

        for key in keys:
            # Aggregate over this ensemble:
            # Ensure we operate on fully qualified localpath's
            key = self.shortcut2path(key)
            data = self.get_df(key).drop(columns="REAL")

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

            groupby = [x for x in groupbycolumncandidates if x in data.columns]

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
                aggobject = data.groupby(groupby)
            else:
                aggobject = data

            if quantilematcher.match(aggregation):
                quantile = int(quantilematcher.match(aggregation).group(1))
                aggregated = aggobject.quantile(q=quantile / 100.0)
            else:
                # Passing through the variable 'aggregation' to
                # Pandas, thus supporting more than we have listed in
                # the docstring.
                aggregated = aggobject.agg(aggregation)

            if groupby:
                aggregated.reset_index(inplace=True)

            vreal.append(key, aggregated)
        return vreal

    def append(self, key, dataframe, overwrite=False):
        """Append a dataframe to the internal datastore

        Incoming dataframe MUST have a column called 'REAL' which
        refers to the realization indices already known to the object.

        Args:
            key: name (localpath) for the data, this will be
                the name under with the dataframe is stored, for
                later retrival via get_df().
            dataframe: a Pandas DataFrame with a REAL column
            overwrite: boolean - set to True if existing data is
                to be overwritten. Defaults to false which will
                only issue a warning if the dataset exists already.
        """
        if not isinstance(dataframe, pd.DataFrame):
            raise ValueError("Can only append dataframes")
        if "REAL" not in dataframe.columns:
            raise ValueError("REAL column not in incoming dataframe")
        if key in self.data.keys() and not overwrite:
            logger.warning("Ignoring %s data already exists", key)
            return
        self.data[key] = dataframe

    def to_disk(
        self,
        filesystempath,
        delete=False,
        dumpcsv=True,
        dumpparquet=True,
        includefiles=False,
        symlinks=False,
    ):
        """Dump all data to disk, in a retrieveable manner.

        Unless dumpcsv is set to False, all data is dumped to CSV files,
        except if some CSV files cannot be dumped as parquet.

        Unless dumpparquet is set to False, all data is attempted dumped
        as Parquet files. If parquet dumping fails for some reason, a CSV
        file is always left behind.

        dumpcsv and dumpparquet cannot be False at the same time.

        Args:
            filesystempath: string with a directory, absolute or relative.
                If it exists already it must be empty, or delete must be True.
            delete: boolean for whether an existing directory will be cleared
                before data is dumped.
            dumpcsv: boolean for whether CSV files should be written.
            dumpparquet: boolean for whether parquet files should be written
            includefiles (boolean): If set to True, files in the files
                dataframe will be included in the disk-dump.
            symlinks (boolean): If includefiles is True, setting this to True
                means that only symlinking will take place, not full copy.
        """
        import pyarrow  # Move to top of file eventually

        # Trigger load of all lazy frames:
        for key in list(self.lazy_frames.keys()):
            self.get_df(key)

        if not dumpcsv and not dumpparquet:
            raise ValueError(
                "dumpcsv and dumpparquet " + "cannot be False at the same time"
            )

        def prepare_vens_directory(filesystempath, delete=False):
            """Prepare a directory for dumping a virtual ensemble.

            The end result is either an error, or a clean empty directory
            at the requested path"""
            logger.info("Preparing %s for a dumped virtual ensemble", filesystempath)
            if os.path.exists(filesystempath):
                if delete:
                    logger.info(" - Deleted existing directory")
                    shutil.rmtree(filesystempath)
                    os.mkdir(filesystempath)
                else:
                    if os.listdir(filesystempath):
                        logger.critical(
                            (
                                "Refusing to write virtual ensemble "
                                " to non-empty directory"
                            )
                        )
                        raise IOError("Directory %s not empty" % filesystempath)
            else:
                os.mkdir(filesystempath)

        prepare_vens_directory(filesystempath, delete)

        includefilesdir = "__discoveredfiles"
        if includefiles:
            os.mkdir(os.path.join(filesystempath, includefilesdir))
        for _, filerow in self.files.iterrows():
            src_fpath = filerow["FULLPATH"]
            dest_fpath = os.path.join(
                filesystempath,
                includefilesdir,
                "realization-" + str(filerow["REAL"]),
                filerow["LOCALPATH"],
            )
            directory = os.path.dirname(dest_fpath)
            if not os.path.exists(directory):
                os.makedirs(os.path.dirname(dest_fpath))
            if symlinks:
                os.symlink(src_fpath, dest_fpath)
            else:
                shutil.copy(src_fpath, dest_fpath)

        # Write ensemble meta-information to disk:
        with open(os.path.join(filesystempath, "_name"), "w") as fhandle:
            fhandle.write(str(self._name))
        with open(os.path.join(filesystempath, "__repr__"), "w") as fhandle:
            fhandle.write(self.__repr__())
        if self._manifest:
            with open(os.path.join(filesystempath, "_manifest.yml"), "w") as fhandle:
                fhandle.write(yaml.dump(self._manifest))

        # The README dumped here is just for convenience. Do not assume
        # anything about its content.
        with open(os.path.join(filesystempath, "README"), "w") as fhandle:
            fhandle.write(
                """The data in here has been dumped by
fmu.ensemble.VirtualEnsemble.to_disk()

Each filename represents a DataFrame with aggregated data
for an ensemble. The DataFrames exists both in a csv format and
in a binary format. If you need to do manual edits, choose to
edit the csv file and delete the parquet file (or vice versa) to
ensure that when reloaded into a VirtualEnsemble object, the correct
file is picked up"""
            )
        # Write all data we have to disk:

        # We are out on a limb with respect to what the keys
        # in the internalized dict-storage should be, and what
        # we should call files on disk.
        # Loaded txt files keep their txt extension in the internalized
        # dict, so that the localpath is often 'npv.txt' for a dataframe
        # This will be written as 'npv.txt.csv' and/or 'npv.txt.parquet' on
        # disk. This we can handle.
        #
        # Internalized csv files are one notch more strange.
        # When we internalize a csv file, we keep the csv extension in
        # the dict-key, say unsmry--daily.csv.
        # The logic from npv.txt implies that we write
        # to unsmry--daily.csv.csv and unsmry--daily.csv.parquet.
        # This would be easier to program, but will look to strange on disk
        # It is an aim that the user should be able to fill the filesystem
        # with CSV files, and reload ensembles.

        # The chosen strategy is currently like this:
        # to_disk:
        # parameters.txt -> parameters.txt.csv and parameters.txt.parquet
        # unsmry--daily.csv -> unsmry--daily.csv and unsmry--daily.parquet

        # from_disk:
        # parameters.txt.csv -> parameters.txt because there is a known
        #     extension after removal of csv.
        # STATUS.csv -> STATUS, because STATUS is a special file
        # unsmry--daily.csv -> unsmry--daily.csv
        # unsmry--daily.parquet -> unsmry--daily.csv

        for key in self.keys():
            dirname = os.path.join(filesystempath, os.path.dirname(key))
            if dirname:
                if not os.path.exists(dirname):
                    os.makedirs(dirname)

            data = self.get_df(key)
            filename = os.path.join(dirname, os.path.basename(key))

            # Trim .csv from end of dict-key
            # .csv will be reinstated by logic in from_disk()
            if filename[-4:] == ".csv":
                filebase = filename[:-4]
            else:
                # parameters.txt or STATUS ends here:
                filebase = filename

            if not isinstance(data, pd.DataFrame):
                raise ValueError("VirtualEnsembles should " + "only store DataFrames")
            parquetfailed = False
            if dumpparquet:
                try:
                    data.to_parquet(filebase + ".parquet", index=False, engine="auto")
                    logger.info("Wrote %s", filebase + ".parquet")
                except (ValueError, pyarrow.ArrowTypeError, TypeError):
                    # Accept that some dataframes cannot be written by parquet,
                    # The CSV file will be there as backup.
                    logger.warning("Could not write %s as parquet file", key)
                    parquetfailed = True

            if dumpcsv or parquetfailed:
                data.to_csv(filebase + ".csv", index=False)
                logger.info("Wrote %s", filebase + ".csv")

    def from_disk(self, filesystempath, fmt="parquet", lazy_load=False):
        """Load data from disk.

        Data must be written like to_disk() would have
        written it. As long as you follow that convention,
        you are able to add data manually to the filesystem
        and load them into a VirtualEnsemble.

        Any DataFrame not containing a column called 'REAL' with
        integers will be ignored.

        Args:
            filesystempath (string): path to a directory that was written by
                VirtualEnsemble.to_disk().
            fmt (string): the preferred format to load,
                must be either csv or parquet. If you say 'csv'
                parquet files will always be ignored. If you
                say parquet, corresponding 'csv' files will still
                be parsed. Delete them if you really don't want them
            lazy_load (bool): If True, loading of dataframes from disk
                will be postponed until get_df() is actually called.
        """
        start_time = datetime.datetime.now()
        if fmt not in ["csv", "parquet"]:
            raise ValueError("Unknown format for from_disk: %s" % fmt)

        # Clear all data we have, we don't dare to merge VirtualEnsembles
        # with data coming from disk.
        self._data = {}
        self._name = None

        for root, _, filenames in os.walk(filesystempath):
            if "__discoveredfiles" in root:
                # Never traverse the collections of dumped
                # discovered files
                continue
            localpath = root.replace(filesystempath, "")
            if localpath and localpath[0] == os.path.sep:
                localpath = localpath[1:]
            for filename in filenames:
                # Special treatment of the filename "_name"
                if filename == "_name":
                    self._name = "".join(
                        open(os.path.join(root, filename), "r").readlines()
                    ).strip()

                if filename == "_manifest.yml":
                    self.manifest = os.path.join(root, "_manifest.yml")

                # We will loop through the directory structure, and
                # data will be duplicated as they can be both in csv
                # and parquet files. We will only load one of them if so.
                elif filename[-4:] == ".csv":
                    filebase = filename[:-4]
                    parquetfile = filebase + ".parquet"
                    # Treat special cases (!!!) NB: Code duplication below
                    if (
                        filebase[-4:] == ".txt"
                        or filebase[-6:] == "STATUS"
                        or filebase[-2:] == "OK"
                        or filebase[0:2] == "__"
                    ):
                        internalizedkey = os.path.join(localpath, filebase)
                    else:
                        internalizedkey = os.path.join(localpath, filebase + ".csv")
                    if fmt == "csv" or not os.path.exists(
                        os.path.join(root, parquetfile)
                    ):
                        self.lazy_frames[internalizedkey] = os.path.join(root, filename)

                elif filename[-8:] == ".parquet":
                    filebase = filename[:-8]
                    if (
                        filebase[-4:] == ".txt"
                        or filebase[-6:] == "STATUS"
                        or filebase[-2:] == "OK"
                        or filebase[0:2] == "__"
                    ):
                        internalizedkey = os.path.join(localpath, filebase)
                    else:
                        internalizedkey = os.path.join(localpath, filebase + ".csv")
                    if fmt == "parquet":
                        self.lazy_frames[internalizedkey] = os.path.join(root, filename)
                else:
                    logger.debug("from_disk: Ignoring file: %s", filename)

        if not lazy_load:
            # Load all found dataframes from disk:
            for internalizedkey, filename in self.lazy_frames.items():
                self._load_frame_fromdisk(internalizedkey, filename)
            self.lazy_frames = {}

        # This function must be called whenever we have done
        # something manually with the dataframes, like adding realizations.
        # IT MIGHT BE INCORRECT IF LAZY_LOAD...
        self.update_realindices()

        end_time = datetime.datetime.now()
        if lazy_load:
            lazy_str = "(lazy) "
        else:
            lazy_str = ""
        logger.info(
            "Loading ensemble from disk %stook %g seconds",
            lazy_str,
            (end_time - start_time).total_seconds(),
        )

    def _load_frame_fromdisk(self, key, filename):
        if filename.endswith(".parquet"):
            parsedframe = pd.read_parquet(filename)
            if self._isvalidframe(parsedframe, filename):
                self.data[key] = parsedframe
        else:
            parsedframe = pd.read_csv(filename)
            if self._isvalidframe(parsedframe, filename):
                self.data[key] = parsedframe

    def __repr__(self):
        """Textual representation of the object"""
        return "<VirtualEnsemble, {}>".format(self._name)

    def get_df(self, localpath):
        """Access the internal datastore which contains dataframes or dicts

        Shorthand is allowed, if the fully qualified localpath is

            'share/results/volumes/simulator_volume_fipnum.csv'
        then you can also get this dataframe returned with these alternatives:

          * simulator_volume_fipnum
          * simulator_volume_fipnum.csv
          * share/results/volumes/simulator_volume_fipnum

        but only as long as there is no ambiguity. In case of ambiguity, a
        ValueError will be raised.

        Args:
            localpath: the idenfier of the data requested

        Returns:
            dataframe or dictionary
        """
        inconsistent_lazy_frames = set(self.data.keys()).intersection(
            set(self.lazy_frames.keys())
        )
        if inconsistent_lazy_frames:
            logger.critical(
                "Internal error, inconsistent lazy frames:\n %s",
                str(inconsistent_lazy_frames),
            )
        if localpath in self.lazy_frames.keys():
            logger.warning("Loading %s from disk, was lazy", localpath)
            self._load_frame_fromdisk(localpath, self.lazy_frames[localpath])
            self.lazy_frames.pop(localpath)

        if localpath in self.data.keys():
            return self.data[localpath]

        # Allow shorthand, but check ambiguity
        allkeys = list(self.data.keys()) + list(self.lazy_frames.keys())
        basenames = [os.path.basename(x) for x in allkeys]
        if basenames.count(localpath) == 1:
            shortcut2path = {os.path.basename(x): x for x in allkeys}
            return self.get_df(shortcut2path[localpath])
        noexts = ["".join(x.split(".")[:-1]) for x in allkeys]
        if noexts.count(localpath) == 1:
            shortcut2path = {"".join(x.split(".")[:-1]): x for x in allkeys}
            return self.get_df(shortcut2path[localpath])
        basenamenoexts = ["".join(os.path.basename(x).split(".")[:-1]) for x in allkeys]
        if basenamenoexts.count(localpath) == 1:
            shortcut2path = {
                "".join(os.path.basename(x).split(".")[:-1]): x for x in allkeys
            }
            return self.get_df(shortcut2path[localpath])
        raise ValueError(localpath)

    def get_smry(self, column_keys=None, time_index="monthly"):
        """
        Function analoguous to the EclSum direct get'ters in ScratchEnsemble,
        but here we have to resort to what we have internalized.

        This will perform interpolation in each realizations data to
        the requested time_index, this is done by creating VirtualRealization
        object for all realizations, which can do the interpolation, and
        the result is merged and returned. This creates some overhead, so
        if you do not need the interpolation, stick with get_df() instead.
        """

        # Get a list ala ['yearly', 'daily']
        available_smry = [
            x.split("/")[-1].replace(".csv", "").replace("unsmry--", "")
            for x in self.keys()
            if "unsmry" in x
        ]

        if (
            isinstance(time_index, str) and time_index not in available_smry
        ) or isinstance(time_index, list):
            # Suboptimal code, we always pick the finest available
            # time resolution:
            priorities = ["raw", "daily", "monthly", "weekly", "yearly", "custom"]
            # (could also sort them by number of rows, or we could
            #  even merge them all)
            # (could have priorities as a dict, for example so we
            #  can interpolate from monthly if we ask for yearly)
            chosen_smry = ""
            for candidate in priorities:
                if candidate in available_smry:
                    chosen_smry = candidate
                    break
            if not chosen_smry:
                logger.error("No internalized summary data " + "to interpolate from")
                return pd.DataFrame()
        else:
            chosen_smry = time_index

        logger.info("Using %s for interpolation", chosen_smry)

        # Explicit creation of VirtualRealization allows for later
        # multiprocessing of the interpolation.
        # We do not use the internal function get_realization() because
        # that copies all internalized data, while we only need
        # summary data.

        smry_path = "unsmry--" + chosen_smry
        smry = self.get_df(smry_path)
        smry_interpolated = []
        for realidx in smry["REAL"].unique():
            vreal = VirtualRealization()
            # Inject the summary data for that specific realization
            vreal.append(smry_path, smry[smry["REAL"] == realidx])

            # Now ask the VirtualRealization to do interpolation
            interp = vreal.get_smry(column_keys=column_keys, time_index=time_index)
            # Assume we get back a dataframe indexed by the dates from vreal
            # We must reset that index, and ensure the index column
            # gets a correct name
            interp.index = interp.index.set_names(["DATE"])
            interp = interp.reset_index()
            interp["REAL"] = realidx
            smry_interpolated.append(interp)
        return pd.concat(smry_interpolated, ignore_index=True, sort=False)

    def get_smry_stats(self, column_keys=None, time_index="monthly", quantiles=None):
        """
        Function to extract the ensemble statistics (Mean, Min, Max, P10, P90)
        for a set of simulation summary vectors (column key).

        Compared to the agg() function, this function only works on summary
        data (time series), and will only operate on actually requested data,
        independent of what is internalized. It accesses the summary files
        directly and can thus obtain data at any time frequency.

        In a virtual ensemble, this function can only provide data it has
        internalized. There is no resampling functionality yet.

        Args:
            column_keys: list of column key wildcards. Defaults
                to match all available columns
            time_index: list of DateTime if interpolation is wanted
                default is None, which returns the raw Eclipse report times
                If a string is supplied, that string is attempted used
                via get_smry_dates() in order to obtain a time index.
            quantiles: list of ints between 0 and 100 for which quantiles
                to compute. Quantiles follow scientific standard,
                for the oil industry p10 you should ask for p90.
        Returns:
            A MultiIndex dataframe. Outer index is 'minimum', 'maximum',
            'mean', 'p10', 'p90', inner index are the dates. Column names
            are the different vectors. The column 'p10' represent the
            scientific p10, not the oil industry p10 for which you
            have to ask for p90.
        """
        if quantiles is None:
            quantiles = [10, 90]

        if column_keys is None:
            column_keys = "*"

        # Check validity of quantiles to compute:
        quantiles = list(map(int, quantiles))  # Potentially raise ValueError
        for quantile in quantiles:
            if quantile < 0 or quantile > 100:
                raise ValueError("Quantiles must be integers " + "between 0 and 100")

        # Obtain an aggregated dataframe for only the needed columns over
        # the entire ensemble. This will fail if we don't have the
        # time frequency already internalized.
        dframe = (
            self.get_smry(time_index=time_index, column_keys=column_keys)
            .drop(columns="REAL")
            .groupby("DATE")
        )

        # Build a dictionary of dataframes to be concatenated
        dframes = {}
        dframes["mean"] = dframe.mean()
        for quantile in quantiles:
            quantile_str = "p" + str(quantile)
            dframes[quantile_str] = dframe.quantile(q=1 - quantile / 100.0)
        dframes["maximum"] = dframe.max()
        dframes["minimum"] = dframe.min()

        return pd.concat(dframes, names=["STATISTIC"], sort=False)

    def get_volumetric_rates(
        self, column_keys=None, time_index="monthly", time_unit=None
    ):
        """Compute volumetric rates from internalized cumulative summary
        vectors

        Column names that are not referring to cumulative summary
        vectors are silently ignored.

        A Dataframe is returned with volumetric rates, that is rate
        values that can be summed up to the cumulative version. The
        'T' in the column name is switched with 'R'. If you ask for
        FOPT, you will get FOPR in the returned dataframe.

        Rates in the returned dataframe are valid **forwards** in time,
        opposed to rates coming directly from the Eclipse simulator which
        are valid backwards in time.

        If time_unit is set, the rates will be scaled to represent
        either daily, monthly or yearly rates. These will sum up to the
        cumulative as long as you multiply with the correct number
        of days, months or year between each consecutive date index.
        Month lengths and leap years are correctly handled.

        Args:
            column_keys: str or list of strings, cumulative summary vectors
            time_index: str or list of datetimes
            time_unit: str or None. If None, the rates returned will
                be the difference in cumulative between each included
                time step (where the time interval can vary arbitrarily)
                If set to 'days', 'months' or 'years', the rates will
                be scaled to represent a daily, monthly or yearly rate that
                is compatible with the date index and the cumulative data.

        """
        vol_rates_dfs = []
        for realidx in self.realindices:
            # Warning: This is potentially a big overhead
            # if a lot of non-summary-related data has been
            # internalized:
            vreal = self.get_realization(realidx)
            vol_rate_df = vreal.get_volumetric_rates(column_keys, time_index, time_unit)
            # Indexed by DATE, ensure index name is correct:
            vol_rate_df.index = vol_rate_df.index.set_names(["DATE"])
            vol_rate_df.reset_index(inplace=True)
            vol_rate_df["REAL"] = realidx
            vol_rates_dfs.append(vol_rate_df)
        return pd.concat(vol_rates_dfs, ignore_index=True, sort=False)

    @property
    def files(self):
        """Access the list of internalized files as they came from
        a ScratchEnsemble. Might be empty

        Return:
            pd.Dataframe. Empty if no files are meaningful"""
        files = self.get_df("__files")
        return files

    @property
    def manifest(self):
        """Get the manifest of the ensemble. The manifest
        is nothing but a dictionary with unspecified content

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
        elif isinstance(manifest, six.string_types):
            if os.path.exists(manifest):
                manifest_fromyaml = yaml.safe_load(open(manifest))
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
        """Quick access to parameters"""
        return self.get_df("parameters.txt")

    @property
    def name(self):
        """The name of the virtual ensemble as set during initialization"""
        return self._name

    @staticmethod
    def _isvalidframe(frame, filename):
        """Validate that a DataFrame we read from disk is acceptable
       as ensemble data. It must for example contain a column called
       REAL with numerical data"""
        if "REAL" not in frame.columns:
            logger.warning(
                "The column 'REAL' was not found in file %s - ignored", filename
            )
            return False
        if frame["REAL"].dtype != np.int64:
            logger.warning(
                (
                    "The column 'REAL' must contain "
                    "only integers in file %s  - ignored"
                ),
                filename,
            )
            return False
        if frame["REAL"].min() < 0:
            logger.warning(
                (
                    "The column 'REAL' must contain only "
                    "positive integers in file %s  - ignored"
                ),
                filename,
            )
            return False
        return True
