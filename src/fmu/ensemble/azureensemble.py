# -*- coding: utf-8 -*-
"""Module containing classes related to Azure and RMRC storage
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import yaml
import shutil
from timeit import default_timer as timer

import pandas as pd

from .etc import Interaction
from .ensemble import ScratchEnsemble, ScratchRealization

xfmu = Interaction()
logger = xfmu.functionlogger(__name__)

class AzureScratchRealization():
    """Class for handling indiviual realisations that are to be uploaded to the 
       RMrC storage on Azure.

       Class is initialized for a specific realisation, assembles an index of this
       particular realisation, to be concatenated by AzureScratchEnsemble.

    Args:
        path (path):                 Path to the realization. Will be given to ScratchRealization.
                                     Path must include the iteration, if iterations are used.
        realisation-id (int):        The ID of the realisation within an ensemble. Will be passed
                                     ScratchRealization through the index argument.
        searchpaths (list of paths): List of pats to feed to ScratchRealization.find_files().
                                     If not provided, will assume all pahts (except ignordirs)
        ignoredirs (list of str):    List of strings representing paths to ignore when searching
                                     for files.
        indexdir (path):             Root directory for placing the index dataframe. Will be made 
                                     if not present.

        
        Inconsistencies with rest of fmu-ensemble:
        - The term 'index' is used in ScratchRealization for representing the realization ID. Here,
          the index refers to the index of files in a realisation.


       """

    def __init__(self, path, realization_id, searchpaths=None, ignoredirs=None, indexdir='share/rmrc/index'):

        self._path = path
        self._searchpaths = searchpaths
        self._ignoredirs = ignoredirs
        self._realization_id = realization_id
        self._indexdir = indexdir
        self._index = None

        if 'realization-{}'.format(realization_id) not in path:
            logger.critical('Incosistency between path and realization ID. This looks very wrong, so crashing.')
            raise ValueError('Realization ID is {}, but that looks wrong given the realization path: {}'.format(realization_id, path))

        if not os.path.isdir(path):
            logger.critical('Given path is not valid: {}'.format(path))
            raise IOError('Path not valid')

        self._indexpath = self.make_indexpath(self._path, self._indexdir)

        self.scratchrealization = ScratchRealization(path=self._path, index=self._realization_id)

        if self._searchpaths is None:
            # assume all files to be indexed
            self._searchpaths = self.list_subdirectories(self._path)
            self._searchpaths = self.remove_ignored_dirs(self._searchpaths, self._ignoredirs)
            self._searchpaths = self.make_searchpaths_scratchrealization_compatible(self._searchpaths)

        else:

            if isinstance(self._searchpaths, str):
                self._searchpaths = [self._searchpaths]
            elif isinstance(self._searchpaths, list):
                pass
            else:
                raise ValueError('Non-valid searchpaths given')

        self.virtualrealization = self.scratchrealization.to_virtual()

    @property
    def index(self):
        """Returns the index as a dataframe
        NOTE! index in this setting refers to the list of files, not the realization number
        """

        return self._index

    def list_subdirectories(self, rootpath):
        """Given a rootpath, return all subdirectories recursively as list"""
        subdirs = [r.replace(rootpath, '') for r, _d, _f in os.walk(rootpath)]

        return subdirs


    def remove_ignored_dirs(self, subdirs, ignoredirs):
        """Given list A and list B, remove all elements in list A that
           starts with any of the elements in list B"""

        keepers = []

        if ignoredirs is not None:
            ignoredirs = tuple(['/' + d if not d.startswith('/') else d for d in ignoredirs])

            for subdir in subdirs:
                if not subdir.startswith(ignoredirs):
                    keepers.append(subdir)
        else:
            keepers = subdirs

        # remove zero-length entries
        keepers = [k for k in keepers if len(k) > 0]

        return keepers

    def make_searchpaths_scratchrealization_compatible(self, searchpaths):
        """ScratchRealization requires searchpaths relative to the realization directory"""

        compatible_paths = []

        for searchpath in searchpaths:
            if searchpath.startswith('/'):
                searchpath = searchpath[1:]
            compatible_paths.append('{}/*.*'.format(searchpath))

        return compatible_paths

    def compile_index(self, searchpaths):
        """Search all files in current realisation, index them. For now
           overwrite the index if already present. Perhaps smarter solutions
           can be added in the future."""

        self.scratchrealization.find_files(searchpaths, metayaml=True)

        # do not return .files directly, to open for possibility of
        # adding more stuff to it before returning
        index = self.scratchrealization.files

        return index

    def make_indexpath(self, path, indexdir):
        """Check/create the directory where index will be dumped"""

        fullpath = os.path.join(path, indexdir)

        if not os.path.isdir(fullpath):
            logger.warning('Directory does not exist. Creating {}'.format(fullpath))
            os.makedirs(fullpath)
            return fullpath
        else:
            return fullpath

        logger.critical('AzureScratchRealization.make_indexpath() made it to the end without returning. That is strange.')
        raise ValueError('Unexpected error')


    def dump_index(self, fname='index.csv'):
        """Dump the index to file in csv format"""
        savepath = os.path.join(self._indexpath, fname)
        self._index.to_csv(savepath, index=False)
        logger.info('Index dumped to {}'.format(savepath))

        return savepath        



class AzureScratchEnsemble():
    """Class for handling ensembles that are to be uploaded to the RMrC
       storage on Azure. Class is initialized in an ensemble-centric way,
       initializes a ScratchEnsemble object, which in turn initializes a
       VirtualEnsemble object.

       This class wraps around ScratchEnsemble and VirtualEnsemble.

    Args:
        ensemble_name (str):         Name of ensemble. Will be passed to ScratchEnsemble
        ensemble_path (path):        Path to root of ensemble
        iter_name (str):             Name of iteration. If not included, will assume no iteration.
        premade_index (bool):        If True, do not search for files, use premade indexes per realisation
        premade_index_path (path):   Path to premade-index. Default: 'share/rmrc/index/index.csv'
        manifest (path/dict):        Path to manifest file (yaml) OR actual manifest as dict
        searchpaths (list of paths): List of paths to feed to ScratchEnsembe.find_files(). If
                                     not provided, will assume all paths (except ignoredirs)
        ignoredirs (list of str):    List of local directories to ignore when not giving searchpaths.
        dumpparquet (bool):          Temporary argument to bypass use of parquet


    Example:

        ensemble_path='/path/to/ensemble/root/on/scratch'
        iter_name = 'iter-0'
        manifest_path='share/runinfo/runinfo.yaml' (to be generalized at some point)
        ignoredirs = ['sim2seis', 'rms', 'eclipse', 'cohiba', 'bin', 'config']

        # initialize the AzureScratchEnsemble
        azureens = ensemble.AzureScratchEnsemble('MyEns',
                                                 ensemble_path=ensemble_path,
                                                 iter_name=iter_name,
                                                 manifest=manifest_path,
                                                 ignoredirs=ignoredirs,
                                                 premade_index=True,
                                                 dumpparquet=False
                                                 )
        # upload the ensemble
        azureens.upload()

    """

    def __init__(self,
        ensemble_path=None,
        iter_name=None,
        premade_index=False,
        premade_index_path=None,
        manifest=None,
        searchpaths=None,
        ignoredirs=None,
        dumpparquet=False
        ):

        # create a unique ID for this instance
        self._id = 'rmrc-{}'.format(self.make_unique_string())
        self._name = None

        if ensemble_path is not None:
            self._ensemble_path = self.confirm_ensemble_path(ensemble_path)
        else:
            self._ensemble_path = None

        if iter_name is not None:
            self._iter_name = iter_name
        else:
            logger.info('Iteration name not given, assuming no iteration')
            self._iter_name = ''

        if isinstance(manifest, str):
            # assume path to manifest is provided, try to parse it
            self._manifest = self.confirm_manifest_path(manifest)
            self._manifest = self.parse_manifest(self._manifest)
        elif isinstance(manifest, dict):
            self._manifest = manifest
        else:
            logger.error('A manifest must be provided')

        self._index = None

        self._tmp_storage_path = None
        self._tmp_stored_on_disk = False
        self._searchpaths = searchpaths
        self._ignoredirs = ignoredirs
        self._premade_index = premade_index
        self._dumpparquet = dumpparquet

        if premade_index:
            if premade_index_path is None:
                self._premade_index_path = 'share/rmrc/index/index.csv'
            else:
                self._premade_index_path = premade_index_path

        if self._searchpaths is None:
            # assume all files to be indexed

            logger.info('No searchpaths given, so assuming all files to be indexed')

            if self._ensemble_path is not None:

                # Assume that all realizations have identical structures
                rootdir = os.path.join(self._ensemble_path, 'realization-0/')
                if self._iter_name is not '':
                    rootdir = os.path.join(rootdir, self._iter_name)

                self._searchpaths = self.list_subdirectories(rootdir)
                self._searchpaths = self.remove_ignored_dirs(self._searchpaths, self._ignoredirs)
                self._searchpaths = self.make_searchpaths_scratchensemble_compatible(self._searchpaths)

        else:

            if self._premade_index:
                logger.warning('searchpaths will be ignored, as premade index will be used.')

            if isinstance(self._searchpaths, str):
                self._searchpaths = [self._searchpaths]
            elif isinstance(self._searchpaths, list):
                pass
            else:
                raise ValueError('Non-valid searchpaths given')

        # create a scratchensemble
        self.scratchensemble = self.build_scratchensemble(self.name,
                                                          self._ensemble_path,
                                                          )

        if not self._premade_index:

            logger.debug('No premade index, running find_files')
            self.scratchensemble.find_files(self._searchpaths, metayaml=True)
            if not len(self.scratchensemble) > 0:
                logger.debug('Length of scratchensemble is 0, returning now')
                logger.warning('ScratchEnsemble returned with no content')
                return

            # create a VirtualEnsemble
            self.virtualensemble = self.scratchensemble.to_virtual()

            # add smry data
            #logger.info('Getting eclipse summary for all realisations')
            #smry = self.scratchensemble.get_smry()

            #self.virtualensemble.append('smry', smry)

            # add x

            # add y

            self._index = self.create_index()

        else:

            # pre-compiled index
            self._index = self.compile_premade_index()

            # create a VirtualEnsemble
            self.virtualensemble = self.scratchensemble.to_virtual()

            # add smry data
            smry = self.scratchensemble.get_smry()
            logger.info('Getting eclipse summary for all realisations')
            self.virtualensemble.append('smry', smry)

        logger.debug('__init__ done')


    def make_searchpaths_scratchensemble_compatible(self, searchpaths):
        """ScratchEnsemble requires searchpaths relative to the realization directory"""

        compatible_paths = []

        for searchpath in searchpaths:
            if searchpath.startswith('/'):
                searchpath = searchpath[1:]
            compatible_paths.append('{}/*.*'.format(searchpath))

        return compatible_paths


    def upload(self, tmp_storage_root='/tmp', symlinks=True):
        """Dump ensemble to temporary storage on format as in rmrc,
           ready for upload.

        TODO: Add actual upload

        """

        starttime = timer()

        if not len(self.scratchensemble) > 0:
            logger.warning('ScratchEnsemble has zero-length. Nothing to upload.')
            return

        tmp_storage_path = self.create_tmp_storage_folder(tmp_storage_root)
        self._tmp_storage_path = tmp_storage_path

        self.prepare_blob_structure(tmp_storage_path)

        # DATA section
        logger.info('Copying files')
        self.copy_files(tmp_storage_path, symlinks=symlinks)
        self.dump_internalized_dataframes(tmp_storage_path)

        ## Alternatively, use virtualensemble.to_disk()
        ## - Have to include additional files (dumped dataframes) directly to .files, not to .index
        ## - Have to include STOREDPATH directly to .files, not in the .index derivative
        ##   Currently, .files is used to initiate the index, but further operations happen on .index, not .files

        #self.virtualensemble.to_disk(os.path.join(tmp_storage_path, 'data'), dumpparquet=False)




        # MANIFEST section
        logger.info('Dumping manifest')
        self.dump_manifest(tmp_storage_path)

        # INDEX section
        logger.info('Dumping index')
        self.dump_index(tmp_storage_path)

        self._tmp_stored_on_disk = True

        elapsedtime = timer() - starttime

        print('*'*50)
        print('Upload done.')
        print('Wall time: {} seconds'.format(round(elapsedtime, 1)))
        print('Tmp storage: {}'.format(self._tmp_storage_path))
        print('')
        print('')

        # TODO authentication towards azure go here, returns a token
        # TODO some checks towards the storage (already existing ensemble, etc) goes here
        # TODO upload function with the returned token go here
        # TODO some confirmation functions (just API calls?) go here

    def create_tmp_storage_folder(self, tmp_storage_root):
        """Make random folde name under the storage root, confirm that it
           is not in use, make it, return the path """

        if not os.path.isdir(tmp_storage_root):
            logger.critical('Non-valid temporary storage path given: {}'.format(tmp_storage_root))
            raise IOError('Not a valid temporary storage: {}'.format(tmp_storage_root))

        unique_folder_name = '{}'.format(self._id)

        tmp_storage_path = os.path.join(tmp_storage_root, unique_folder_name)
        if os.path.exists(tmp_storage_path):
            logger.critical('temporary storage path already exists: {}'.format(tmp_storage_path))

        # got a unique folder name, now create it
        os.mkdir(tmp_storage_path)

        return tmp_storage_path

    def make_unique_string(self):
        """Return unique string, 6 characters, lowercase"""

        import uuid

        unique = str(uuid.uuid4())[0:6].lower()
        
        return unique


    @property
    def name(self):
        """Name of ensemble. Parsed from the manifest."""

        if self._name is None:
            self._name = self.manifest.get('case', {}).get('case', None)
        if self._name is None:
            # Well that failed, so make something up
            self._name = "NoName-{}".format(self._id)

        self._name = self._name
        return self._name

    def __len__(self):
        return len(self.scratchensemble._realizations)


    @property
    def manifest(self):
        """Returns the internally stored manifest (dict)"""
        return self._manifest


    @property
    def tmp_storage(self):
        """If ensemble is stored on disk, return the path. Otherwise, return None."""
        if self._tmp_stored_on_disk:
            return self._tmp_storage
        return None


    def list_subdirectories(self, rootpath):
        """Given a rootpath, return all subdirectories recursively as list"""
        subdirs = [r.replace(rootpath, '') for r, _d, _f in os.walk(rootpath)]

        return subdirs


    def remove_ignored_dirs(self, subdirs, ignoredirs):
        """Given list A and list B, remove all elements in list A that
           starts with any of the elements in list B"""

        keepers = []

        if ignoredirs is not None:
            ignoredirs = tuple(['/' + d if not d.startswith('/') else d for d in ignoredirs])

            for subdir in subdirs:
                if not subdir.startswith(ignoredirs):
                    keepers.append(subdir)
        else:
            keepers = subdirs

        # remove zero-length entries
        keepers = [k for k in keepers if len(k) > 0]

        return keepers


    def confirm_ensemble_path(self, path):
        """QC function for checking that the given
           ensemble path is a valid path.

           TODO: Also check that it's a valid ensemble?
        """

        if os.path.exists(path):
            return path

        raise ValueError('Not a valid ensemble path: {}'.format(path))


    def confirm_manifest_path(self, fname):
        """Given a path that could be local or global, confirm
           the path of the manifest, return functioning path"""

        # check if local or global path was given
        if os.path.isabs(fname):
            if not fname.startswith(self._ensemble_path):
                logger.error('Absolute path was given for manifest, but was not' +
                             'part of the same ensemble.')
                raise ValueError('Illegal manifest path given (#1)')

            if not os.path.isfile(fname):
                logger.error('Absolute path given for manifest, but could not find file')
                logger.error(fname)
                raise ValueError('Illegal manifest path given (#2)')
            else:
                return fname

        else:
            #check if path is a valid local path
            candidate = os.path.join(self._ensemble_path, fname)
            if os.path.isfile(candidate):
                logger.info('Think I found the manifest: {}'.format(candidate))
                return candidate
            candidate = os.path.join(self._ensemble_path, self._iter_name, fname)
            if os.path.isfile(candidate):
                logger.info('Think I found the manifest: {}'.format(candidate))
                return candidate

        raise ValueError('Illegal manifest path given (#3)')


    def parse_manifest(self, fname):
        """Parse the manifest from yaml, return as dict"""
 
        if not os.path.exists(fname):
            logger.warning('Could not find manifest in this location: {}'.format(fname))
            return None

        with open(fname, 'r') as stream:
            manifest = yaml.load(stream, Loader=yaml.FullLoader)

        return manifest

    def build_scratchensemble(self, ensemble_name, ensemble_path):
        """Initialize and return a ScratchEnsemble object"""

        if ensemble_path is None:
            return ScratchEnsemble(ensemble_name)

        paths = '{}/realization-*/{}'.format(ensemble_path, self._iter_name)
        manifest = self._manifest

        scratchensemble = ScratchEnsemble(ensemble_name,
                                          paths=paths)

        return scratchensemble


    def prepare_blob_structure(self, filesystempath, delete=False):
        """Prepare a directory for dumping a virtual ensemble.

        The end result is either an error, or a clean empty directory
        at the requested path"""
        if os.path.exists(filesystempath):
            if delete:
                shutil.rmtree(filesystempath)
                os.mkdir(filesystempath)
            else:
                if os.listdir(filesystempath):
                    logger.critical(
                        "Refusing to write virtual ensemble "
                        + " to non-empty directory"
                    )
                    raise IOError("Directory {} not empty".format(filesystempath))
        else:
            os.mkdir(filesystempath)

        subfolders = ['data', 'data/share', 'manifest', 'index']
        #subfolders = ['data', 'manifest', 'index']

        for subfolder in subfolders:
            if not os.path.exists(os.path.join(filesystempath, subfolder)):
                os.mkdir(os.path.join(filesystempath, subfolder))


    def copy_files(self, filesystempath, localdir="data", symlinks=True):
        """Symlink (or copy) files to temporary storage location, update the index"""


        storedpaths = []

        if not symlinks:
            print('Copying files to temporary storage. This will take some time.')

        for src_fpath, real, localpath in zip(self._index['FULLPATH'], self._index['REAL'], self._index['LOCALPATH']):
            storedpath = os.path.join('realization-' + str(real), localpath)
            dest_fpath = os.path.join(
                    filesystempath,
                    localdir,
                    storedpath)

            directory = os.path.dirname(dest_fpath)
            
            if not os.path.exists(directory):
                os.makedirs(os.path.dirname(dest_fpath))

            if not symlinks:
                shutil.copy(src_fpath, dest_fpath)
            else:
                os.symlink(src_fpath, dest_fpath)

            storedpaths.append(storedpath)

        self._index['STOREDPATH'] = storedpaths


    def dump_manifest(self, filesystempath, localdir="manifest"):
        """Dump the internalized ensemble manifest to disk"""

        # initialise it with some values we might want to inject
        compiled_manifest = {'rmrc' : {'name' : self.name,
                                       'id'   : self._id,
                                       }
                            }

        if self._manifest is not None:
            compiled_manifest.update(self._manifest)

        dest_fpath = os.path.join(
            filesystempath,
            localdir,
            'manifest.yml')

        with open(dest_fpath, 'w+') as outfile:
            yaml.dump(compiled_manifest, outfile, default_flow_style=False)

        logger.info('yaml dumped')

    def create_index(self):
        """Assemble index as dataframe

        TODO: Explore possibility of using json
        TODO: Explore extension of metadata fields
        """

        index = self.virtualensemble.files

        return index


        # parse metadata if present for each file, append to the index.
        # should be code for this elsewhere...


    def compile_premade_index(self):
        """Concatenate index files for all realisation in self.scratchensemble"""

        dfs = []
        for real in self.scratchensemble._realizations.keys():
            path = os.path.join(self._ensemble_path, 'realization-{}'.format(real), self._iter_name, self._premade_index_path)
            df_this_real = pd.read_csv(path)
            df_this_real['REAL'] = real
            dfs.append(df_this_real)

        df = pd.concat(dfs)

        return df


    def dump_index(self, filesystempath, localdir="index", indexfname="index.csv"):
        """Dump the index to disk"""

        indexpath = os.path.join(filesystempath, localdir, indexfname)

        self._index.to_csv(indexpath, index=False)
        
        logger.info('index dumped')


    def dump_internalized_dataframes(self, filesystempath, localdir="data/share", fformat=None):
        """Dump dataframes carried by the ScratchEnsemble object for tabular
           data to disk. Append to index.

           Args:
                filesystempath (path): Absolute path to temporary storage location usually on /tmp/
                localdir (path): Relative path inside the package for storing the dumped data
                fformat (str): Allowed file format ('csv' or 'parquet') for forcing file format. If None,
                               format will be derived from looking at the data.

        """

        def derive_fformat(df):
            """Look at the data, decide which format to use, return format as string"""
            if len(df) > 1000 and self._dumpparquet:
                return 'parquet'
            return 'csv'

        import pyarrow

        allowed_fformats = ['csv', 'parquet', None]

        if not fformat in allowed_fformats:
            logger.critical('dump_internalized_dataframes() was given {} as the fformat argument. Not allowed.'.format(fformat))
            raise ValueError('Illegal fformat: {}. Allowed formats: {}'.format(fformat, str(allowed_fformats)))

        for key in self.virtualensemble.data:
            if not key.startswith('__'):
                filebase = str(key)
                localpath = os.path.join(localdir, filebase)

                df = self.virtualensemble.data[key]

                if fformat is None:
                    use_fformat = derive_fformat(df)
                    logger.info('Working with filebase. Derived format is {}'.format(use_fformat))
                else:
                    use_fformat = fformat
                    logger.info('fformat was predefined to {}'.format(fformat))

                if use_fformat == 'csv':
                    savepath = os.path.join(filesystempath, localpath+'.'+use_fformat)
                    df.to_csv(savepath, index=False)
                elif use_fformat == 'parquet':
                    savepath = os.path.join(filesystempath, localpath+'.'+use_fformat)
                    df.to_parquet(savepath, index=False, engine="auto")
                else:
                    ValueError('Non-valid file format: {}'.format(use_fformat))

                # add reference to the index
                df = pd.DataFrame({'LOCALPATH' : [localpath+'.'+use_fformat],
                                   'FILETYPE' : [use_fformat],}
                                   )
                self._index = self._index.append(df, sort=False)

                print('added to index:')
                print(key)

                logger.info('dumped {}'.format(savepath))


