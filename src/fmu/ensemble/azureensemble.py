# -*- coding: utf-8 -*-
"""Module containing classes related to Azure and RMRC storage
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import yaml
import shutil

import pandas as pd

from .etc import Interaction
from .ensemble import ScratchEnsemble

xfmu = Interaction()
logger = xfmu.functionlogger(__name__)

class AzureScratchEnsemble():
    """Class for handling ensembles that are to be uploaded to the RMrC
       storage on Azure. Class is initialized in an ensemble-centric way,
       initializes a ScratchEnsemble object, which in turn initializes a
       VirtualEnsemble object

    Args:
        ensemble_name (str): Name of ensemble. Will be passed to ScratchEnsemble
        ensemble_path (path): Path to root of ensemble
        iter_name (str): Name of iteration. If not included, will assume no iteration.
        manifest (path/dict): Path to manifest file (yaml) OR actual manifest as dict
        searchpaths (list of paths): List of paths to feed to ScratchEnsembe.find_files(). If
                                     not provided, will assume all paths (except ignoredirs)
        ignoredirs (list of str): List of local directories to ignore when not giving searchpaths.


    Examples:

        searchpaths = ['share/maps/depth/VIKING_Top*.gri',
                       'eclipse/model/2019*.UNSMRY']

        ignoredirs = ['rms/model', 'sim2seis', 'cohiba', 'config', 'eclipse/include', 'share']

    ensemble_path='/scratch/johan_sverdrup2/rmrc/ens_full_5/'
    iter_name = 'pred'
    manifest_path='/scratch/johan_sverdrup2/rmrc/ens_full_5/pred/share/runinfo/runinfo.yaml'

    tmp_storage_path='/scratch/johan_sverdrup2/rmrc/ens_full_5_azureprep15/'

    from fmu import ensemble

    myens = ensemble.AzureScratchEnsemble('MyEns', 
                            ensemble_path='/path/to/ensemble/on/scratch, 
                            iter_name='pred', 
                            manifest='/', 
                            ignoredirs=ignoredirs,
                            )

    azureens.upload('/tmp/storage/location/')




    TODO:
        - tmp_storage_path to be replaced by a /tmp/<random>

    """

    def __init__(self,
        name,
        ensemble_path=None,
        iter_name=None,
        manifest=None,
        searchpaths=None,
        ignoredirs=None,
        ):

        self._ensemble_name = name

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

        self._tmp_stored_on_disk = False
        self._index = None
        self._searchpaths = searchpaths
        self._ignoredirs = ignoredirs

        if self._searchpaths is None:
            # assume all files to be indexed

            if self._ensemble_path is None:
                self._searchpaths = None
            else:
                print('All paths')
                # Assume that all realizations have identical structures
                rootdir = os.path.join(self._ensemble_path, 'realization-0/')
                if self._iter_name is not '':
                    rootdir = os.path.join(rootdir, self._iter_name)
                self._searchpaths = self.list_subdirectories(rootdir)

                self._searchpaths = self.remove_ignored_dirs(self._searchpaths, self._ignoredirs)

        else:
            print('Using searchpaths')
            if isinstance(self._searchpaths, str):
                self._searchpaths = [self._searchpaths]
            else:
                pass

        # create a scratchensemble
        self.scratchensemble = self.build_scratchensemble(self._ensemble_name,
                                                          self._ensemble_path,
                                                          self._searchpaths)


        if len(self.scratchensemble) > 0:
            # create a VirtualEnsemble
            self.virtualensemble = self.scratchensemble.to_virtual()

            # add smry data
            smry = self.scratchensemble.get_smry()
            self.virtualensemble.append('smry', smry)   # shared is indicating that data goes across realizations

            # add x

            # add y

            self.index = self.create_index()

            print('OK')


    def upload(self, tmp_storage_path):
        """Dump ensemble to temporary storage on format as in rmrc,
           ready for upload.

        TODO: Add upload function

        """

        ########### DEV ONLY ############
        if not isinstance(tmp_storage_path, str):
            raise ValueError('tmp_storage_path must be provided')
        
        #################################


        self.prepare_blob_structure(tmp_storage_path)

        # DATA section
        self.copy_files(tmp_storage_path)
        self.dump_internalized_dataframes(tmp_storage_path)

        # MANIFEST section
        self.dump_manifest(tmp_storage_path)

        # INDEX section
        self.dump_index(tmp_storage_path)

        self._tmp_stored_on_disk = True

        print('OK')

        # TODO authentication towards azure go here, returns a token
        # TODO some checks towards the storage (already existing ensemble, etc) goes here
        # TODO upload function with the returned token go here
        # TODO some confirmation functions (just API calls?) go here


    @property
    def name(self):
        return self._ensemble_name

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
        print('searching for subdirs')
        subdirs = [r.replace(rootpath, '') for r, _d, _f in os.walk(rootpath)]

        print('found {} subdirs in total'.format(len(subdirs)))
        return subdirs

    def remove_ignored_dirs(self, subdirs, ignoredirs):
        """Given list A and list B, remove all elements in list A that
           starts with any of the elements in list B"""

        keepers = []

        if isinstance(ignoredirs, list):

            print('ignoredirs before: {}'.format(ignoredirs))
            ignoredirs = tuple(['/' + d if not d.startswith('/') else d for d in ignoredirs])
            print('ignoredirs after: {}'.format(ignoredirs))

            for subdir in subdirs:
                if not subdir.startswith(ignoredirs) and len(subdir) > 0:
                    keepers.append(subdir)

            print('Removed subdirs, left with {}'.format(len(keepers)))
            print(keepers)
            return keepers
        else:
            print('No ignoredirs, still have {} subdirs'.format(len(subdirs)))
            return subdirs


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

        print('checking for manifest')

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

    def build_scratchensemble(self, ensemble_name, ensemble_path, searchpaths):
        """Initialize and return a ScratchEnsemble object"""

        if ensemble_path is None:
            return ScratchEnsemble(ensemble_name)

        paths = '{}/realization-*/{}'.format(ensemble_path, self._iter_name)
        manifest = self._manifest

        scratchensemble = ScratchEnsemble(ensemble_name,
                                          paths=paths)

        for searchpath in searchpaths:
            #print('searching {}'.format(searchpath))
            scratchensemble.find_files(os.path.join(searchpath, '*.*'), metayaml=True)
            #print(len(scratchensemble.files))

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

        for subfolder in subfolders:
            if not os.path.exists(os.path.join(filesystempath, subfolder)):
                os.mkdir(os.path.join(filesystempath, subfolder))


    def get_unique_foldername(self, prefix="/tmp/"):
        """Return the prefix + a unique folder name"""
        import uuid

        unique_folder = os.path.join(prefix, str(uuid.uuid4()))

        # double-check that this worked, call it again if not
        if os.path.exists(unique_folder):
            unique_folder = self.get_unique_foldername()

        return unique_folder


    def copy_files(self, filesystempath, localdir="data"):

        # code from .to_disk()
        for _, filerow in self.scratchensemble.files.iterrows():
            src_fpath = filerow["FULLPATH"]
            dest_fpath = os.path.join(
                filesystempath,
                localdir,
                "realization-" + str(filerow["REAL"]),
                filerow["LOCALPATH"],
            )
            directory = os.path.dirname(dest_fpath)
            if not os.path.exists(directory):
                os.makedirs(os.path.dirname(dest_fpath))
            shutil.copy(src_fpath, dest_fpath)


    def dump_manifest(self, filesystempath, localdir="manifest"):
        """Dump the internalized ensemble manifest to disk"""

        # initialise it with some values we might want to inject
        compiled_manifest = {'rmrc' : {'name' : self.name,
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

        print('yaml dumped')

    def create_index(self):
        """Assemble index as dataframe

        TODO: Explore possibility of using json
        TODO: Explore extension of metadata fields
        """

        index = self.virtualensemble.files

        return index


        # parse metadata if present for each file, append to the index.
        # should be code for this elsewhere...


    def dump_index(self, filesystempath, localdir="index", indexfname="index.csv"):
        """Dump the index to disk"""

        indexpath = os.path.join(filesystempath, localdir, indexfname)

        self.index.to_csv(indexpath, index=False)
        
        print('index dumped')


    def dump_internalized_dataframes(self, filesystempath, localdir="data/share"):
        """Dump dataframes carried by the ScratchEnsemble object for tabular
           data to disk. Append to index."""

        print('dumping internal dataframes')

        for key in self.virtualensemble.data:
            if not key.startswith('__'):

                fname = str(key) + '.csv'
                localpath = os.path.join(localdir, fname)
                savepath = os.path.join(filesystempath, localpath)
                self.virtualensemble.data[key].to_csv(savepath, index=False)

                # add reference to the index
                df = pd.DataFrame({'LOCALPATH' : [localpath],
                                   'FILETYPE' : ['csv'],}
                                   )
                self.index = self.index.append(df, sort=False)

                print('dumped {}'.format(savepath))


