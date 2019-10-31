# -*- coding: utf-8 -*-
"""Module containing classes related to Azure and RMRC storage
"""


class AzureScratchEnsemble():
    """Class for handling ensembles that are to be uploaded to the RMrC
       storage on Azure. Class is initialized in an ensemble-centric way,
       initializes a ScratchEnsemble object, which in turn initializes a
       VirtualEnsemble object

    Args:
        ensemble_name (str): Name of ensemble. Will be passed to ScratchEnsemble
        ensemble_path (path): Path to root of ensemble
        manifest_path (path): 

    TODO:
        - tmp_storage_path to be replaced by a /tmp/<random>

    """

    def __init__(self,
        ensemble_name,
        ensemble_path,
        iter_name,
        manifest_path,
        tmp_storage_path,        # DEV ONLY, will be replaced
        searchpaths=None,
        ):

        self._ensemble_name = ensemble_name
        self._ensemble_path = self.confirm_ensemble_path(ensemble_path)
        self._iter_name = iter_name
        self._manifest = self.parse_manifest(manifest_path)

        self._index = None

        if searchpaths is None:
            # assume all files to be indexed
            # for now just a dummy path
            searchpaths = ['share/maps/*/*.*', 
                           #'share/maps/isochores/*.*',
                           #'share/maps/recoverables/*.*'
                          ]
        else:
            if isinstance(searchpaths, str):
                searchpaths = [searchpaths]
            else:
                pass

        # create a scratchensemble
        self.scratchensemble = self.build_scratchensemble(self._ensemble_name,
                                                          self._ensemble_path,
                                                          searchpaths)

        # create a VirtualEnsemble
        vens = self.scratchensemble.to_virtual()

        # add smry data
        smry = self.scratchensemble.get_smry()
        vens.append('shared--smry', smry)   # shared is indicating that data goes across realizations

        # add x

        # add y
        
        # Dump to disk ready for upload
        prepare_blob_structure(temporary_filesystempath)

        # DATA section
        copy_files(temporary_filesystempath)
        dump_internalized_dataframes(temporary_filesystempath)

        # MANIFEST section
        dump_manifest(temporary_filesystempath)

        # INDEX section
        self.index = self.vens.files()
        self.dump_index(temporary_filesystempath)

        print('OK')

        # TODO authentication towards azure go here, returns a token
        # TODO some checks towards the storage (already existing ensemble, etc) goes here
        # TODO upload function with the returned token go here
        # TODO some confirmation functions (just API calls?) go here

    @property
    def manifest(self):
        return self._manifest


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
                    raise IOError("Directory %s not empty" % filesystempath)
        else:
            os.mkdir(filesystempath)
            os.mkdir(os.path.join(filesystempath, "data"))
            os.mkdir(os.path.join(filesystempath, "data/share"))
            os.mkdir(os.path.join(filesystempath, "manifest"))
            os.mkdir(os.path.join(filesystempath, "index"))

    def copy_files(self, filesystempath, localdir="data"):

        # code from .to_disk()
        for _, filerow in self.files.iterrows():
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
        compiled_manifest = {'rmrc' : {'SomeKey' : 'SomeValue',
                                       'SomeOtherKey' : 'SomeValue'
                                       }
                            }

        if self._manifest is not None:
            compiled_manifest.update(self._manifest)

        dest_fpath = os.path.join(
            filesystempath,
            localdir,
            'manifest.yaml')

        with open(dest_fpath, 'w+') as outfile:
            yaml.dump(compiled_manifest, outfile, default_flow_style=False)

        print('yaml dumped')

    def create_index(self, filesystempath, localdir="index", indexfname="index.csv"):
        """Assemble index as dataframe

        TODO: Explore possibility of using json
        TODO: Explore extension of metadata fields
        """

        indexpath = os.path.join(filesystempath, localdir, indexfname)
        return 


        # parse metadata if present for each file, append to the index.
        # should be code for this elsewhere...



    def dump_index(self, filesystempath, localdir="index", indexfname="index.csv"):
        """Dump the index to disk"""
        self.files.to_csv(indexpath, index=False)
        print('index dumped')


    def dump_internalized_dataframes(self, filesystempath, localdir="data/share"):
        """Dump dataframes carried by the ScratchEnsemble object for tabular
           data to disk. Return information that must be appended to index."""

        print('dumping internal dataframes')

        for key in self.data:
            fname = os.path.join(filesystempath, localdir, str(key)+'.csv')
            print('dumping {}'.format(fname))
            self.data[key].to_csv(fname, index=False)

        # return index info here | or print to __files?



