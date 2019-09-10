# -*- coding: utf-8 -*-
"""Contains the Surface class"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import pandas as pd
import numpy as np
import xtgeo
import yaml
import collections

from .etc import Interaction
#from etc import Interaction

fmux = Interaction()
logger = fmux.basiclogger(__name__)


########## WORK IN PROGRESS - CONCEPTUAL DISCUSSIONS ONLY #################



class EnsembleSurface:
    """Representation of a surface from FMU represented by multiple realizations
    and statistical surfaces with functions for expanding the data with statistical
    representations if not present.

    Surface() is initialized with information about the surface, including paths to datafiles
    describing the surface (realizations, aggregations):

    Args:
        metadata: Required. Must at least contain 'name'. Dictionary of metadata that should follow the surface.
        realizations: Optional. Dictionary of realizations with realization number as key, filepath to
                      realization surface as value. Default: None
        aggregations: Optional: Dictionary of aggregations with type as key (string), filepath to
                      realizastion surface as value. Default: None
        force_calculate: Optional, default: False. If True, force recalculation and replacement of
                         aggregated data even if they exist on file already.
        file_format: Optional. Default: 'irap_binary'. For now the class will only support irap binary.

    Example usage:
        >>> metadata = {'name' : 'MySurfaceName', 'source' : 'SomeSource', 'SomeMetaData' : 'SomeMetaDataValue'}
        >>> realizations = {1 : 'Path/to/file', 
                        2 : 'path/to/file'
                        }
        >>> aggregations = {'mean' : 'path/to/file'}

        >>> s = EnsembleSurface(metadata, realizations, aggregations)
        >>> s.mean(force_calculate=False)


    Concepts:
        ## Surface
        A specific surface (depth surfaces, isochore, etc) from FMU that can be represented by
        realizations and/or statistical aggregations and that has a defined set of metadata.
        Note that this means that a surface in this context is generally not something that is 
        stored in one single file*. Surface in this context is similar to what a Horizon is in RMS**, 
        but that term is not used further here due to it's implicit relationship with stratigraphy. 
        A Surface in this context can be related to stratigraphy, or independent of stratigraphy.

        *) But could technically be represented by only one single file.
        **) But opposite of what a Horizon is in e.g. OpenWorks.

        ## Metadata on the Surface
        Metadata on the surface object is valid for all representations of it. Example: name

        ## Representations

            ## Realization
            A specific realization of the surface, represented by a dictionary. Realization contents
            are lazy-loaded into the internal datastore.

            ## Aggregation
            A specific aggregation of the surface, represented by a dictionary. Aggregation contents
            are lazy-loaded into the internal datastore.

            ## Metadata on the representation
            Metadata on a specific representation. Can be fetched from the file, or inserted. Example:
            filepath, rotation, etc

            Representation dictionaries can have any keys, technically. Between different functions
            in the class, the whole dictionary will generally be passed as one object.

            Realizations are separated from aggregations in the syntax to make it real clear what is realizations
            (raw data) and what is results (aggregations). Other than that, they are very similar.

        ## Internal data store
        The internal datastore is a dictionary that the class instance keeps in memory as long as it's
        alive. It is a dictionary on the following form:

            internal_storage = {
                'name' : 'SomeName',
                'source' : 'FMU',
                'coordinate_system' : {
                    'zone' : UTM zone,
                    'etc' : etc,
                    },
                'whatever' : {
                    whatever : whatever
                    }

                'data' : {
                    'realizations' : 
                            {

                            0 : {
                                'filepath' = 'path/to/file'
                                'regular_surface' : xtgeo.surface.regular_surface.RegularSurface(),
                                'representation' : 'realization',
                                }
                            1 : {
                                'filepath' = 'path/to/file'
                                'regular_surface' : xtgeo.surface.regular_surface.RegularSurface(),
                                'representation' : 'realization',
                                }

                            ...

                            }

                    'aggregations' : {
            
                            'mean' : {
                                'filepath' = 'path/to/file'
                                'regular_surface' : xtgeo.surface.regular_surface.RegularSurface(),
                                'representation' : 'aggregation',
                                }
                            'max' : {
                                'filepath' = 'path/to/file'
                                'regular_surface' : xtgeo.surface.regular_surface.RegularSurface(),
                                'representation' : 'aggregation',
                                }
                            }
                        }
                    }


    The internal datastore holds everything related to a specific surface, including data.
    On initiation, the datastore is filled with whatever was included in the call. Then expands on that as requests
    are received.

    All metadata except those derived from the files themselves or generated during calculations will be broadcasted
    to Surface with the call. The reason for this is that we want Surface() to be storage agnostic. While an ensemble
    on disk has yaml-files attached to the various .gri files, an ensemble on Azure has not. This means that the 
    metadata must be collected and broadcasted along with the paths on the outside of Surface. 
    
    This is partly also to avoid making Surface too big and cumbersome. It should, relatively narrowly, only deal 
    with the math related to surfaces.

    Surface can be initialized without aggregations and without realizations. Initializing without realizations will
    obviously limit the possibilities (no statistics can be calculated, etc), but is still meaningful in cases where
    only statistical surfaces exists. Similary, initializing with just realizations, no aggregations, is meaningful
    where no statistics have been calculated. When asked for an aggregation, Surface will first look for existing
    data. If not present, it will calculate it.

    Ideally, the internal storage should retain all information and all fields given to it in the call, whatever it
    is. We might not want to explicitly program everything, and this will allow users to use metadata that is not
    currently present without requiring an update of fmu.ensemble.

    A "to_disk" function could be added to dump the surface to disk, and this functionality can be
    part of Surface. But the actual call needs to be made from outside. This means that if Surface
    calculates an aggregation, and the caller does not dump it to disk, the aggregation is gone
    once the Surface instance disappear.

    For now, the class will assume that all files belonging to the same surface have the exact same 
    geometrical layout (xinc, yinc, origin, ncols, nrows) and check that this is true #TODO

    The class should return answers when queried for specific metadata values, such as xinc, yinc.

    zvalues is specified, to make room for possible future addons of x and y values. Want to avoid
    too generic names (such as "data" or "values" as its meaning can be ambigous)

    Surface should support operations between two separate surfaces.

        def __add__(self, other):
            return self.value + other, whatever

        This should return a new Surface instance. Example:
        MySurface = Surface(dict)
        MyOtherSurface = Surface(dict)

        DiffSurf = MySurface - MyOtherSurface

    When two surfaces with different amounts of realizations are added/subtracted, the resulting
    surface object should only contain those realizations that are present in both.

    For discussion: The difference between difference in mean, and mean of differences:

        If surface1 is represented by 1000 realizations, and surface2 by 50 realizations, and we wan the
        difference in mean, we would simply calculate mean for both, then subtract. But if we want the mean 
        of differences, each realization should be diffed, then the mean should be calculated on the 50
        realizations that represents the overlap between the ensembles. This will possibly be very wrong... 
        We are basically ignoring 950 realizations from the first ensemble... I think the best way to do this
        is to do it on aggregations, or on specified realisations. So this means that aggregation must be given.
        This further means that Surface1 + Surface2 is not possible to easily implement! 

        (How has this been done in fmu.ensemble with other datatypes??)


    Returning of surface aggregatations and/or realizations
    When a specific aggregation (or realization) is requested, Surface will first check the internal datastore.
    If the requested aggregation is not there, it will calculate it, store it in the internal datastore, and
    return it. The format of the returned data will be a dict-like structure that follows the specs of the internal
    data store, but with only the requested aggregation or realization included. 

    Suggestion:

    returned_array = {
        'name' : 'SomeName',
        'source' : 'FMU',
        'coordinate_system' : {
            'zone' : UTM zone,
            'etc' : etc,
            },
        'rep' : {                  # in general we should NOT put client-specific things into the datamodel
            whatever : whatever    # but this may have to be present because of legacy
            }                      #
        data : {

            'aggregations' : {
                'mean' : {
                    'regular_surface' : xtgeo.surface.regular_surface.RegularSurface(),
                    'representation' : 'aggregation',
                }
            }
        }

    In the example above, the requested aggregation is the mean, and that's the only thing returned under the 'data' key. 
    This structure makes it possible to expand a request to multiple aggregations in the same return, if wanted. 
    Again, it puts some responsibility on the caller side, as the elements present in the data  will be different 
    depending on what has been requested.

    Perhaps a "just give me the zvalues, nothing else" can be implemented, but I really think this should be handled on
    the caller/receiver end.

    """

    def __init__(self, metadata, 
                       realizations=None, 
                       aggregations=None, 
                       file_format='irap_binary',
                       largest_zvalue = 1000000.,
                       fill_nan_value = None,
                       ):


        """Initialize the Surface object
    
        Args:
            metadata (dict) : metadata to follow the surface object. Must at least contain 'name'
            realizations          (dict) : Filepaths per realization as dictionary with realization
                                           number (int) as key, and full filepath (str) as value.
                                           Optional. Default: None
            aggregations (dict)          : Filepaths per aggregation as dictionary with realization
                                           number (int) as key, and full filepath (str) as value.
                                           Optional. Default: None
            file_format (str)            : File format that will be passed to xtgeo for parsing files.
                                           Default: 'irap_binary'. See xtgeo documentation for options.
            largest_zvalue (float)       : Largest zvalue to parse in the realizations. Larger than this
                                           will be set to undefined if given. Default: 1000000.
            fill_nan_value (float)       : Value to fill undefined values in realizations. If None,
                                           undefined values will remain undefined. For some surface
                                           types (e.g. volume thickness), undefined means zero when
                                           they come out of RMS. In these cases, 0.0 should be passed to
                                           avoid wrong statistical results.

        """

        self.file_format = file_format  # setting it globally now, but could also be given per filepath (but not derived from the filename!)
        self.metadata = metadata
        self.largest_zvalue = largest_zvalue

        if fill_nan_value is None:
            self.fill_nan_value = np.nan
        else:
            self.fill_nan_value = fill_nan_value

        # start building the internal data store by copying the provided metadata
        internal_datastore = metadata.copy()

        # initialize the data key of the internal data store
        internal_datastore['data'] = data = {}

        # if realization filepaths was provided, put those into the internal data store
        if realizations is None:
            realizations = {}
            logger.warning('Surface initialized without any realizations.')
        else:
            if not isinstance(realizations, dict):
                raise ValueError('realizations must be a dictionary. Type {} was given.'.format(type(realizations)))
        
        data['realizations'] = realizations
        for key in realizations:
            if not isinstance(key, int):
                raise ValueError('Realizations must be indexed by integer numbers as keys. Found this that I did not like: {} (type is {})'.format(key, type(key)))
            realizations.update({key : {'filepath' : realizations.get(key)}})

        # if aggregation filepaths were given, put those into the internal data store
        if aggregations is None:
            aggregations = {}
        else:
            if not isinstance(aggregations, dict):
                raise ValueError('aggregations must be a dictionary. Type {} was given.'.format(type(aggregations)))
        
        data['aggregations'] = aggregations
        for key in aggregations:
            if not isinstance(key, str):
                raise ValueError('Aggregations must be indexed by a stringed key. Found this that I did not like: {} (type is {})'.format(key, type(key)))
            aggregations.update({key : {'filepath' : aggregations.get(key)}})


        self.data = self.validate_data(internal_datastore)

        self._name = self.data.get('name', None)
        self._source = self.data.get('source', None)
        self._coordinate_system = self.data.get('coordinate_system', None)
        self._description = self.make_description()

        self._dummy_zvalue = 0.0


    def validate_data(self, data):
        """Perform validation checks on the input data with possibility to make adjustments"""

        # a clear advantage here would be to actually parse the files. That would show
        # us if the files are actually valid, readable, and even existing. But might cost a lot
        # in terms of performance and data transfer from blob store.

        return data


    @property
    def dummy_zvalue(self):
        """Dummy value used during development. Will be removed."""
        self._dummy_zvalue += 1.0
        return self._dummy_zvalue
    


    @property
    def name(self):
        """Returns the surface name"""
        return self._name

    @property
    def source(self):
        """Returns the surface source"""
        return self._source
        
    @property
    def realizations(self):
        """Returns a list of realizations held by the Surface object"""
        return [r for r in self.data.get('data', {}).get('realizations', [])]

    @property
    def aggregations(self):
        """Returns a list of aggregations held by the Surface object"""

        return [a for a in self.data.get('data', {}).get('aggregations', [])]
    
    @property
    def coordinate_system(self):
        """Returns the coordinate system"""
        return self._coordinate_system
    
    def __repr__(self):
        """Represent the surface. Show number of realisations included and key metadata."""
        return "<Surface, {}>".format(self._description)

    def make_description(self):
        """Returns the class description"""
        name = self.data.get('name', None)

        if name is None:
            raise ValueError('metadata must contain "name"')

        realizations = self.data.get('data', {}).get('realizations', [])

        referenced_realizations_count = len(realizations)
        loaded_realizations_count = 0
        for r in realizations:
            if realizations.get(r, {}).get('zvalues', None) is not None:
                loaded_realizations_count += 1

        aggregations = self.data.get('data', {}).get('aggregations', [])
        referenced_aggregations_count = len(aggregations)

        loaded_aggregations_count = 0
        for a in aggregations:
            if aggregations.get(a, {}).get('zvalues', None) is not None:
                loaded_aggregations_count += 1

# Python 3
#        descr = f"""{name}
#    Realizations: {referenced_realizations_count} referenced, {loaded_realizations_count} loaded. 
#    Aggregations: {referenced_aggregations_count} referenced, {loaded_aggregations_count} loaded.
#    """
        descr = """
        Realizations: {} referenced, {} loaded.
        Aggregations: {} referenced, {} loaded.

        """.format(referenced_realizations_count, loaded_realizations_count, referenced_aggregations_count, loaded_aggregations_count)


        return descr

    def mean(self, force_calculate=False):
        """Short-hand for getting the mean aggregation

        Args:
            force_calculate (bool): If True, mean will be calculated irrespective
                                    of cached data.

        """
        return self.get_aggregation('mean', force_calculate)

    #    def percentile(self, percentile: int):   # python3
    def percentile(self, percentile):
        """Return a specific percentile. Follow fmu.ensemble conventions and use normal percentiles,
        not reverse oil-industry percentiles. I.e. p90 is the upper, p10 is the lower.

        Args:
            percentile (int 0 and 100): The requested percentile on normal conventions, not
                                                reverse oil-industry conventions. Will be passed
                                                directly to numpy.percentile().

        Returns:
            percentile (dict)
        """

        if percentile < 0 or percentile > 100:
            raise ValueError('Percentile must be a positive number between 0 and 100')

        return self.get_aggregation('p{}'.format(percentile))





    def merge_dicts(self, source, destination):
        """Utility-function for merging dictionaries. Given two dicts, 
        merge them recursively. If overlapping information, source has priority.

        https://stackoverflow.com/questions/20656135/python-deep-merge-dictionary-data

        Args:
            source (dict): Source dictionary
            destination (dict) : Destination dictionary

        Returns:
            destination (dict)
        """

        if not isinstance(source, dict):
            #py3   raise ValueError(f'A {type(source)} ({source}) was passed to merge_dicts(). Takes dicts only.')
            raise ValueError('A {} ({}) was passed to merge_dicts(). Takes dicts only.'.format(
                            type(source),
                            source))
        for key, value in source.items():
            if isinstance(value, dict):
                # get node or create one
                node = destination.setdefault(key, {})
                self.merge_dicts(value, node)
            else:
                destination[key] = value

        return destination



    def get_aggregation(self, agg, force_calculate=False):
        """Get requested aggregation. If not present in internal
        data store, get it from file or make it. Return dictionary according
        to output spec in class docstring.

        There are four possible outcomes, with different actions:

        1) The force_calculate variable is True
           --> Calculate it, and append to/overwrite the internal data store.
               Give warning if it already exists.
        2) The internal datastore has no records of the requested aggregation
           --> Calculate it, and add it to the internal data storage
        3) The internal datastore has some records, but not the zvalues
           --> Parse it from file
        4) The internal datastore has the zvalues, and hence everything else (assumed)
           --> Return as-is in the internal data store

        Args:
            agg (str): Name of valid aggregation
            force_calculate (bool): If True, calculation will be done irrespective of
                                    cached versions

        Returns:
            aggregation (dict): Full representation as dict, including zvalues.

        """

        stored_aggregation = self.data.get('data', {}).get('aggregations', {}).get(agg, None)

        if force_calculate:
            #logger.info(f'Requested agg is {agg} and force_calculate is True, so calculating.')
            logger.info('Requested agg is {} and force_calculate is True, so calculating.'.format(agg))
            aggregation = self.make_aggregation(agg)  # must return the whole dict

            if stored_aggregation is not None:
                logger.warning('Aggregation {} was requested, with force_calculate = True'.format(agg))
                logger.warning('The aggregation already exists and will be overwritten.')

            # update self.data
            # self.data = self.merge_dicts(aggregation, self.data)
            self.data['data']['aggregations'].update({agg : aggregation})

            logger.info('"{}" is added to the internal datastore. '.format(agg))

            return aggregation


        if stored_aggregation is None:
            # need to calculate
            logger.info('Requested agg is {} which is not found in internal datastore, so calculating.'.format(agg))
            aggregation = self.make_aggregation(agg)  # must return the whole dict

            # update self.data
            self.data['data']['aggregations'].update({agg : aggregation})
            #self.data = self.merge_dicts(aggregation, self.data)

            logger.info('"{}" is added to the internal datastore. '.format(agg))

            return aggregation

        stored_surf_obj = stored_aggregation.get('regular_surface', None)
        stored_filepath = stored_aggregation.get('filepath', None)

        if stored_surf_obj is None and stored_filepath is not None:
            # the aggregation is there, but only has a reference to the file, no zvalues
            file_format = stored_aggregation.get('file_format', None)
            aggregation = self.parse_file(stored_filepath, file_format)

            # update self.data
            self.data['data']['aggregations'].update({agg : aggregation})
            #self.data = self.merge_dicts(aggregation, self.data)

            logger.info('"{}" is added to the internal datastore. '.format(agg))

            return aggregation


        if stored_surf_obj is not None:
            # stored aggregation has values, so we can return it as is

            return stored_aggregation

        raise ValueError('''Unexpected error: get_aggregation() made it all the way to the end 
                         without triggering any of the conditions. 
                         Requested aggregation was {} and force_calculate was {}.'''.format(agg, force_calculate))

    def get_aggregations(self, aggs, force_calculate):
        """Calculate or get multiple aggregations, return as dict with metadata
        according to specs

        Args:
            aggs ([str]) : List of aggregations to return
            force_calculate (bool) : Force calculation even if already cached

        Return:
            Surface (dict): Dictionary of metadata and values for requested aggregations

        """

        raise NotImplementedError('Return of multiple aggregations is not implemented')


#    def get_realization(self, r: int):         # python3
    def get_realization(self, r):
        """Get requested realization. If not present in internal
        data store, get it from file if possible, otherwise return None. 
        Return dictionary according to output spec in class docstring.

        There are four possible outcomes, with different actions:

        1) The internal datastore has no records of the requested realization
           --> Return None
        2) The internal datastore has some records, but not the zvalues
           --> Parse it from file
           --> Update internal datastore
           --> Return it
        3) The internal datastore has the zvalues, and hence everything else (assumed)
           --> Return as-is in the internal data store

        Args:
            r (int): Realization index (as from FMU)
        Returns:
            Aggregation (dict): full representation, including z-values

        """

        stored_realization = self.data.get('data').get('realizations', {}).get(r, None)

        if stored_realization is None:
            # It's not there at all
            logger.warning('''Realization {} was requested, but does not exist.
                           Available realizations are {}'''.format(r, self.realizations))
            return None

        stored_zvalues = stored_realization.get('zvalues', None)
        stored_filepath = stored_realization.get('filepath', None)

        if stored_zvalues is None and stored_filepath is not None:
            # the aggregation is there, but only has a reference to the file, no zvalues
            file_format = stored_realization.get('file_format', None)
            realization = self.parse_file(stored_filepath, file_format)
            realization['representation'] = 'realization'
            realization['type'] = 'realization'
            realization['realization_id'] = r

            #update self.data
            self.data['data']['realizations'][r].update(realization)
            logger.info('Realization {} values has been added to the internal datastore. '.format(r))

            return realization

        if 'zvalues' in stored_realization:
            # stored aggregation has values, so we can return it as is

            return stored_realization

        raise ValueError('''Unexpected error: get_realization went all the way to
                         the end without triggering any conditions.
                         The requested realization was {}'''.format(r))


    def get_realizations(self, realizations):
        """Get multiple realizations, return as dict with metadata
        according to specs

        Args:
            realizations ([str]) : List of realizations to return

        Return:
            Surface (dict): Dictionary of metadata and values for requested realizations

        """

        raise NotImplementedError('Return of multiple realizations is not implemented')


    def get_surface_specs(self, s):
        """Given a surface object, return it's specifications, corresponding to what
        is necessary to initiate a new Surface in XTgeo."""

        return {'ncol' : s.ncol, 
                'nrow' : s.nrow, 
                'xori' : s.xori,
                'yori' : s.yori,
                'rotation' : s.rotation,
                'xinc' : s.xinc,
                'yinc' : s.yinc,
                'n_nodes' : len(s.values1d),
                }


    def get_all_realization_zvalues(self):
        """Function for loading all realization zvalues into memory as a numpy matrix.
        Used by functions that will make aggregations.

        Might be/should be/could be replaced by get_realizations()

        Args:
            None
        Returns:
            2D array (np.ndarray)

        """

        realizations = self.data.get('data', {}).get('realizations', None)
        n_nodes = None

        if realizations is None:
            raise ValueError('No realisations')

        # first make sure all realizations are in the internal datastore and that they are all identical
        for i, r in enumerate(realizations):   # n is incremental, r is realization number

            if i > 0:
                surface_specs_previous = surface_specs_current.copy()

            self.get_realization(r)

            surfobj = self.data.get('data').get('realizations').get(r).get('regular_surface')
            surface_specs_current = self.get_surface_specs(surfobj)
            #surface_specs_all.append(surface_specs)

            if i > 0:
                n_nodes = surface_specs_current.get('n_nodes')
                n_nodes_previous = surface_specs_previous.get('n_nodes')
                if not n_nodes == n_nodes_previous:
                    raise ValueError('When checking realization {r}, inconsistency in n_nodes was found with the previous.')

        # passed without crashing, assuming all surface specs are the same
        surface_specs = surface_specs_current

#            # check that specs is the same as other realizations
#            if n_nodes is None:  # will trigger only first iteration
#                n_nodes = len(surfobj.values1d)
#            
#            if not n_nodes == len(surfobj.values1d):
#                raise ValueError('''Realization {} has {len(zvalues)} nodes. Expected {}.'''.format(r, len(surfobj.values1d), n_nodes))


        # zvalues for this realization now exists in the datastore
        # now add zvalues to the 2darray
        # this has (...) do be done in a separate loop, as information from the first
        # realization is used to initialize the numpy array

        #initialize big array to hold all realization zvalues
        zvalues_matrix = np.zeros((len(realizations), n_nodes))

        for i, r in enumerate(realizations):
            realization = self.data.get('data').get('realizations').get(r)
            zvalues_matrix[i, :] = realization.get('regular_surface').values1d   # not hedging this because it should be there...
            
            # replace funny-nans with np.nan
            undef = realization.get('undef')
            zvalues_matrix[zvalues_matrix == undef] = np.nan

        # remove hilarous values, the to/from np.inf trick is inherited
        # from bkh's scripts to avoid RuntimeWarning
        zvalues_matrix[np.isnan(zvalues_matrix)] = -np.inf
        zvalues_matrix[zvalues_matrix > self.largest_zvalue] = np.nan
        zvalues_matrix[zvalues_matrix == -np.inf] = np.nan

        return zvalues_matrix, [r for r in realizations], surface_specs



    def make_aggregation(self, a):
        """Create the aggregation from realizations, by determining which type of aggregation
        is requested and calling the appropriate maker function. Do not check internal datastore.
        Return full representation according to specs.

        Args:

          a, str : Valid aggregation. Example: 'mean'

        """


        # is it a percentile?
        p = self.is_valid_pvalue(a)

        if p is not None:

            return self.make_aggregation_percentile(p)

        # no... is it something we can use numpy native functions to calculate?
        if a in ['mean', 'min', 'max', 'std']:

            return self.make_aggregation_numpy_native(a)   # must return full dict according to spec

        # no... is it something we know how to handle?
        if a in ['numreal']:

            return self.make_aggregation_non_numpy_native(a)

        # no... Well, then now is a good time to crash.
        raise ValueError('Unknown aggregation requested: {} '.format(a))


    def make_aggregation_percentile(self, p):
        """Get all the realizations in the internal datastore, calculate
        the requested percentile, add it to the datastore and return it
        as a dict according to specs. Do point-wise aggregation: Percentile
        is calculated per node across all realizations.

        Args:
            p (int): Percentile between 0 and 100

        Returns:
            Aggregation (dict)
        """

        # check that p is valud
        if not isinstance(p, int) or not 0 <= p <= 100:
            raise ValueError('''A wrongly formatted percentile was passed to make_aggregation_percentile(). 
                'It looked like this: {} and the type was {}  '''.format(p, type(p)))

        # get the realization data into a 2D array
        realizations_zvalues_matrix, list_of_realizations, surface_specs = self.get_all_realization_zvalues()

        # calculate the new aggregated array
        zvalues = np.percentile(realizations_zvalues_matrix, p, axis=0)


        # The math here must be validated, and also the handling of undefined values, zeros, etc etc
        # See code from BKH where there is a lot of things going on.

        # compile and return the results

        # create regular surface
        rs = self.create_regular_surface(surface_specs, zvalues)

        percentile = {
               'representation' : 'p{}'.format(p),    # adding it here, which is a bit redudant, but useful
               'type' : 'aggregation',
               'regular_surface' : rs,
               'included_realizations' : list_of_realizations,
               'fill_nan_value' : self.fill_nan_value,
               }


        return percentile

        #return {'data' : {'aggregations' : {p : percentile_block}}}

    def make_aggregation_numpy_native(self, a):
        """Translate the requested aggregation a to a numpy function and return full
        dictionary according to specs. This function assumes that checks have been made
        to see if the aggregation is present in some form in the internal datastore.
        So no need to check for that. Safely overwrite........

        Args:
            a (str): Valid aggregation as a string. Example: 'mean'

        Return:
            aggregation (dict): Aggregation in dict according to specs

        """

        # Unclear right now: Should the function just calculate everything, even if a file
        # reference is included in the datastore? If so, we risk that the calculations for some
        # reason is different than the one on disk. Could parse the one on disk, but then
        # it will slow down and the point is gone. And if we don't do that, we risk that
        # all realizations are parsed over and over again.

        # if the realizations are stored in the datastore, it should be sufficiently fast
        # to get it, so for now, jus calculate the one that has been requested.


        # Get all realizations from datastore, load into one big numpy
        # Possible target for multi-threading
        # The function loading all the realizations must also collect metadata for them
        # end up with something like this:

        #realizations = np.array([np.random.rand(100), np.random.rand(100), np.random.rand(100)])
        #metadata = {'xinc' : 123, 'yinc' : 123, 'origin' : (12345, 123456)}  # this will be separated out

        realizations_zvalues_matrix, list_of_realizations, surface_specs = self.get_all_realization_zvalues()

        # possibly possible to use getattr(numpy, a) here instead of explicitly programming all

        zvalues = None

        if a == 'min':
            zvalues = np.min(realizations_zvalues_matrix, axis=0)

        if a == 'max':
            zvalues = np.max(realizations_zvalues_matrix, axis=0)

        if a == 'mean':
            zvalues = np.mean(realizations_zvalues_matrix, axis=0)

        if a == 'std':
            zvalues = np.max(realizations_zvalues_matrix, axis=0)

        if zvalues is None:
            raise ValueError('''Function did not trigger on any of the defined numpy native functions.
                             This was passed as a: {}.'''.format(a))

        # create regular surface
        rs = self.create_regular_surface(surface_specs, zvalues)


        aggregation = {
               'representation' : a,    # adding it here, which is a bit redudant, but useful
               'type' : 'aggregation',
               'regular_surface' : rs,
               'included_realizations' : list_of_realizations,
               'fill_nan_value' : self.fill_nan_value,
               }


        return aggregation

    def create_regular_surface(self, surface_specs, zvalues):
        """Given specs and zvalues, create and return an instance
        of xtgeo.RegularSurface"""

        rs = xtgeo.RegularSurface(ncol=surface_specs.get('ncol'), 
                                  nrow=surface_specs.get('nrow'), 
                                  xori=surface_specs.get('xori'), 
                                  yori=surface_specs.get('yori'),
                                  rotation=surface_specs.get('rotation'), 
                                  xinc=surface_specs.get('xinx'), 
                                  yinc=surface_specs.get('yinc'),
                                  values=np.zeros((surface_specs.get('ncol'),surface_specs.get('nrow'))))

        rs.set_values1d(zvalues)

        return rs


    def make_aggregation_non_numpy_native(self, a):
        """Function for making aggregations that cannot be linked to a native numpy function"""

        # Get all realizations from datastore, load into one big numpy
        # Possible target for multi-threading
        # The function loading all the realizations must also collect metadata for them
        # end up with something like this:

        realizations_zvalues_matrix, list_of_realizations, surface_specs = self.get_all_realization_zvalues()

        zvalues = None

        if a == 'numreal':
            zvalues = np.random.rand(100)  # Dummy, dont know how to calculate this

        if zvalues is None:
            raise ValueError('''Function did not trigger on any of the defined non-numpy native functions.
                             This was passed as a: {}.'''.format(a))

        # create regular surface
        rs = self.create_regular_surface(surface_specs, zvalues)

        aggregation = {
               'representation' : a,    # adding it here, which is a bit redudant, but useful
               'type' : 'aggregation',
               'regular_surface' : rs,
               'included_realizations' : list_of_realizations,
               'fill_nan_value' : self.fill_nan_value,
               }


        return aggregation



    def is_valid_pvalue(self, pstring):
        """Return a float percentile between 0 and 1 if pstring is valid p-value
        (P##), None if not"""

        # could use re for this, but apparently typing is fun...

        pstring = pstring.lower()

        if not pstring.startswith('p'):
            return None

        if len(pstring) > 3 and pstring != 'p100':   # allow for P2 and P20 and P99 but not 3 and P101
            return None

        if len(pstring) < 2:
            return None

        try:
            p = int(pstring[1:])
        except:
            return None

        return p


    def parse_file(self, filepath, file_format):
        """Given a file path, check that it is valid, and parse the file
        using xtgeo. The file format is directly passed to xtgeo. No checks
        here. Also derive metadata from the parsed file.
        Return as dictionary according to specs.

        Args:
            filepath (str): Full path to file, will be passed to xtgeo.
            file_format (str): Format of file, will be passed to xtgeo.

        Returns:
            Data and values (dict)
            """


        rs = xtgeo.surface_from_file(filepath, fformat=file_format)


        return {
               'filepath' : filepath,
               'regular_surface' : rs,
               'fill_nan_value' : self.fill_nan_value,
               }

