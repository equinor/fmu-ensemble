from collections import defaultdict
import pandas as pd
import yaml


def observations_parser(path):
    """ Function to parse the observation yaml file

    Args:
    path: string. file path the observation yaml file

    Returns:
        dictionary containing a dataframe for each observation type
    """

    def read_obs_yaml(path):
        """ Function returns the contents of the observation yaml """
        with open(path, 'r') as stream:
                observations = yaml.load(stream)
        return observations

    def summary_observations(obs_data):
        """ Function returns summary observations in a dataframe  """
        data = defaultdict(list)
        for obs_group in obs_data:
            for obs in obs_group['observations']:
                data['key'].append(obs_group['key'])
                data['date'].append(obs['date'])
                data['value'].append(obs['value'])
                data['error'].append(obs['error'])
                data['comments_key'].append(obs_group['comment'] if 'comment' in obs_group.keys() else None)
                data['comments_value'].append(obs['comment'] if 'comment' in obs.keys() else None)
        columns = ['key', 'date', 'value', 'error', 'comments_value', 'comments_key']
        dframe = pd.DataFrame(data)
        dframe = dframe[columns]
        return dframe

    def rft_observations(gen_data):
        """ Function returns RFT observations in a dataframe  """
        data = defaultdict(list)
        for obs in gen_data['observations']:
            data['category'].append(gen_data['category'])
            data['well'].append(gen_data['well'])
            data['date'].append(gen_data['date'])
            data['value'].append(obs['value'])
            data['error'].append(obs['error'])
            data['zone'].append(obs['zone'])
            data['MDmsl'].append(obs['MDmsl'])
            data['x'].append(obs['x'])
            data['y'].append(obs['y'])
            data['z'].append(obs['z'])
            data['comments_key'].append(gen_data['comment'] if 'comment' in gen_data.keys() else None)
            data['comments_value'].append(obs['comment'] if 'comment' in obs.keys() else None)
        columns = ['category', 'well', 'date', 'value', 'error', 'x', 'y', 'z', 'MDmsl', 'zone', 'comments_value', 'comments_key']
        dframe = pd.DataFrame(data)
        dframe = dframe[columns]
        return dframe

    # main
    observations_yaml = read_obs_yaml(path)
    data = {}
    for obs_type, obs_data in observations_yaml.iteritems():
        if obs_type == 'summary_vectors':
            data['summary_vectors'] = summary_observations(obs_data)

        if obs_type == 'general_observations':
            for gen_data in obs_data:
                if gen_data['category'] == 'rft_pressure':
                    data['rft_observations'] = rft_observations(gen_data)
                if gen_data['category'] == 'gravity':
                    raise NotImplementedError
                if gen_data['category'] == 'some_random_other_obserbations':
                    raise NotImplementedError

    return data


