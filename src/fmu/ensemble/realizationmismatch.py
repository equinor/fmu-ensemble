import pandas as pd
from collections import defaultdict


def read_sim_smry_obs(real, smry):
    """ This function reads the simulated observation for a given key and
    date"""
    if not real._eclsum:  # check if it is cached
        real.get_eclsum()
    return real._eclsum.get_interp(smry['key'], date=smry['date'])


def read_sim_rft_obs(real, rft):
    """ Read RFT observations for a given realization.l
    """
    pass


def mismatch(real, data):
    """" Calculates the mismatch beteeen the simulated
    observation and the measured value for a given
    realization.

    Extracts the simulated observation for summary vectors.

    Args:
        real : realization object
        data: observation data

    TODO (fdil) : extract different types of simulated observations
                  e.g. RFT observations.

    """

    mismatch_data = defaultdict(list)
    for obs_type, obs_data in data.iteritems():

        if obs_type == 'summary_vectors':
            for index, smry in obs_data.iterrows():
                sim_value = read_sim_smry_obs(real, smry)
                mismatch_data['id'].append(smry['id'])
                mismatch_data['sim_value'].append(sim_value)
                mismatch_data['REAL'].append(real.index)
                mismatch_data['Mismatch'].append(smry['value'] - sim_value)
                mismatch_data['Mismatch_norm'].append((smry['value'] -
                                                      sim_value) /
                                                      smry['error'])
                mismatch_data['state'].append('Negative' if (smry['value'] -
                                              sim_value) < 0 else 'Positive')
            return pd.DataFrame(mismatch_data)
        if obs_type == 'rft_pressure':
            pass
        #     for rft in obs_frame:
        #         rft_values.append(read_sim_rft_obs(real, rft))
