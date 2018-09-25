import pandas as pd
from collections import defaultdict



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
                sim_value = real.get_eclsum().get_interp(smry['key'],
                                                         date=smry['date'])
                mismatch_data['ID'].append(smry['id'])
                mismatch_data['SIM_VALUE'].append(sim_value)
                mismatch_data['REAL'].append(real.index)
                mismatch_data['MISMATCH'].append(smry['value'] - sim_value)
                mismatch_data['MISMATCH_NORM'].append((smry['value'] -
                                                      sim_value) /
                                                      smry['error'])
                mismatch_data['STATE'].append('Negative' if (smry['value'] -
                                              sim_value) < 0 else 'Positive')
            return pd.DataFrame(mismatch_data)
        if obs_type == 'rft_pressure':
            pass
        #     for rft in obs_frame:
        #         rft_values.append(read_sim_rft_obs(real, rft))
