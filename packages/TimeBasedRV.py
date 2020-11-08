import pandas as pd
import datetime
from datetime import datetime as dt
from dateutil.relativedelta import *


class TimeBasedRV(object):
    '''
    Parameters
    ----------
    test_period: int
        number of time units to include in each test set
        default is 7
    freq: string
        frequency of input parameters. possible values are: days, months, years, weeks, hours, minutes, seconds
        possible values designed to be used by dateutil.relativedelta class
        deafault is days
    train_end_dt: string
        the last day of training set from where the rolling window starts
    '''
    def __init__(self, test_period=7, freq='days', train_end_dt=None):
        self.train_end_dt = train_end_dt
        self.test_period = test_period
        self.freq = freq
        self.n_splits = 0

    def split(self, data, gap):
        '''
        Generate indices to split data into training and test set

        Parameters
        ----------
        data: pandas DataFrame
            your data, contain one column for the record date
        gap: int, default=0
            for cases the test set does not come right after the train set,
            *gap* days are left between train and test sets

        Returns
        -------
        train_index ,test_index:
            list of tuples (train index, test index) similar to sklearn model selection
        '''

        # check that date_column exist in the data:
        try:
            isinstance(data.index, pd.core.indexes.datetimes.DatetimeIndex)
        except:
            raise KeyError('datetime index missing')

        train_indices_list = []
        test_indices_list = []

        start_train = data.index.min().date()
        end_train = dt.strptime(self.train_end_dt, '%Y-%m-%d %H:%M:%S').date()
        start_test = end_train + eval('relativedelta(' + self.freq + '=gap)')
        end_test = start_test + eval('relativedelta(' + self.freq + '=self.test_period)')

        while end_test < data.index.max().date():

            start_train = start_train + eval('relativedelta(' + self.freq + '=1)')
            end_train = end_train + eval('relativedelta(' + self.freq + '=1)')
            start_test = end_train + eval('relativedelta(' + self.freq + '=gap)')
            end_test = start_test + eval('relativedelta(' + self.freq + '=self.test_period)')

            # train indices:
            cur_train_indices = list(data[(data.index.date >= start_train) &
                                               (data.index.date < end_train)].index)
            # test indices:
            cur_test_indices = list(data[(data.index.date >= start_test) &
                                              (data.index.date < end_test)].index)

            if len(cur_test_indices) < self.test_period:
                end_test = start_test + eval('relativedelta(' + self.freq + '=self.test_period+1)')
                cur_test_indices = list(data[(data.index.date >= start_test) &
                                                  (data.index.date < end_test)].index)

            if len(cur_test_indices) < self.test_period:
                end_test = start_test + eval('relativedelta(' + self.freq + '=self.test_period+2)')
                cur_test_indices = list(data[(data.index.date >= start_test) &
                                                  (data.index.date < end_test)].index)
                
                
            # print("Train period:", start_train, "-", end_train, ", Test period", start_test, "-", end_test,
                  # "# train records", len(cur_train_indices), ", # test records", len(cur_test_indices))

            train_indices_list.append(cur_train_indices)
            test_indices_list.append(cur_test_indices)


        # mimic sklearn output
        index_output = [(train, test) for train, test in zip(train_indices_list, test_indices_list)]

        self.n_splits = len(index_output)

        return index_output

    def get_n_splits(self):
        """Returns the number of splitting iterations in the cross-validator
        Returns
        -------
        n_splits : int
            Returns the number of splitting iterations in the cross-validator.
        """
        return self.n_splits

