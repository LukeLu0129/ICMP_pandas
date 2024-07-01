import pandas as pd
import h5py
from ._utility import _Trend_h5dir, Ser2DT, DT2Ser, Path
from .Episode import Episode_Dataframe

def getTrendEpisodeDF(IsInEpisodeSeries:pd.Series, MAX_TIME_GAP='5min',MIN_EPISODE_DURATION='5min') -> pd.DataFrame:  
    '''
    Extract continuous episodes within a time list (usually datetimeindex from time series)
    MAX_TIME_GAP: Closely adjacent periods with time gap < MAX_TIME_GAP will be treated as a single period. Tolerance for short resolution.
    MIN_EPISODE_DURATION: Filter out episode with duration < MIN_EPISODE_DURATION
    '''
    if isinstance(MAX_TIME_GAP, str):
        '''attempt converting to Timedelta if arg is str'''
        MAX_TIME_GAP = pd.to_timedelta(MAX_TIME_GAP)

    if isinstance(MIN_EPISODE_DURATION, str):
        '''attempt converting to Timedelta if arg is str'''
        MIN_EPISODE_DURATION = pd.to_timedelta(MIN_EPISODE_DURATION)

    if IsInEpisodeSeries.dtype != bool:
        IsInEpisodeSeries = IsInEpisodeSeries.astype(bool)

    EpisodeDTList = IsInEpisodeSeries[IsInEpisodeSeries].index
    EpisodeDatetimeDict = {'StartDatetime':[],'DurationTimedelta':[],'EndDatetime':[]}       
    # reduce processing time by early return if EpisodeDTList is empty (avoid for loop)
    if len(EpisodeDTList) ==0:
        return Episode_Dataframe()
    EpiStartDT = LastDT = EpisodeDTList[0]
    for CurrentDT in EpisodeDTList[1::]:
        TIntervalDT = CurrentDT-LastDT
        # Ideally maximum time gap should be 1min (since minutely average trend), a higher maximum will join closely adjacent period 
        IsContinuous = TIntervalDT<MAX_TIME_GAP 
        #Start new episode if not continuous or if reached end of trend data
        if not IsContinuous or CurrentDT==EpisodeDTList[-1]:  
            EpiEndDT = LastDT    
            EpiDuration = EpiEndDT-EpiStartDT
            EpisodeDatetimeDict['StartDatetime'].append(EpiStartDT)
            EpisodeDatetimeDict['DurationTimedelta'].append(EpiDuration)
            EpisodeDatetimeDict['EndDatetime'].append(EpiEndDT)                                                
            # if abnormal value is not continuous then start a new potential episode 
            EpiStartDT = CurrentDT 
        # update LastDT for next iteration 
        LastDT = CurrentDT 
    EpisodeDatetimeDF = Episode_Dataframe(EpisodeDatetimeDict)
    # exclude period where duration is less then minimum duration of an episode
    if not EpisodeDatetimeDF.empty and isinstance(MIN_EPISODE_DURATION, pd.Timedelta):
        ValidPeriod = EpisodeDatetimeDF['DurationTimedelta'] > MIN_EPISODE_DURATION
        return EpisodeDatetimeDF[ValidPeriod]
    return EpisodeDatetimeDF


class Trend_Series(pd.Series):
    '''handle pandas internal operation that utilise pd.Series. This ensure pandas method will return Trend_DF instead pd.DataFrame'''
    
    def __init__(self,*args, **kwargs):
        super().__init__(*args, **kwargs)

    @property
    def _constructor(self):
        return Trend_Series
    @property
    def _constructor_expanddim(self):
        return Trend_DataFrame

    def get_episode(self, *args, **kwargs):
        return getTrendEpisodeDF(self, *args, **kwargs)
    
    def trapzoidal_auc(self, target_thres=None, IsUnderCurve:bool= False):
        '''
        calculate AUC with trapzoidal rule. if target_thres has numerical value, then calculate value exceeding the threshold before calculating burden.
        Note! this function accept datetimeidx and assess continuity by 1min interval (interval exceed 1 min will assumed discoutinuous)
        '''
        curve=self.copy()
        if target_thres!=None and isinstance(target_thres,(float,int)):
            if IsUnderCurve:
                curve = target_thres - curve
            else:
                curve = curve - target_thres
            curve = curve.where(curve>0)
        AUC_Series = curve*float('nan') 
        time1=curve.index[0]
        value1=curve[time1]
        dt_1hr=pd.to_timedelta('0day 01:00:00')
        for time2 in curve.index[1:]:
            value2 = curve[time2]
            dt = time2-time1   # change of time 
            dt_hr = dt/dt_1hr  # change of time (convert unit to hours)
            dv = value2+value1 
            auc = (dv*dt_hr)/2 # Area under curve mmHg*hrs using trapezoidal rule  
            # check if data is continuous/connection
            if dt<pd.to_timedelta('0day 00:01:02'):   
                AUC_Series[time1]=auc   # only include data that's 1 min apart to exclude auc value calculate across disconnection (>1 min)
            # update previous time and data value
            time1 = time2
            value1 = value2
        return AUC_Series.rename(f'{self.name}_burden')


class Trend_DataFrame(pd.DataFrame):
    '''Read ICMP trend data from varies file type (HDF5, CSV, XLSX). Return empty DataFrame if file path is invalid'''
    @property
    def _constructor(self):
        '''Overwrite internal method for compatibility'''
        return Trend_DataFrame

    @property
    def _constructor_sliced(self):
        '''Overwrite internal method for compatibility'''
        return Trend_Series

    def __init__(self, data=None, convert_dtidx = False, drop_index_col = False, dtidx_col_rename = {'datetime':'DateTime'}, **kwargs) -> None:
        '''
        Accept directory of ICM+ generated event file in the following formats (csv, txt, xml, hdf5)        
        '''
        if Path(data).is_file():
            TrendFile_dir = data
            TrendFileType = TrendFile_dir.split("\\")[-1].split(".")[-1]

            if TrendFileType == 'hdf5':       
                try:
                    _TrendDF = pd.DataFrame(h5py.File(TrendFile_dir, 'r').get(_Trend_h5dir)[()])  #Orignial trend data (default in minutes) from the hdf5 file
                    _TrendDF = _TrendDF.rename(columns = dtidx_col_rename)
                    convert_dtidx = True
                except:
                    raise Exception(f'Trend file not found in HDF5 file or {TrendFile_dir}')
            elif TrendFileType == 'csv': 
                _TrendDF = pd.read_csv(TrendFile_dir)       
            elif TrendFileType == 'xlsx': 
                _TrendDF = pd.read_excel(TrendFile_dir)
            else:
                raise Exception('Trend data file directory invalid')
            #remove sqaure brackets that incidate the unit
            _TrendDF = self.remove_col_bracket(_TrendDF)
            #Change DateTime data type from str to pd.DataTime
            dtidx_col = 'DateTime' #dtidx_col_rename.popitem()
            if convert_dtidx:
                _TrendDF[dtidx_col] = _TrendDF[dtidx_col].apply(Ser2DT)
            elif type(_TrendDF[dtidx_col][0]) == str:
                _TrendDF[dtidx_col] = [pd.to_datetime(strDT, dayfirst=True) for strDT in _TrendDF['DateTime']]
            super().__init__(_TrendDF.set_index('DateTime',drop=drop_index_col),**kwargs)
        else:
            super().__init__(data=data,**kwargs)
        

    @staticmethod
    def remove_col_bracket(DF, delimiter = '['):
        '''rename column name by getting the str before a certain limiter e.g. sqaure bracket "[" '''
        return DF.rename(lambda col:col.split(delimiter)[0],axis='columns')   
    
    