import pandas as pd
from collections import defaultdict as dd
import warnings

class Episode_Series(pd.Series):
    '''handle pandas internal operation that utilise pd.Series. This ensure pandas method will return Episode_Series instead pd.DataFrame'''
    @property
    def _constructor(self):
        return Episode_Series
    @property
    def _constructor_expanddim(self):
        return Episode_Dataframe
    
class Episode_Dataframe(pd.DataFrame): 

    def _constructor(self,DF, *args, **kwargs):
        return Episode_Dataframe(DF, *args, **kwargs)

    @property
    def _constructor_sliced(self):
        '''Overwrite internal method for compatibility'''
        return Episode_Series

    def __init__(self, DataFrameArg = {'StartDatetime':[],'DurationTimedelta':[],'EndDatetime':[]}, *args, **kwargs):#, *args, **kwargs):
        '''initialise a pd.DataFrame with three columns (StartDatetime, DurationTimedelta, EndDatetime)'''
        super().__init__(DataFrameArg, *args, **kwargs)

    def getProlongEpi(self,ProlongEpiDuration = pd.to_timedelta('30min')):
        '''Filtered and extract prolong critical episodes'''
        dtype = type(ProlongEpiDuration)
        if dtype != pd.Timestamp and dtype == str:
            ProlongEpiDuration = pd.to_timedelta(ProlongEpiDuration)
        try:
            return self._constructor(self[self.DurationTimedelta > pd.to_timedelta(ProlongEpiDuration)])
        except:
            return self._constructor(self)  


    def addEvents(self, EventSeries,col_name, ValueOnly = False):
        '''Add a new columns with dict of event logged within an EpisodeDF'''
        EpisodeDF = self
        if not isinstance(EventSeries,pd.Series):
            warnings.warn('"addEvents" only accept pd.Series')
            return self
        EpiIntervDict = {'EpiNumIndex':[],'EventsDict':[]}
        OutboundEventList = []
        EventDTList = list(EventSeries.index)
        EventDT = EventDTList.pop(0)
        for ValidEpisode in EpisodeDF.iterrows():
            EventsDict = dd(pd.Timestamp)
            EpiNum = ValidEpisode[0]
            Episode = ValidEpisode[1].to_dict()
            EpiStartDT = Episode['StartDatetime']
            EpiEndDT = Episode['EndDatetime'] 
            while EventDTList:
                if EventDT >= EpiStartDT and EventDT <= EpiEndDT:
                    EventsDict[EventDT]=EventSeries.loc[EventDT]                  
                elif EventDT < EpiStartDT:
                    OutboundEventList.append(EventDT)    
                if EventDT <= EpiEndDT:
                    EventDT = EventDTList.pop(0) 
                if EventDT > EpiEndDT or not EventDTList:
                    if EventDT >= EpiStartDT and EventDT <= EpiEndDT:
                        EventsDict[EventDT]=EventSeries.loc[EventDT]   
                    if EventsDict:
                        EpiIntervDict['EpiNumIndex'].append(EpiNum)
                        if ValueOnly:
                            EpiIntervDict['EventsDict'].append(EventsDict.values())   
                        else:
                            EpiIntervDict['EventsDict'].append(EventsDict)   
                    break                

        return self._constructor(EpisodeDF.join(pd.Series(EpiIntervDict['EventsDict'],name=col_name,index=EpiIntervDict['EpiNumIndex'],dtype='object')))


    def getEmptyInterv(self):
        '''Extract Episode with no intervention logged (IntervDict is empty)'''
        if 'IntervsDict' not in self.columns:
            warnings.warn('"IntervsDict" column not found, try "addIntervs()" first.')
            return self             
        return self._constructor(self[self.IntervsDict.isnull()])


    # def getViolationEvent(self, MAX_TIME_GAP_TILL_DEVIATION = None): 
    #     '''Iterate through EpisodeDF and the IntervDict within then extract event where time gap exceed "MAX_TIME_GAP_TILL_DEVIATION" '''
    #     if 'IntervsDict' not in self.columns:
    #         warnings.warn('"IntervsDict" column not found, try "addIntervs()" first.')
    #         return self       
    #     if not MAX_TIME_GAP_TILL_DEVIATION:
    #         MAX_TIME_GAP_TILL_DEVIATION = self.BONANZA_main.MaxDurationTillViolation
    #     elif isinstance(MAX_TIME_GAP_TILL_DEVIATION, str):
    #         MAX_TIME_GAP_TILL_DEVIATION = pd.to_timedelta(MAX_TIME_GAP_TILL_DEVIATION)
    #     elif isinstance(MAX_TIME_GAP_TILL_DEVIATION, pd.Timestamp):
    #         pass
    #     else:
    #         print('MAX_TIME_GAP_TILL_DEVIATION must be pd.Timestamp or a str value')
    #         return None
    #     ProlongEpiWithIntervDF = self.query('IntervsDict.notnull()', engine='python')
    #     ProtocolDeviationEventDict = dd(list)
    #     for ProlongEpiWithInterv in ProlongEpiWithIntervDF.iterrows():
    #         ProlongEpisode = ProlongEpiWithInterv[1].to_dict()
    #         DatetimeList = list(ProlongEpisode['IntervsDict'].keys()) + [ProlongEpisode['EndDatetime']]
    #         LastDT = ProlongEpisode['StartDatetime']
    #         for datetime in DatetimeList:
    #             DTInterval = datetime - LastDT 
    #             if DTInterval > MAX_TIME_GAP_TILL_DEVIATION:
    #                 ProtocolDeviationEventDict['StartDatetime'].append(LastDT)
    #                 ProtocolDeviationEventDict['DurationTimedelta'].append(DTInterval)
    #                 ProtocolDeviationEventDict['EndDatetime'].append(datetime)
    #             LastDT = datetime             
    #     return self._constructor(ProtocolDeviationEventDict, dtype='timedelta64[ns]')


    def getAllDeviationDF(self, MAX_TIME_GAP_TILL_DEVIATION):
        '''output an EpisodeDF where each episode is a potential violation event and added "Info" column explaining the type of violation event'''
        if 'IntervsDict' not in self.columns:
            warnings.warn('"IntervsDict" column not found, try "addIntervs()" first.')
            return self             
        EmptyProlongEpiDF = self.query('IntervsDict.isnull()', engine='python')
        ProtocolDeviationDF = self.getDeviaitionEventDF(MAX_TIME_GAP_TILL_DEVIATION)
        InfoList = []
        DeviationEventDF = ProtocolDeviationDF.append(EmptyProlongEpiDF[['StartDatetime','DurationTimedelta','EndDatetime']], ignore_index = True)
        col={'StartDatetime':'Time of last valid event',
        'DurationTimedelta':'Duration',
        'EndDatetime':'Time of deviation event',
        'Info':'Type of deviation event'}

        for row in DeviationEventDF.iterrows():
            entry = row[1].to_dict()
            Start = entry['StartDatetime'].microsecond
            End = entry['EndDatetime'].microsecond

            FromEpiStart = True if Start !=0 else False
            TillEpiEnd = True if End !=0 else False
            if FromEpiStart and TillEpiEnd:
                InfoList.append('Empty Episode')
            elif FromEpiStart and not TillEpiEnd:
                InfoList.append('From start of episode to first intervention logged')
            elif not FromEpiStart and TillEpiEnd:
                InfoList.append('From last intervention logged to end of episode')
            else:
                InfoList.append('Between interventions')
        DeviationEventDF = DeviationEventDF.join(pd.Series(InfoList, name='Type of deviation event',dtype='object'))
        DeviationEventDF = DeviationEventDF.sort_values(by=['StartDatetime'])                                          
        return DeviationEventDF

    def addIntervCount(self):
        if 'IntervsDict' not in self.columns:
            warnings.warn('"IntervsDict" column not found, try "addIntervs()" first.')
            return self   
        return self.join(self.IntervsDict.str.len().replace(float('nan'),0).rename('Interventions_counts'))