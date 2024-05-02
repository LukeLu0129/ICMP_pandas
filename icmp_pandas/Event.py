import pandas as pd
import lxml.etree as ET
import h5py
from ._utility import _ICMPEvent_h5dir, _FieldValue_xmldir, _Event_xmldir, get_file_path
from .Episode import Episode_Dataframe
from collections import defaultdict as dd


class Event_Series(pd.Series):
    '''handle pandas internal operation that utilise pd.Series. This ensure pandas method will return Event_DF instead pd.DataFrame'''
    @property
    def _constructor(self):
        return Event_Series
    @property
    def _constructor_expanddim(self):
        return Event_DataFrame

class Event_DataFrame(pd.DataFrame):
    '''This read ICMP+ event files into pandas.DataFrame'''    
    @property
    def _constructor(self):
        '''Overwrite internal method for compatibility'''
        return Event_DataFrame

    @property
    def _constructor_sliced(self):
        '''Overwrite internal method for compatibility'''
        return Event_Series

    def __init__(self,*args,file_dir:str=None, **kwargs):
        '''
        Accept directory of ICM+ generated event file in the following formats (csv, txt, xml, hdf5). Return empty DataFrame if file path is invalid.        
        '''
        if  get_file_path(file_dir) == None:
            super().__init__(*args, **kwargs)
        else:
            patientFileName = file_dir.split('\\')[-1].split('_event')[0]
            file_type = file_dir.split('.')[-1]
            if file_type == 'csv':
                event_df = pd.read_csv(file_dir,delimiter=r',(?![^\[]*[\]])', engine="python")
            elif file_type == 'txt':
                event_df = pd.read_table(file_dir,delimiter='\t')
            elif file_type == 'xml':
                xroot = ET.parse(file_dir).getroot()
                event_df = self._EventXML2Dataframe(xroot,patientFileName)
            elif file_type == 'hdf5':
                H5file = h5py.File(file_dir, 'r')
                H5EventXML_string_in_bytes = H5file.get(_ICMPEvent_h5dir)[()][0]
                xroot = ET.fromstring(H5EventXML_string_in_bytes)
                event_df = self._EventXML2Dataframe(xroot,patientFileName)
                H5file.close()
            super().__init__(event_df)

    def _EventXML2Dataframe(self,xroot,DataSource):
            '''
            convert event.xml to event_df
            '''
            # create a list of Data entry
            DataFieldElementList = xroot.findall(_FieldValue_xmldir)
            DataFieldList = ['DataSource','EventGroup','EventName','Category','StartTime','EndTime','DataFields','Comments']

            EventDict = {q:[] for q in DataFieldList}
            # find all the event entry
            for event in xroot.findall(_Event_xmldir):
                # create a temporary dictionary to store data entry within single event
                EventDict['DataSource'].append(DataSource)
                EventDict['EventGroup'].append(event.attrib['Group'])
                EventDict['EventName'].append(event.attrib['Name'])
                EventDict['Category'].append(event.attrib['Category'])
                EventDict['StartTime'].append(event.attrib['StartTime'])
                try: 
                    EventDict['EndTime'].append(event.attrib['EndTime'])
                except:
                    EventDict['EndTime'].append(float('nan'))
                comment = event.find("Comment").text
                if comment is None:
                    comment = float('nan')
                EventDict['Comments'].append(comment)
                # record FieldValue of single event into a list of string with same format according to ICM+ csv
                DataFieldstr = '['
                for fieldvalue in event.findall("FieldValue"):
                    DataFieldstr += f"{fieldvalue.attrib['Name']}:{fieldvalue.attrib['Value']}|"
                DataFieldstr = DataFieldstr[:-1] + ']'
                if len(DataFieldstr) <3:
                    DataFieldstr = float('nan')
                EventDict['DataFields'].append(DataFieldstr)
            return pd.DataFrame(EventDict) 
    
    def getEvent(self, EventList, Event_col = 'EventName'):
        '''Extract Tier events'''
        return self.query(f"{Event_col} in @EventList", engine='python')

    def datafield_df(self, datafield_col = 'DataFields'):
        DataField_series = self.loc[:,datafield_col]
        l_str = DataField_series.str.strip('[]').str.split('|')
        DataField_dict = dd(list)
        nan_list = []
        for dict_list in l_str.values:
            key_list = []
            if isinstance(dict_list, list):
                for item in dict_list:
                    key, val = item.split(':')
                    key_list.append(key)
                    # padding with nan for new column
                    if not key in DataField_dict.keys():
                        DataField_dict[key] = nan_list.copy()
                    DataField_dict[key].append(val)
                
                for key in DataField_dict.keys():
                    # adding nan to col with no new entry
                    if not key in key_list:
                        DataField_dict[key].append(float('nan'))
            nan_list.append(float('nan'))
        return pd.DataFrame.from_dict(DataField_dict)
