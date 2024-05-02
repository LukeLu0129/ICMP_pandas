import pandas as pd
import xml.etree.ElementTree as ET
from datetime import datetime as dt
from collections import defaultdict as dd
from ._utility import subDF_str_attr
from pathlib import Path


class ArtfSeries(pd.Series):
    '''handle pandas internal operation that utilise pd.Series. This ensure pandas method will return ArtfDataFrame instead pd.DataFrame'''
    @property
    def _constructor(self):
        return ArtfSeries
    @property
    def _constructor_expanddim(self):
        return Artf_DataFrame


class Artf_DataFrame(pd.DataFrame):
    '''subclass of pd.DataFrame that can be saved to artf file for ICM+'''
    @property
    def _ArtfDF_dict(self):
        '''ArtfDF dict template'''
        return {"signal_label":[],"ModifiedBy":[],"ModifiedDate":[],"StartTime":[],"EndTime":[]}

    @property
    def _constructor(self):
        '''Overwrite internal method for compatibility'''
        return Artf_DataFrame

    @property
    def _constructor_sliced(self):
        '''Overwrite internal method for compatibility'''
        return ArtfSeries

    def __init__(self,*args, artf_source=None, ModifiedByDefault = 'ArtfDataFrame', default_signal = 'Global', **kwargs) -> None:
        '''
        takes csv file, artf file, dict, ArtSeries and pd.Series and pd.DataFrame with the essential keys transform into pd.DataFrame subclass that is capable of output artf file.
        default_signal is only relavent when artf_source is a dict or list

        '''
        if isinstance(artf_source,(str,Path)): # check if artf_source is a file directory
            dir = Path(artf_source)
            if dir.is_file():
                filetype = dir.name.split('.')[-1]
                # read csv:
                if filetype == 'csv':
                    artf_df_arg = self._from_artf_csv(artf_source)
                # read artf as xml
                elif filetype == 'artf':
                    artf_df_arg = self.convert_artf(artf_source)
                else:
                    raise Exception('artf_source must be a directory of csv or artf file')
            else:
                raise Exception('artf_source is not a valid file directory')
        elif isinstance(artf_source,dict): 
            'take dictionary with "StartTime" and "EndTime" key and fill for signal_label, ModifiedBy and ModifiedDate.'
            artf_df_arg = self._from_startstop_dict(startstop_dict=artf_source, ModifiedByDefault = ModifiedByDefault, default_signal=default_signal, **kwargs)
        elif isinstance(artf_source,list):
            artf_df_arg = self.from_startstop_dict(self.startstop_dict_from_dt_list(artf_source), ModifiedByDefault=ModifiedByDefault,default_signal=default_signal, **kwargs)
        elif isinstance(artf_source,(pd.Series,ArtfSeries)):
            artf_df_arg = artf_source.to_frame().transpose()
            self._check_essential_key(artf_df_arg.columns, 'series_df')
        elif isinstance(artf_source,pd.DataFrame):
            artf_df_arg = artf_source
            self._check_essential_key(artf_df_arg.columns, 'source_df')
        elif artf_source == None:
            artf_df_arg = pd.DataFrame.from_dict(self._ArtfDF_dict)
        else:
            return None
        if list(args) != [] or kwargs.get('data',None) != None:
            super().__init__(*args, **kwargs)
        else:
            super().__init__(data=artf_df_arg, **kwargs)
        del artf_df_arg
        self._ModifiedByDefault = subDF_str_attr(ModifiedByDefault)
    
    @property
    def ModifiedByDefault(self):
        '''Default value of ModifiedBy in the Artf file'''
        return str(self._ModifiedByDefault)
    
    @classmethod
    def from_artf(cls, artf_xml_dir,return_ET = False):
        return cls(artf_source=cls().convert_artf(artf_xml_dir, return_ET = return_ET))

    def read_artf(self,artf_xml_dir):
        '''read artf in xml format. same as open artf as txt in notepad'''
        return self.convert_artf(artf_xml_dir,return_ET =True)

    def convert_artf(self, artf_xml_dir, return_ET = False)-> pd.DataFrame or ET:
        '''Extract all artefact entry from artf file and into dataframe'''
        # artf_df_dict = {"signal_label":[],"ModifiedBy":[],"ModifiedDate":[],"StartTime":[],"EndTime":[]}
        artf_df_dict = dict(self._ArtfDF_dict)
        root = ET.parse(artf_xml_dir).getroot()
        for elem in root.findall("./*"):
            if elem.tag == "Global":
                SignalLabel = "Global"
            else:
                SignalLabel = elem.get('Name')
            for artf in elem.findall('Artefact'):
                artf_dict = dict(artf.attrib)
                artf_dict.update({"signal_label":SignalLabel})
                for k,v in artf_dict.items():
                    artf_df_dict[k].append(v)
        if return_ET:
            return root
        else:
            return artf_df_dict

    @classmethod
    def from_artf_csv(cls, artf_csv_dir):
        return cls(artf_source=cls()._from_artf_csv(artf_csv_dir))

    def _from_artf_csv(self,artf_csv_dir) -> pd.DataFrame:
        '''Extract csv as dataframe and check is all essentail column is present.'''
        csv_df = pd.read_csv(artf_csv_dir)
        self._check_essential_key(csv_df.columns, 'csv_df')
        return csv_df.loc[:,self._ArtfDF_dict.keys()]\
    
    @classmethod
    def from_dt_list(cls,dt_list,signal_label='Global', dt_interval = '10s',window_before='5s', window_after='5s',dayfirst=True, **kwargs):
        return cls(artf_source=cls.startstop_dict_from_dt_list(dt_list,signal_label=signal_label, dt_interval = dt_interval, window_before=window_before, window_after=window_after,dayfirst=dayfirst), **kwargs)

    @staticmethod
    def startstop_dict_from_dt_list(dt_list,signal_label, dt_interval = '10s',window_before='5s', window_after='5s',dayfirst=True):
        '''take list of datetime and group consective datetime into to a dict with StartTime and EndTime.
        dt_interval should be no less then sample frequency.'''
        dt_list = sorted(pd.to_datetime(dt_list, dayfirst=dayfirst))
        dt_interval = pd.to_timedelta(dt_interval)
        window_after = pd.to_timedelta(window_after)
        window_before = pd.to_timedelta(window_before)
        dt_dict = {'signal_label':[],'StartTime':[], 'EndTime':[]}
        start_dt = dt_list[0]
        current_dt = start_dt
        last_dt = dt_list[-1]
        for next_dt in dt_list[1:]:
            if next_dt > current_dt+dt_interval:
                dt_dict['signal_label'].append(signal_label)
                dt_dict['StartTime'].append(start_dt-window_before)
                dt_dict['EndTime'].append(current_dt+window_after)
                start_dt = next_dt
            current_dt = next_dt
            if current_dt == last_dt:
                dt_dict['signal_label'].append(signal_label)
                dt_dict['StartTime'].append(start_dt-window_before)
                dt_dict['EndTime'].append(current_dt+window_after)
                return dt_dict

    @classmethod
    def from_startstop_dict(cls,startstop_dict, ModifiedByDefault = 'ArtfDataFrame', default_signal = 'Global', **kwargs):
        return cls(artf_source=cls()._from_startstop_dict(startstop_dict=startstop_dict, ModifiedByDefault = ModifiedByDefault, default_signal = default_signal, **kwargs))

    def _from_startstop_dict(self,startstop_dict, ModifiedByDefault,default_signal, **kwargs):
        '''
        
        '''
        first_value = list(startstop_dict.values())[0]
        if isinstance(first_value,list):
            value_len = len(first_value)
        else:
            value_len = 1
        if 'signal_label' not in startstop_dict.keys():
            startstop_dict.update({'signal_label':[default_signal for _ in range(value_len)]})
        if 'ModifiedBy' not in startstop_dict.keys():
            startstop_dict.update({'ModifiedBy':[ModifiedByDefault for _ in range(value_len)]})
        if 'ModifiedDate' not in startstop_dict.keys():
            now_dt = dt.now().strftime("%d/%m/%Y %H:%M:%S")
            startstop_dict.update({'ModifiedDate':[now_dt for _ in range(value_len)]})
        self._check_essential_key(startstop_dict.keys(), 'dict')
        return pd.DataFrame(startstop_dict, **kwargs)

    def _check_essential_key(self, key_list, input_type='artf_source'):
        '''Raise exception if missing essential key'''
        for essential_key in self._ArtfDF_dict.keys():
            if not essential_key in key_list:
                raise Exception(f'{input_type} is missing essential key/column "{essential_key}"')

    def to_df(self)-> pd.DataFrame:
        '''return a DataFrame object'''
        return pd.DataFrame(self)

    @property
    def artf_xml(self) -> ET.ElementTree:
        '''convert dataframe to xml format compatible for artf file'''
        root = ET.Element('ICMPArtefacts')
        ## convert pd.timestamp to str
        SignalNames = self.signal_label.drop_duplicates()
        for Name in SignalNames:
            if Name != "Global":
                ET.SubElement(root,"SignalGroup",attrib= {"Name": Name}) # for series artf
            else:
                ET.SubElement(root, Name)   # for global artf
        for row in self.strdf.iterrows():
            row = row[1]
            SignalName = row.signal_label
            if SignalName != "Global":
                signal_group = root.find(f".*/[@Name='{SignalName}']")
            else:
                signal_group = root.find("Global")
            attrib_dict = {"ModifiedBy": row.ModifiedBy, "ModifiedDate":row.ModifiedDate,"StartTime":row.StartTime, "EndTime":row.EndTime}
            ET.SubElement(signal_group,"Artefact",attrib=attrib_dict)
        ET.indent(root)
        return root

    def show_artf(self):
        '''show the xml format of the artf'''
        return ET.dump(self.artf_xml)

    def save_artf(self, folder_dir=None, file_name:str =None):
        """file_suffix is only available if file_name is not specified. Default = '_test'."""
        if folder_dir is None:
            folder_dir = self.folder_dir
        if file_name is None:
            file_name = self.study_file_name
        save_dir = folder_dir+file_name+".artf"
        try:
            ET.ElementTree(self.artf_xml).write(save_dir)
            print(f"artf file saved to {save_dir}")
        except:
            print(f'failed to save artf file to {save_dir}')

    @staticmethod
    def to_strftime(Time, to_ms= True):
        '''convert str or pd.ts to appropriate str format for artf'''
        if isinstance(Time, pd.Timestamp):
            if to_ms:
                str_format = "%d/%m/%Y %H:%M:%S.%f"
                return pd.to_datetime(Time, dayfirst=True).strftime(str_format)[:-3]
            else: 
                str_format = "%d/%m/%Y %H:%M:%S"
                return pd.to_datetime(Time, dayfirst=True).strftime(str_format)
        else:
            return Time

    def append_artf_ts(self,signal_label="Global",StartTime=None, EndTime='None'):
        '''Add a single entry of artf_ts'''
        attrib_dict = {"signal_label":signal_label, "ModifiedBy": self.ModifiedByDefault, "ModifiedDate":dt.now().strftime("%d/%m/%Y %H:%M:%S"),
                        "StartTime":self.to_strftime(StartTime), "EndTime":self.to_strftime(EndTime)}
        return self.append(attrib_dict,ignore_index=True)

    @property
    def dtdf(self):
        '''retrun ArtfDataFrame with all time related entry converted to timestamp.'''
        out_artf = self.copy()
        out_artf[['ModifiedDate','StartTime','EndTime']] = out_artf[['ModifiedDate','StartTime','EndTime']].applymap(lambda t:pd.to_datetime(t, dayfirst=True))
        return out_artf
    
    @property
    def strdf(self):
        '''
        return ArtfDataFrame with all timestamp converted to appropriate str format.
        This is for compatible formatting when writing into artf file.
        '''
        out_artf = self.copy()
        out_artf.ModifiedDate = out_artf.ModifiedDate.apply(lambda t:self.to_strftime(t,False))
        out_artf[['StartTime','EndTime']] = out_artf[['StartTime','EndTime']].applymap(lambda t:self.to_strftime(t))
        return out_artf
        