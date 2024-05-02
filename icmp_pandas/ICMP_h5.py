import pandas as pd
import h5py
from collections import defaultdict as dd
import warnings
from tables import NaturalNameWarning
warnings.filterwarnings('ignore', category=NaturalNameWarning)
warnings.simplefilter(action='ignore', category=FutureWarning)

from ._utility import _Note_dir
from .Event import Event_DataFrame 
from .Trend import Trend_DataFrame



class ICMP_h5py(h5py.File):    

    '''extension of h5py.File
    initialise with a file directory to an HDF5 file generated from ICM+ packaging

    '''
    
    def __init__(self,file_dir, **kwargs):
        super().__init__(file_dir, **kwargs)
        self._file_dir = file_dir

    @property
    def NoteSeries(self):
        '''series extract from .hdf5'''
        try:
            H5Note = pd.DataFrame(self.get(_Note_dir)[()])
            H5Note.starttime = [pd.Timestamp.fromtimestamp(timestamp/1e6) for timestamp in H5Note.starttime]
            return H5Note.set_index('starttime').text.rename('Note').str.decode("utf-8")
        except:
            print('Note not found')
            return None
    
    @property
    def package_log(self):
        try:
            return pd.Series(self.get('packaging.log')[()]).str.decode("utf-8")
        except:
            print('package log not found')
            return None
    

    @property
    def EventDF(self):
        return Event_DataFrame(file_dir=self._file_dir)
    
    @property
    def TrendDF(self):
        return Trend_DataFrame(file_dir=self._file_dir)
    
    def RawSignal(self, GroupDir, Dataset):
        try:
            DTidx = self.getDTidx_fromH5Index(self.get(f'{GroupDir}/{Dataset}.index')[()])
            return pd.Series(self.H5PY.get(f'{GroupDir}/{Dataset}')[()][:len(DTidx)], index=DTidx)
        except:
            raise Exception(f'Unable to extract {Dataset} from {GroupDir}/{Dataset}')
    
    def get_numerics(self,data_label):
        return self.RawSignal('numerics',data_label)
    
    def get_waves(self,data_label):
        return self.RawSignal('waves',data_label)
    
    @staticmethod
    def getDTidx_fromH5Index(IndexDF) -> list:
        '''Extract DateTimeIndex of a raw signal (in ICM+ HDF5 format)'''
        if not isinstance(IndexDF, pd.DataFrame):
            IndexDF = pd.DataFrame(IndexDF)
        UnixDTidxlist=[]
        for Index in IndexDF.iterrows():
            startidx = Index[1].startidx
            starttime = Index[1].starttime.astype('uint64')
            length = Index[1].length.astype('uint64')
            freq = Index[1].frequency
            if freq <=0:
                continue
            usec_per_sample = (1e6/freq).astype('uint64') 
            endtime = (starttime+usec_per_sample*length).astype('uint64') 
            timelist = list(range(starttime,endtime,usec_per_sample))
            checklength = len(timelist) == length
            if not checklength:
                print(len(timelist),length)
            UnixDTidxlist.extend(timelist)
        return pd.to_datetime(UnixDTidxlist, unit='us')
    
    @property
    def FileAttribute(self):        
        fileAttrList = list(self.get('/').attrs.items())
        fileAttrDict = {i[0]:i[1][0] for i in fileAttrList}
        return fileAttrDict   
        
    @property
    def ICMPVersion(self):
        dataCollection = self.FileAttribute['dataCollectionSoftware']
        dataProcession = self.FileAttribute['dataProcessingSoftware']
        return f'Data Collected with {dataCollection}, Processed with {dataProcession}'   

    @property       
    def RecordingStartTime(self):
        return pd.to_datetime(self.FileAttribute['dataStartTime'].replace('.',':'))

    @property       
    def RecordingEndTime(self):
        return pd.to_datetime(self.FileAttribute['dataEndTime'].replace('.',':'))

    @property       
    def RecordingDuration(self):
        return self.RecordingEndTime - self.RecordingStartTime
    
    @property
    def TrendInfo(self):
        print(f'Patient ID: {self.PatientID}')
        print(f'Overall recording started at: {self.RecordingStartTime}')
        print(f'Overall recording ended at: {self.RecordingEndTime}')
        print(f'Overall recording duration: {self.RecordingDuration}')

    def get_labels(self, parent_group):
        out_lst = []
        self.visit(lambda x: out_lst.append(x.split('/')[1]) if (f"{parent_group}/" in x) and ("." not in x) else None)
        return out_lst
    
    @property
    def waves_label(self):
        '''output all labels in waves as list'''
        return self.get_labels('waves')
    
    @property
    def numerics_label(self):
        '''output all labels in waves as list'''
        return self.get_labels('numerics')