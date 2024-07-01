from pathlib import Path
import pandas as pd

# Define class variable
_EmptyEntryFiller = float('nan')

## HDF5 directory related variable
_Trend_h5dir ='summaries/minutes'
_ICMPEvent_h5dir='aux/ICM+/icmevents'
## event xml directory related variable
_FieldValue_xmldir = "EventsConfig/EventGroup/EventType/DataField"
#_FieldValue_xmldir = "Events/Event/FieldValue"
_Event_xmldir = "Events/Event"
_Note_dir = "annotations/notes"


class subDF_str_attr:
    '''exist only for adding pd.DataFrame subclass attribute, since adding str as DF subclass attribute will trigger internal error'''
    def __init__(self, attr) -> None:
        self.attr = attr

    def __repr__(self) -> str:
        return str(self.attr)

    def __call__(self) -> str:
        return str(self.attr)

def get_file_path(file_dir):
    '''check if file_dir is a valid file path and return a path object'''
    try:
        file_path = Path(file_dir)
        if file_path.is_file():
            return file_path
        else:
            print(f'{file_dir} is not a valid file path')
            return None
    except:
        # print(f'{file_dir} cannot be converted to Path object')
        return None

def get_file_list(folder_dir, file_type = None, include_subfolders = False, export_txt_dir = None, export_path_obj = False, glob_regex = None):
    '''
    get a list of file path.
    file_type and inlcude_subfolders are ignored when glob_regex is used.
    export_txt_dir specify the directory of the txt file output. Please include full file name along with ".txt".
    '''
    if file_type is None and glob_regex is None:
        return None
    if glob_regex is None:
        glob_regex = f'*{file_type}'
        if include_subfolders:
            glob_regex = "**/" + glob_regex

    out_list = list(Path(folder_dir).glob(glob_regex))
    if not export_path_obj:
        out_list = [str(i) for i in out_list]

    if not export_txt_dir is None:
        with open(export_txt_dir,'w') as output:
            for file_dir in out_list:
                output.write(str(file_dir)+'\n')
        print(f"file saved to {export_txt_dir}")
        return None

    return out_list

###====Human readable & serial datetime converter    
def Ser2DT(serDT) -> pd.Timestamp:
    '''change serial datetime in excel to human readable datetime accuracy to 9th decimal'''
    # from serial datetime (excel compatible) to human readable datetime
    try:
        return pd.to_datetime('1899-12-30') + pd.to_timedelta(serDT,'D')
    except:
        raise Exception('Input type not float or np.float64')
        
def DT2Ser(DT) -> float:
    '''change human readable datetime to serial datetime in excel accuracy to 9th decimal'''
    # From human readable datetime string to serial datetime
    try:
        date1= pd.to_datetime(DT.replace('.',':')) # for some reason, icmevents in HDF5 will record time as xx.xx.xx which should be corrected to xx:xx:xx
    except:
        date1= DT
        
    temp = pd.to_datetime('1899-12-30')    # Note, not 31st Dec but 30th!
    delta = date1 - temp
    ser = float(delta.days) + float(delta.seconds/(86400)+delta.microseconds/(86400*10**6))
    return ser     

def filter_by_id(pd, id_list):
        '''filter the dataframe by id_list'''
        return pd.filter(items=id_list,axis=0)