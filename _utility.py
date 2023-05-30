from pathlib import Path

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

def file_dir(file_dir):
    '''check if file_dir is a valid file path and return a path object'''
    try:
        file_path = Path(file_dir)
        if file_path.is_file():
            return file_path
        else:
            print(f'{file_dir} is not a valid file path')
    except:
        print(f'{file_dir} cannot be converted to Path object')