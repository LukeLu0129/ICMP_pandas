from .ARTF import Artf_DataFrame 
from .Event import Event_DataFrame 
from .Trend import Trend_DataFrame 
from .ICMP_h5 import ICMP_h5py 
from ._utility import *

def read_h5(*args, **kwargs):
    '''read file (hdf5) as a h5py subclass called ICMP_h5'''
    return ICMP_h5py(*args, **kwargs)

def read_Event(file_dir=None,*args, **kwargs):
    '''read file (hdf5, xml, csv) as a pd.DataFrame subclass called Event_DataFrame'''
    return Event_DataFrame(file_dir, *args, **kwargs)


def read_Trend(file_dir=None,*args, **kwargs):
    '''read file (hdf5, xml, csv) as a pd.DataFrame subclass called Trend_DataFrame'''
    return Trend_DataFrame(file_dir, *args, **kwargs)


def read_Artf(artf_source=None, *args, **kwargs):
    '''read file (artf, xml) as a pd.DataFrame subclass called Artf_DataFrame'''
    return Artf_DataFrame(artf_source, *args, **kwargs)

