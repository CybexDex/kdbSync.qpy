from distutils.core import setup
from distutils.core import Extension
import platform
import sys


def InstallPython():
    
    plate = platform.architecture()
    strbit= plate[0]
    iswin = 'Windows' in platform.system();
    version=sys.version  
    #print(version);
    verss=version.split()[0].split('.');
    ver=int(verss[0])+float(verss[1])/10;
    bit=int(strbit.split('bit')[0]);

    if(len(sys.argv)<=1):
        print('No kdb_q path!');
        return;
    print(sys.argv[1:])

    srcpath=sys.argv[1];
    if(iswin):
        if not (srcpath.endswith('\\')):
            srcpath=srcpath+'\\'
    else:     
        if not (srcpath.endswith('/')):
            srcpath=srcpath+'/'
        
    sitepath=".";
    if(bit==64 ):
        print('Python is 64 bits')
        srcpath=srcpath+"lib"
    else:
        print('Python is 32 bits')
        srcpath=srcpath+"lib"
    for x in sys.path:
        ix=x.find('site-packages')
        iy=x.find('dist-packages')
        if( (ix>=0 and x[ix:]=='site-packages') or (iy>=0 and x[iy:]=='dist-packages')):
            sitepath=x;
            if(iswin):
                filepath=sitepath+"\\kdbq.pth"
            else:
                filepath=sitepath+"/kdbq.pth"
            if(ver<2.6):
                print('Error: Python version must be >=2.6!')
                return;

            #print(srcpath);
            sitefile=open(filepath,'w');
            sitefile.writelines(srcpath)
            sitefile.close();
    print('Installed into'),
    print(sitepath),
    print('OK!');


InstallPython()
