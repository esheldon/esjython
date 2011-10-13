"""
    %prog [options] dataset band

Look up all coadd images in the input dataset and write out their file ids,
along with some other info. Dataset is something like 'dc6b', which implies
a release, or a release id like 'dr012'

"""
import os
import sys
import des
import csv

from optparse import OptionParser
parser=OptionParser(__doc__)
parser.add_option("-u","--user",default=None, help="Username.")
parser.add_option("-p","--password",default=None, help="Password.")

def main():

    options,args = parser.parse_args(sys.argv[1:])

    if len(args) < 2:
        parser.print_help()
        sys.exit(45)


    dataset=args[0].strip()
    band=args[1].strip()

    release=des.desdb.dataset2release(dataset)

    # ugh, jython is still on 2.5, no nice string formatting
    query="""
    select
        id,filetype,run,tilename,band,filename
    from
        %s_files
    where
        filetype='coadd' %s
        and band = '%s'\n""" % (release,band)

    desdb=des.desdb.Connection(user=options.user,password=options.password)

    r = desdb.executeWrite(query,show=True)

if __name__=="__main__":
    main()
