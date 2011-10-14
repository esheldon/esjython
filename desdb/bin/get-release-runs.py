"""
    %prog [options] release

Print all red runs for input dataset.

"""
import os
import sys
from desdb import desdb
import csv

from optparse import OptionParser
parser=OptionParser(__doc__)
parser.add_option("-u","--user",default=None, help="Username.")
parser.add_option("-p","--password",default=None, help="Password.")

def main():

    options,args = parser.parse_args(sys.argv[1:])

    if len(args) < 1:
        parser.print_help()
        sys.exit(45)

    release=args[0].strip()

    # ugh, jython is still on 2.5, no nice string formatting
    query="""
    select
        distinct(run)
    from
        %s_files
    where
        filetype='red'\n""" % release

    desdb=desdb.desdb.Connection(user=options.user,password=options.password)

    r = desdb.executeWrite(query,show=True)

if __name__=="__main__":
    main()
