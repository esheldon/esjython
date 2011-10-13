"""
    %prog [options] dataset band

Look up all coadd images in the input dataset and bandpass, find the SE images
that were used as input, and write out the coadd id and se filename info.
Dataset is something like 'dc6b' or 'dr012'

columns will be

coadd_id,id,filetype,run,exposurename,filename,band,ccd

"""
import os
import sys
from sys import stderr
import des
import csv

from optparse import OptionParser
parser=OptionParser(__doc__)
parser.add_option("-u","--user",default=None, help="Username.")
parser.add_option("-p","--password",default=None, help="Password.")
parser.add_option("-v","--verbose",action="store_true",default=False, 
                  help="Print out queries as they are executed.")

def main():

    options,args = parser.parse_args(sys.argv[1:])

    if len(args) < 2:
        parser.print_help()
        sys.exit(45)


    dataset=args[0].strip()
    band=args[1].strip()
    verbose=options.verbose

    release=des.desdb.dataset2release(dataset)

    # ugh, jython is still on 2.5, no nice string formatting
    query="""
    SELECT
        id
    FROM
        %s_files
    WHERE
        filetype='coadd'
        and band = '%s'\n""" % (release,band)

    desdb=des.desdb.Connection(user=options.user,password=options.password)

    res = desdb.execute(query, show=verbose)
    first=True
    for iddict in res:
        coadd_id = iddict['id']
        query="""
        SELECT
            image.parentid
        FROM
            image,coadd_src
        WHERE
            coadd_src.coadd_imageid = %d
            AND coadd_src.src_imageid = image.id\n""" % coadd_id

        res = desdb.execute(query, show=verbose)

        idlist = [] 
        idlist = [str(d['parentid']) for d in res]

        ftype=None
        itmax=5

        i=0 
        while ftype != 'red' and i < itmax:
            idcsv = ', '.join(idlist)

            query="""
            SELECT
                id,
                imagetype,
                parentid
            FROM
                image
            WHERE
                id in (%s)\n""" % idcsv

            res = desdb.execute(query, show=verbose)
            idlist = [str(d['parentid']) for d in res]
            ftype = res[0]['imagetype']
            
            if verbose: stderr.write('ftype: %s' % ftype)
            i+=1

        if ftype != 'red':
            raise ValueError("Reach itmax=%s before finding 'red' images. last is %s" % (itmax, ftype))

        if verbose: stderr.write("Found %d red images after %d iterations" % (len(idlist),i))

        # now the idlist comes from id instead of parentid
        query="""
        select
            %s as coadd_id,
            id,
            filetype,
            run,
            exposurename,
            band,
            ccd,
            filename
        from
            location
        where
            id in (%s)
        """ % (coadd_id,idcsv)


        if first:
            header=True
        else:
            header=False

        desdb.executeWrite(query, show=verbose, header=header)

        first=False

if __name__=="__main__":
    main()
