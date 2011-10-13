"""
Based heavily off the DES trivialAccess.py
"""

import os
import sys
from sys import stderr
import csv

import java.sql
from java.sql import DriverManager
#import oracle

#from oracle.jdbc.driver import OracleDriver 
#from  ncsa import  OracleFits
#import oracle.jdbc.driver.OracleResultSetImpl
#import oracle.jdbc.driver.OracleResultSet
#import oracle.jdbc.driver

# a map so one can give a dataset or release
_release_map={'dc6b':'dr012', 'dr012':'dr012'}


def dataset2release(dataset):
    if dataset not in _release_map:
        raise ValueError("Unknown data set '%s'" % dataset)
    return _release_map[dataset]

class Connection:
    def __init__(self, user=None, password=None, host='leovip148.ncsa.uiuc.edu', 
                 port=1521, dbname='desoper', row_prefetch=10000):
        self.host=host
        self.port=port
        self.dbname=dbname
        self.row_prefetch=row_prefetch

        self.user=None
        self.url=None
        self.conn=None

        self._connect(user=user, password=password)

    def __repr__(self):
        rep=["DESDB Connection"]
        indent=' '*4
        rep.append("%surl: %s" % (indent,self.url))
        rep.append("%suser: %s" % (indent,self.user))
        rep.append("%sprefetch: %d" % (indent,self.row_prefetch))

        return '\n'.join(rep)

    def _connect(self, user=None, password=None):
        if self.conn is not None:
            raise RuntimeError("You are already connected")

        if user is not None or password is not None:
            if user is None or password is None:
                raise ValueError("Send either both or neither of user password")
        else:
            f=os.path.join( os.environ['HOME'], '.desdb_pass')
            data=open(f).readlines()
            if len(data) != 2:
                raise ValueError("Expected first line user second line pass in %s" % f)
            user=data[0].strip()
            password=data[1].strip()

        self.user=user                
        self.url =  "jdbc:oracle:thin://@%s:%s/%s" % (self.host, self.port, self.dbname)
        self.conn =  DriverManager.getConnection(self.url, user, password)
        self.conn.setDefaultRowPrefetch(self.row_prefetch) 

    def _extract_rset_value(self, rset, meta, colnum, strings=False):
        """
        Extract a value.  Default to string for non-numeric types
        """
        if strings:
            return rset.getString(colnum)
        dtype = meta.getColumnTypeName(colnum)
        if 'BINARY' in dtype:
            val =  rset.getDouble(colnum)
        elif dtype == 'NUMBER':
            scale = meta.getScale(colnum)
            if scale > 0:
                val =  rset.getDouble(colnum)
            else:
                val =  rset.getLong(colnum)
        else:
            # this includes varchar,varchar2,nchar,nchar2
            # dates, etc
            val = rset.getString(colnum)
        return val

    def _extract_results(self, rset, lists=False, strings=False):
        meta = rset.getMetaData()
        ncol = meta.getColumnCount()

        res=[]
        while rset.next():
            if lists:
                this=[]
                for colnum in xrange(1,ncol+1):
                    this.append( self._extract_rset_value(rset,meta,colnum,strings=strings) )
            else:
                this={}
                for colnum in xrange(1,ncol+1):
                    name = meta.getColumnName(colnum).lower()
                    this[name] = self._extract_rset_value(rset,meta,colnum,strings=strings)
            res.append(this)
        return res

    def execute(self, query, lists=False, strings=False, show=False):
        """
        Execute the query and return the result.

        By default returns a list of dicts.

        parameters
        ----------
        query: string
            A query to execute
        lists: bool, optional
            Return a list of lists instead of a list of dicts.
        strings: bool, optional
            Convert all values to strings
        show: bool, optional
            If True, print the query to stderr
        """
        stmt = self.conn.createStatement()
        if show: 
            stderr.write(query)

        rset = stmt.executeQuery(query)
        res = self._extract_results(rset, lists=lists, strings=strings)

        stmt.close()
        return res

    def executeWrite(self, query, type='csv', header=True, file=sys.stdout, show=False):
        """
        Execute the query and print the results.

        parameters
        ----------
        query: string
            A query to execute
        type: string, optional
            The format for writing.  Default 'csv'
        header: bool, optional
            Put a header on csv outputs.
        file: file object, optional
            Write the results to the file. Default is stdout
        show: bool, optional
            If True, print the query to stderr
        """

        stmt = self.conn.createStatement()
        if show: 
            stderr.write(query)

        rset = stmt.executeQuery(query)

        rw=ResultWriter(rset)
        rw.write(type=type, header=header, file=file)
        stmt.close()

def write_result(rset, type='csv', header=True, file=sys.stdout):
    rw=ResultWriter(rset)
    rw.write(type=type, file=file, header=header)

class ResultWriter:
    """
    You cannot inherit from an oracle result set for some reason.
    So this encapsulates and tries to implement as many things
    as possible.
    """

    def __init__(self, rset):
        self.rset=rset

    def __repr__(self):
        meta = self.rset.getMetaData()
        ncol = meta.getColumnCount()

        rep=['ResultSet']

        cspacing = ' '*4
        nspace = 4
        nname = 15
        ntype = 15
        format = "%s%-" + str(nname) + "s %" + str(ntype) + "s"
        pformat = "%s%-" + str(nname) + "s\n %" + str(nspace+nname+ntype) + "s"

        indent=' '*4
        for c in xrange(ncol):
            name = meta.getColumnLabel(c+1).lower()
            dtype = meta.getColumnTypeName(c+1).lower()

            if len(name) > nname:
                f = pformat
            else:
                f = format

            crep = f % (indent,name,dtype)
            rep.append(crep)

        return '\n'.join(rep)

    def write(self, type='csv', header=True, file=sys.stdout):
        """
        Print out a result set.  Need to figure out how
        to use a custom result set that can write itself
        """


        if type == 'csv':
            self.write_csv(header=header, file=file)
        else:
            raise ValueError("only support csv writing for now")

    def write_csv(self, header=True, file=sys.stdout):
        """
        informed by trivialAccess.py
        """

        meta = self.rset.getMetaData()

        ncol = meta.getColumnCount()
        if 0 == ncol:
            # no results,  exit, no header
            return

        hdr=[]
        for c in xrange(ncol): 
            name = meta.getColumnLabel(c+1).lower()
            hdr.append(name)

        writer = csv.writer(sys.stdout,dialect='excel',
                            quoting=csv.QUOTE_MINIMAL)

        if header : 
            writer.writerow(hdr)

        nresults = 0
        while (self.rset.next()):
            rowdata = []
            for c in xrange(ncol): 
                rowdata.append(self.rset.getString(c+1))
            writer.writerow(rowdata)
            nresults += 1
        return nresults


