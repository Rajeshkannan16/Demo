import sys
import glob
import pandas as pd
import datetime

frame = pd.DataFrame()
list_ = []
date_time=datetime.datetime.today()
date = datetime.datetime.strftime(date_time, "%Y%m%d%I%M")

def printFileName(filename):
    args = sys.argv[1:]
    temp_file=args[0]
    args1=sys.argv[2:]
    print "args1", args1
    print "date", date
    with open(temp_file,'r') as template_file:
        data2 = pd.read_csv(template_file, nrows=1, header=0, sep=";")
        template = list(data2.head())
        print "template" ,template
    for filename in args1:
        with open(filename,'r') as afile:
#           read_data = afile.read()
            df=pd.read_csv(afile,index_col=None, header=0,sep=",")
            list_.append(df)
            frame=pd.concat(list_)
            data=list(frame.head())
            #print "data" ,data
         #data2 = pd.read_csv(template_file, nrows=1, header=0, sep=";")
            missing_cols=list(set(template)-set(data))
#            print missing_cols
            print "Missing cols in %s for %s is %s" % (filename,date,missing_cols)

if __name__ == '__main__':
  printFileName(sys.argv[1:])

