library("dplyr")
library("glue")
library("stringr")
library("rlist")
library("data.table")
library("readr")
library("stringi")
library("swat")

conn <- swat::CAS('viya35.local.fr', 5570, username='sasdemo', password='Lprzwb31CA')
out <- cas.sessionProp.setSessOpt(conn, metrics=TRUE)


import_all_csv_in_memory <- function(inputcaslib)
{
  listfiles=cas.table.fileInfo(caslib=inputcaslib)
  
  for(k_ in 1:length(list_ind)){
    
  } 
  
  do row over listfiles.fileinfo[1:listfiles.fileinfo.nrows];
  if (index(row.Name,'.csv')<>0) and (index(row.Name,'creditcard')=0) then do;
  datafile=row.Name;
  tablename=scan(row.Name,1);
  table.droptable / caslib="public" name=tablename quiet=true;
  table.loadTable / 
    casout={caslib="public" name=tablename promote=true} 
    caslib="myCaslib" 
    path=datafile
    importoptions={delimiter=";" filetype="csv" guessRows=10000 getnames=true varchars=true stripblanks=true};
    end;
    end;
    run;
    end;
  
  
  return(TableASauvegarder)
}


function import_all_csv_files();
table.fileinfo result=listfiles / caslib="myCaslib";

do row over listfiles.fileinfo[1:listfiles.fileinfo.nrows];
if (index(row.Name,'.csv')<>0) and (index(row.Name,'creditcard')=0) then do;
datafile=row.Name;
tablename=scan(row.Name,1);
table.droptable / caslib="public" name=tablename quiet=true;
table.loadTable / 
  casout={caslib="public" name=tablename promote=true} 
  caslib="myCaslib" 
  path=datafile
  importoptions={delimiter=";" filetype="csv" guessRows=10000 getnames=true varchars=true stripblanks=true};
  end;
  end;
  run;
  end;