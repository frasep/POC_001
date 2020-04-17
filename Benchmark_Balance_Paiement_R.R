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

# Defini une librairie CAS pointant sur le repertoire contenant tous les fichiers CSV en entree
# On definie la source en DNFS, cela signifie que la lecture et ecriture des fichiers se fera en
# multitheading automatiquement
cas.table.addCaslib(conn,name="mycaslib", path="/data/data/BDF_SMALL_DB", dataSource={srcType="dnfs"})

# Fonction permettant d'importer tous les fichiers presents dans la librairie CAS definie plus haut
import_all_csv_in_memory <- function(casconn, inputcaslib) {
  listfiles=cas.table.fileInfo(casconn,caslib=inputcaslib)
  
  for(i in 1:length(listfiles$FileInfo$Name)){
    file_name <- listfiles$FileInfo$Name[i]
    if ((grepl('.csv',file_name)) & !(grepl('creditcard',file_name))) {
      split <- strsplit(file_name, ".")[[1]]
      table_name <- split[1]
      cas.table.dropTable(conn, caslib='public', name=table_name, quiet='true');
      cas.table.loadTable(conn, casout=list(caslib='public',name=table_name,promote='true'), caslib=inputcaslib, path=file_name, importoptions=list(delimiter=';',filetype='csv',guessRows=10000,getnames='true',varchars='true',stripblanks='true'))
    }
  }
}

# Programme principal

import_all_csv_in_memory(conn, "mycaslib")
  