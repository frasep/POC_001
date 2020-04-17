# ######################################################################################################
# Fonctions utilisant le moteur SAS Viya nativement
# ######################################################################################################
# Fonction permettant d'importer tous les fichiers presents dans la librairie CAS definie plus haut
import_all_csv_in_memory <- function(casconn, inputcaslib, outputcaslib) {
  listfiles=cas.table.fileInfo(casconn,caslib=inputcaslib)
  
  for(i in 1:length(listfiles$FileInfo$Name)){
    file_name <- listfiles$FileInfo$Name[i]
    if ((grepl('.csv',file_name)) & !(grepl('creditcard',file_name))) {
      split <- strsplit(file_name, "[.]")[[1]]
      table_name <- split[1]
      print(table_name)
      cas.table.dropTable(conn, caslib='public', name=table_name, quiet=TRUE);
      cas.table.loadTable(conn, casout=list(caslib=outputcaslib,name=table_name), caslib=inputcaslib, path=file_name, importoptions=list(delimiter=';',filetype='csv',guessRows=10000,getnames=TRUE,varchars=TRUE,stripblanks=TRUE))
    }
  }
}

# ######################################################################################################
# Fonction permettant de concatener deux tables CAS
cas_append2tables <- function(casconn, inputcaslib, inputcastab, outputcaslib, outputcastab) {
  codeds <- paste0('data "', outputcastab, '"(caslib="', outputcaslib , '" append=yes replace=yes); set "',inputcastab,'"(caslib="', inputcaslib, '"); length tablename $ 5; tablename="',inputcastab, '"; run;')
  cas.dataStep.runCode(casconn,code=codeds)
}

# ######################################################################################################
# Fonction permettant de concatener toutes les tables en memoire d'une librairie CAS donnee
cas_concat_all_tables <- function(casconn, inputcaslib,outputcaslib,outcastab) {
  listtab=cas.table.tableInfo(casconn,caslib=inputcaslib)
  cas.table.dropTable(conn, caslib=outputcaslib, name=outcastab, quiet='true');
  
  for(i in 1:length(listtab$TableInfo$Name)){
    tab_name <- listtab$TableInfo$Name[i]
    if (!(grepl('PLANAGREGATION',tab_name)) & !(grepl('TABLEPAYSZONE',tab_name)) & !(grepl('CREDITCARD',tab_name))) {
      cas_append2tables(casconn, inputcaslib, tab_name, outputcaslib, outcastab)
    }
  }
}


# ######################################################################################################
# Fonction permettant de sauvegarder une table CAS dans la datasource associ?e ? la librairie CAS (Filesystem, Hadoop, Oracle,etc...)
sauvegarder_cas_table <- function(casconn, incaslib, incastab, targetcaslib,  nomcible) {
      cas.table.save(casconn, table=list(name=incastab,caslib=incaslib), caslib=targetcaslib, name=nomcible, replace=TRUE)
}

# ######################################################################################################
# ######################################################################################################
# ######################################################################################################
# Fonctions reutilisees du code existant en R
# ######################################################################################################

get_machine_ram_info <- function()
{
  memory_info <- matrix(system("cat /proc/meminfo", intern = TRUE))
  
  return(
    list(unit = "KB",
         total_memory = as.numeric(unlist(strsplit(memory_info[1,], "\\s+", perl = TRUE))[2]),
         free_memory = as.numeric(unlist(strsplit(memory_info[2,], "\\s+", perl = TRUE))[2]),
         available_memory = as.numeric(unlist(strsplit(memory_info[3,], "\\s+", perl = TRUE))[2])
    )
  )
}

# ######################################################################################################
ExtractString <- function(x, start, stop)
{
  return(substr(x, start, start+stop-1))
}

# ######################################################################################################
# PETITEs FONCTIONs UTILES POUR ENLEVER DES ESPACES AU BOUT DES CHAINES
# DE CARACTERE ET UNIFORMISER LES ESPACES
corrige_double_point<- function(caractere)
{
  caractere_corrige <- stringr::str_replace_all(caractere, "\\.\\.", "\\.")
  return(caractere_corrige)
}

# ######################################################################################################
traite_espace_chaine_caractere <- function(DataFrame)
{
  DataFrame <-  dplyr::mutate_if(DataFrame, is.character, function(.){stringr::str_replace_all(., ("\\s+"), " ")})
  DataFrame <- dplyr::mutate_if(DataFrame, is.character, trimws)
  return(DataFrame)
}

# ######################################################################################################
# FONCTION QUI DECOUPE UNE CHAINE DE CARACTERE AVEC SEPARATEUR Delimiteur ET RETOURNE L'ELEMENT DE RANG indexElement

decouper_caractere_recuperer_element <- function(Caractere, Delimiteur, indexElement)
{
  CaractereVecteur <- unlist(stringr::str_split(string = as.character(Caractere), pattern = as.character(Delimiteur)))
  return(CaractereVecteur[indexElement])
}

# ######################################################################################################
# FONCTION QUI DECOUPE UNE CHAINE DE CARACTERE AVEC SEPARATEUR Delimiteur ET RETOURNE LA Longueur de 
# L'ELEMENT DE RANG indexElement AINSI QUE LE DEBUT DE SA POSITION

decouper_caractere_recuperer_longueur_position_element <- function(Caractere, Delimiteur, indexElement)
{
  Caractere_decoupe <- unlist(stringr::str_split(string = Caractere, pattern = Delimiteur))
  ElementIndexElement <- Caractere_decoupe[indexElement]
  Position <- sum(nchar(Caractere_decoupe[1:(indexElement-1)])) + (indexElement-1) + 1
  Longueur <- stringr::str_length(ElementIndexElement)
  return(list("position" = Position, "longueur" = Longueur))
}


recuperation_traitement_table_code_pays <- function(ReferencePiZones, ConnectionSecureDB)
{
  # ReferencePiZones devient une refence a une CASTable deja en memoire et declaree en R dans le programme appelant la fonction
  # ConnectionSecureDB est maintenant une reference a une connexion CAS non utilisee dans notre cas
  print("recuperation_traitement_table_code_pays monostream")
  print(Sys.time())
  TableCodePays <- data.table::data.table(to.data.frame(to.casDataFrame(ReferencePiZones[(ReferencePiZones$code_pays!='') & (ReferencePiZones$code_zone!=''),])))
  setorder(TableCodePays, code_pays)
  TablePays <- TableCodePays[, .(code_pays)]
  TableZone <- TableCodePays[, .(code_zone)]
  PiZone <- data.table::rbindlist(list(TablePays[, pays := code_pays], TableZone[, pays := code_zone]))
  # Modification ncol en nrow
  PiZone$cle <- rep("_Z", nrow(PiZone))
  PiZone[, c("code_pays", "pays", "cle") := list(trimws(code_pays), trimws(pays), trimws(cle))]
  setorder(PiZone, pays, cle)
  print("end recuperation_traitement_table_code_pays monostream")
  print(Sys.time())
  return(unique(PiZone))
}

# ######################################################################################################
lecture_fichier_aggregation_codeSerie <-function(planAggregation, connectionSecureDB)
{
  # planAggregation devient une refence a une CASTable deja en memoire et declaree en R dans le programme appelant la fonction
  # ConnectionSecureDB est maintenant une reference a une connexion CAS
  print("lecture_fichier_aggregation_codeSerie monostream")
  #CheminRepertoirePlanAggregation <- "/home/ardtr/appli/travail/traard1562/traard1562_D/commun/NAB/referentiel_agregation/"
  #CheminDataSet <- paste0("'",CheminRepertoirePlanAggregation, planAggregation, "/", "agregation_code_serie.sas7bdat","'")
  #RequeteSql <- paste0("SELECT * FROM ", CheminDataSet)
  # TableCode <- data.table::data.table(secureDB::getQuery(connectionId = ConnectionSecureDB, query = RequeteSql))
  #TableCode <- data.table::data.table(read.csv2(planAggregation))
  TableCode <- data.table::data.table(to.data.frame(to.casDataFrame(planAggregation)))
  df_list <- list()
  for(i in 1:20)
  {
    df_list <- rlist::list.append(df_list, setNames(TableCode[, .(get("code_sortie"), get(paste0("code_entree_", i)), get(paste0("formule_", i)))],
                                                    c("code_sortie", "code_entree", "formule")))
    
    #if(planAggregation == "V4" && i>= 14) break
    if(i>= 14) break
  }
  
  TableCodeFinal <- data.table::rbindlist(df_list)
  TableCodeFinal <- setDT(TableCodeFinal)
  TableCodeFinal[, code_sortie:= trimws(code_sortie)]
  TableCodeFinal[, code_entree:= trimws(code_entree)]
  TableFinal <- TableCodeFinal[code_entree!= "", ]
  return(TableFinal)
}

# ######################################################################################################
parametrage_aggregation_code_serie <-function(AggregationTable, TablePiZone, Frequence)
{
  print("parametrage_aggregation_code_serie monostream")
  print(Sys.time())
  
  freq <- as.character(Frequence)
  
  AggregationTable <- data.table::data.table(AggregationTable)
  
  list_ind <- c("T", "KA", "K7A", "K7B", "LE", "K", "K7")
  
  AggregationTable <- AggregationTable[ , c("enfants", "parents") := list(code_entree, code_sortie)] 
  
  AggregationTable$cle <- unlist(lapply(AggregationTable$enfants, function(Caractere){decouper_caractere_recuperer_element(Caractere,  Delimiteur = "\\.", indexElement = 4)}))
  
  AggregationTable$ind_ <- unlist(lapply(AggregationTable$enfants, function(Caractere){decouper_caractere_recuperer_element(Caractere,  Delimiteur = "\\.", indexElement = 7)}))
  
  print("Recuperation des positions et longueur parents dans les codes") 
  
  AggregationTable$PositionEnfant <- unlist(lapply(AggregationTable$enfants, 
                                                   function(Caractere)
                                                   {
                                                     resultparam <- decouper_caractere_recuperer_longueur_position_element(Caractere, Delimiteur = "\\.", indexElement = 4)
                                                     return(resultparam$position)
                                                   }
  )
  )
  
  AggregationTable$LongueurEnfant  <- unlist(lapply(AggregationTable$enfants, 
                                                    function(Caractere)
                                                    {
                                                      resultparam <- decouper_caractere_recuperer_longueur_position_element(Caractere, Delimiteur = "\\.", indexElement = 4)
                                                      return(resultparam$longueur)
                                                    }
  )
  )
  
  print("Recuperation des positions et longueur des parametres dans les codes")
  
  AggregationTable$PositionParametre <- unlist(lapply(AggregationTable$parents, 
                                                      function(Caractere)
                                                      {
                                                        resultparam <- decouper_caractere_recuperer_longueur_position_element(Caractere, Delimiteur = "\\.", indexElement = 4)
                                                        return(resultparam$position)
                                                      }
  )
  )
  
  AggregationTable$LongueurParametre <- unlist(lapply(AggregationTable$parents, 
                                                      function(Caractere)
                                                      {
                                                        resultparam <- decouper_caractere_recuperer_longueur_position_element(Caractere, Delimiteur = "\\.", indexElement = 4)
                                                        return(resultparam$longueur)
                                                      }
  )
  )
  
  print(Sys.time())
  
  print("Recuperation des positions et longueur des index dans les codes")
  
  AggregationTable$PositionIndex <- unlist(lapply(AggregationTable$parents, 
                                                  function(Caractere)
                                                  {
                                                    resultparam <- decouper_caractere_recuperer_longueur_position_element(Caractere, Delimiteur = "\\.", indexElement = 7)
                                                    return(resultparam$position)
                                                  }
  )
  )
  
  AggregationTable$LongueurIndex <- unlist(lapply(AggregationTable$parents, 
                                                  function(Caractere)
                                                  {
                                                    resultparam <- decouper_caractere_recuperer_longueur_position_element(Caractere, Delimiteur = "\\.", indexElement = 7)
                                                    return(resultparam$longueur)
                                                  }
  )
  )
  
  print(Sys.time())
  
  print(paste0(as.character(round(x = (get_machine_ram_info()$total_memory-get_machine_ram_info()$free_memory)/(1024*1024))), " GB"))
  
  print("Creation des variables cd2_, cd3_, cd1_, refsec, countsec par decoupage des codes")
  
  AggregationTable$cd2_ <-  unlist(lapply(1:nrow(AggregationTable),function(i){ExtractString(AggregationTable$parents[i],
                                                                                             (AggregationTable$PositionIndex[i]+AggregationTable$LongueurIndex[i]+1),
                                                                                             (nchar(AggregationTable$parents[i])-(AggregationTable$PositionIndex[i]+AggregationTable$LongueurIndex[i]))
  )
  }))
  
  
  AggregationTable$cd3_ <-  unlist(lapply(1:nrow(AggregationTable),function(i){ ExtractString(AggregationTable$enfants[i],
                                                                                              AggregationTable$PositionIndex[i]+AggregationTable$LongueurIndex[i]+1,
                                                                                              (nchar(AggregationTable$enfants[i])-(AggregationTable$PositionIndex[i]+AggregationTable$LongueurIndex[i]))
  )
  }))
  
  AggregationTable$cd1_ <-  unlist(lapply(1:nrow(AggregationTable),function(i){ ExtractString(AggregationTable$parents[i], 3,
                                                                                              (AggregationTable$PositionIndex[i]-3))}))
  
  AggregationTable$refsec <- unlist(lapply(AggregationTable$enfants, function(Caractere){decouper_caractere_recuperer_element(Caractere,  Delimiteur = "\\.", indexElement = 5)}))
  AggregationTable$countsec <- unlist(lapply(AggregationTable$enfants, function(Caractere){decouper_caractere_recuperer_element(Caractere,  Delimiteur = "\\.", indexElement = 6)}))
  
  print(Sys.time())
  
  print("Modification conditionnelle des valeurs de code entree et code sortie")
  
  AggregationTable$selectionLigne1 <- ((AggregationTable$cle != '_Z') & (AggregationTable$ind_ == 'T'))
  AggregationTable$selectionLigne3 <- ((AggregationTable$cle != '_Z') & (AggregationTable$ind_ == '_Z')) # Temporaire
  
  AggregationTable <- data.table::data.table(AggregationTable)
  
  AggregationTable[selectionLigne1 == TRUE, code_entree := paste0(freq,".", ExtractString(enfants, 3, (nchar(enfants)-2)))]
  AggregationTable[selectionLigne1 == TRUE, code_sortie := paste0(freq,".",ExtractString(parents, 3, (nchar(parents)-2)))]
  
  Liste_Table_Agreg <- list()
  
  for(k_ in 1:length(list_ind)){
    AggregationTable[selectionLigne3 == TRUE, code_entree := paste0(freq, cd1_, list_ind[k_], ".",cd3_)]
    AggregationTable[selectionLigne3 == TRUE, code_sortie := paste0(freq, cd1_, list_ind[k_], ".",cd2_)]
    
    Liste_Table_Agreg <- rlist::list.append(Liste_Table_Agreg, AggregationTable[selectionLigne3 == TRUE]) # AggregationTable[selectionLigne2 == TRUE]
  } 
  
  AggregationTable1 <- unique(data.table::rbindlist(
    list(data.table::rbindlist(Liste_Table_Agreg),
         AggregationTable)
  ))
  
  AggregationTable2 <- unique(data.table::rbindlist(
    list(data.table::rbindlist(Liste_Table_Agreg),
         AggregationTable[selectionLigne1 == TRUE])
  ))
  
  print(Sys.time())
  print("Jointure de la table AggregationTable TablePiZone")
  
  TablePiZone <- data.table::data.table(TablePiZone)
  
  setkey(TablePiZone, cle)
  setkey(AggregationTable1, cle)
  
  print(dim(AggregationTable1))
  print(names(AggregationTable1))
  
  AggregationTable4 <- merge(AggregationTable1, TablePiZone, all.x = TRUE, allow.cartesian=TRUE)
  
  AggregationTable4 <- AggregationTable4[cle=="_Z",]
  
  print("Modification conditionelle (lorsque la cle jointure n'existe pas) de code entree et code sortie")
  try({
    AggregationTable4$selectionLigne4 <- (AggregationTable4$ind_ == 'T')
    
    AggregationTable4[selectionLigne4 == TRUE, code_entree :=  paste0(freq, ExtractString(enfants, 3, (PositionEnfant-3)),pays, 
                                                                      ExtractString(enfants, (PositionEnfant+LongueurEnfant),
                                                                                    (nchar(enfants)-PositionEnfant-LongueurEnfant+1))
    )
    ]
    
    AggregationTable4[selectionLigne4 == TRUE, code_sortie := paste0(freq, ExtractString(parents, 3, (PositionParametre-3)),pays, 
                                                                     ExtractString(parents, (PositionParametre+LongueurParametre),
                                                                                   (nchar(parents)-PositionParametre-LongueurParametre+1))
    )
    ]
    
    AggregationTable4$selectionLigne5 <- (AggregationTable4$ind_ == '_Z')
    
    Liste_Table_Agreg <- list()
    
    for(k_ in 1:length(list_ind)){
      AggregationTable4[selectionLigne5 == TRUE, code_entree := paste0(freq,".N.FR.", pays, ".", refsec, ".", countsec,".", list_ind[k_], ".", cd3_)]
      AggregationTable4[selectionLigne5 == TRUE, code_sortie := paste0(freq,".N.FR.", pays, ".", refsec, ".", countsec, ".", list_ind[k_], ".",cd2_)]
      
      Liste_Table_Agreg <- rlist::list.append(Liste_Table_Agreg, AggregationTable4[selectionLigne5 == TRUE])
    } 
    
    AggregationTable5 <- unique(data.table::rbindlist(
      list(data.table::rbindlist(Liste_Table_Agreg),
           AggregationTable4)
    ))
  })  
  
  AggregationTable <- unique(data.table::rbindlist(list(AggregationTable4[, .(code_entree, code_sortie,formule)],
                                                        AggregationTable5[, .(code_entree, code_sortie,formule)],
                                                        AggregationTable2[, .(code_entree, code_sortie,formule)]))
  )
  
  AggregationTable[ , code_entree := corrige_double_point(code_entree)]
  AggregationTable[ , code_sortie := corrige_double_point(code_sortie)]
  
  AggregationTable <- unique(AggregationTable)
  
  print(Sys.time())
  
  print("TAILLE TABLE PLAN AGREGATION")
  print(dim(AggregationTable[, .(code_entree, code_sortie, formule)]))
  
  print("end parametrage_aggregation_code_serie monostream")
  print(Sys.time())
  
  return(AggregationTable[, .(code_entree, code_sortie, formule)])
}

# ######################################################################################################
detection_nbPeriode_moisDebut<- function(freq, RevFin, PeriodeFin)
{
  print("detection_nbPeriode_moisDebut monostream")
  nbper <- NULL
  Annee <- as.numeric(ExtractString(as.character(PeriodeFin), start = 1, stop = 4))
  if(freq == "M")
  {
    if( RevFin == "KI")
    {
      nbper <- 1
    }
    else if(ExtractString(as.character(PeriodeFin), start = 1, stop = 3) == "SD1")
    {
      nbper <- 3
    }
    else{
      nbper <- 12 
      moisDebut <- PeriodeFin
    }
  }
  
  if(freq == "Q")
  {
    nbper <- 1
    mm <- as.numeric(ExtractString(as.character(PeriodeFin), start = 6, stop = 1))
    if(mm != 4)
    { 
      mm <-  mm*3
      moisDebut <- paste0(Annee, "0", mm)
    }
    else{
      moisDebut <- paste0(Annee, "12")
    }
  }
  
  if(freq == "A")
  {
    nbper <- 1
    moisDebut <- PeriodeFin
  }
  return(list("nbper" = nbper, "moisDebut" = moisDebut))
}

# ######################################################################################################
lecture_tables_aggregation <-function(Freq, Mois, RevFin, PeriodeFin, Repertoire, ConnectionSecureDB, TableAggTteCategorie, tableAggMensuelOut)
{
  print("lecture_tables_aggregation monostream")
  print(Sys.time())
  
  print(paste0(as.character(round(x = (get_machine_ram_info()$total_memory-get_machine_ram_info()$free_memory)/(1024*1024))), " GB"))
  
  an <- as.numeric(ExtractString(PeriodeFin, 1, 4))
  
  #DebutNomDataset <- c(
  #  "Agregation_D_",
  #  "Agregation_D1_",
  #  "Agregation_D2_",
  #  "Agregation_D3_",
  #  "Agregation_F_",
  #  "Agregation_O_",
  #  "Agregation_P_",
  #  "Agregation_U1_",
  #  "Agregation_U2_",
  #  "Agregation_U3_",
  #  "Agregation__Z_"
  #)
  
  CheminDataset <- paste0(Repertoire, ".", DebutNomDataset, RevFin, "_", PeriodeFin)
  
  ConditionSpeciale <- ((an <= 2011) | (an == 2014 & (RevFin %in% c("SD10", "SD11"))) | (an == 2013 & (RevFin %in% c("SD10", "SD11", "SD20"))))
  
  if(RevFin == "KI")
  {
    if(Freq =="M")
    {
      if(ConditionSpeciale)
      {
        #case 1
        #CheminData <- CheminDataset[-c(10,11,12)]
        #RequetesSQL <- paste0("SELECT * FROM ",CheminData, " WHERE PeriodeFin =", "'", Mois, "'", " AND scan(code, 15) <> '_X'")
        RequetesSQL <- paste0("SELECT * FROM ",TableAggTteCategorie, " WHERE PeriodeFin =", "'", Mois, "'", " AND scan(code, 15) <> '_X' AND tablename in ('U3','Z')")
      }
      else
      {
        #case 2
        #CheminData <- CheminDataset[-c(10,11,12)]
        RequetesSQL <- paste0("SELECT * FROM ",TableAggTteCategorie, " WHERE PeriodeFin =", Mois,"  AND tablename in ('U3','Z')")
      }
    }
    else
    {
      if(ConditionSpeciale)
      {
        #case 3
        #CheminData <- CheminDataset[-c(10,11,12)]
        RequetesSQL <- paste0("SELECT * FROM ",TableAggTteCategorie, " WHERE scan(code, 15) <> '_X' AND tablename in ('U3','Z')")
      }
      else
      {
        #case 4
        #CheminData <- CheminDataset[-c(10,11,12)]
        RequetesSQL <- paste0("SELECT * FROM ", TableAggTteCategorie," WHERE tablename in ('U3','Z')")
      }
    }
  }
  else
  {
    if(Freq =="M")
    {
      if(ConditionSpeciale)
      {
        #case 5
        #CheminData <- CheminDataset
        RequetesSQL <- paste0("SELECT * FROM ",TableAggTteCategorie, " WHERE PeriodeFin =", "'", Mois, "'", " AND scan(code, 15) <> '_X'")
      }
      else
      {
        #case 6
        #CheminData <- CheminDataset
        RequetesSQL <- paste0("SELECT * FROM ",TableAggTteCategorie, " WHERE PeriodeFin =","'", Mois, "'")
      }
    }
    else
    {
      if(ConditionSpeciale)
      {
        #case 7
        #CheminData <- CheminDataset
        RequetesSQL <- paste0("SELECT * FROM ",TableAggTteCategorie, " WHERE scan(code, 15) <> '_X'")
      }
      else
      {
        #case 8
        #CheminData <- CheminDataset
        RequetesSQL <- paste0("SELECT * FROM ", TableAggTteCategorie)
      }
    }
  }
  
    
  print("CHARGEMENT DES TABLES D'AGGREGATION, monostream")
  print(Sys.time())
  
  #Liste_Table <- list()
  #ind <- 1
  #xx <- c("/home/dbigdata/appli/users/i250421/balance_paiements_sparklyr/tables_aggregation_entieres")
  
  #RequetesSQL <- paste(xx[ind], DebutNomDataset, sep = "/")
  
  print(paste("TABLES AGREGATION LU DE LA TABLE CAS", TableAggTteCategorie))
  
  #for(j in 1:length(RequetesSQL))
  #{
  #  print(Sys.time())
  #  print(paste0("       CHARGEMENT TABLE ", DebutNomDataset[j]))
    
  # Liste_Table <- rlist::list.append(Liste_Table, try(secureDB::getQuery(ConnectionSecureDB, RequetesSQL[j])))
  #  name_table <- paste0(RequetesSQL[j], ".csv")
  #  print(paste("Reading", name_table))
  #  Liste_Table <- rlist::list.append(Liste_Table, read_csv2(name_table, guess_max = 12000000))
  #}
  
  #AggregationTouteCategorie <- data.table::rbindlist(Liste_Table)
  
  #AggregationTouteCategorie[, code:= trimws(code)]
  
  # Remove it when testing dplyr
  #AggregationTouteCategorie = AggregationTouteCategorie[, montant:= as.numeric(montant)]
  #AggregationTouteCategorie = AggregationTouteCategorie[, CONF_STATUS:= as.character(CONF_STATUS)]

  RequeteSqlFinaleCAS <- paste0("create table ",tableAggMensuelOut, " {options replication=0 replace=true} as ",RequetesSQL,";")
  cas.fedSql.execDirect(ConnectionSecureDB,query=RequeteSqlFinaleCAS)
  
  print("end lecture_tables_aggregation monostream")
  print(Sys.time())

  AggregationTouteCategorie <- defCasTable(conn, tablename=tableAggMensuelOut, caslib='casuser')
  return(AggregationTouteCategorie)
}

# ######################################################################################################
lecture_tables_mixte <-function(Freq, Mois, RevFin, PeriodeFin, Repertoire, ConnectionSecureDB,TableAggTteCategorie, tableMixteOut)
{
  print("lecture_tables_mixte monostream")
  print(Sys.time())
  
  print(paste0(as.character(round(x = (get_machine_ram_info()$total_memory-get_machine_ram_info()$free_memory)/(1024*1024))), " GB"))
  
  an <- as.numeric(ExtractString(PeriodeFin, 1, 4))
  
  ConditionSpecialeMixte <- ((an <= 2011) | (an == 2014 & (RevFin %in% c("SD10", "SD11"))) | (an == 2013 & (RevFin %in% c("SD10", "SD11", "SD20"))))
  
  #DebutNomMixte <- c(
  #  "Agregation_D2_",
  #  "Agregation_U3_"
  #)
  
  #CheminDatasetMixte <- paste0(Repertoire, ".", DebutNomMixte , RevFin, "_", PeriodeFin)
  
  list_col <- names(TableAggTteCategorie)
  traite_espace_list_col <- paste("COMPBL(",list_col,"), ",collapse='')
  traite_espace_list_col <- substr(traite_espace_list_col,1,nchar(l)-2)

  TableIdMixte <- NULL
  
  if(ConditionSpecialeMixte)
  {
    if(Freq == "M")
    {
      RequetesSQLIdMixte <- paste0("SELECT ",traite_espace_list_col," FROM ", TableAggTteCategorie, " WHERE PeriodeFin =", "'", Mois, "'", " AND scan(code,15,'.') = _X' AND scan(code,11,'.') = 'F5A' AND tablename in ('D2','U3')")
    }
    else
    {
      RequetesSQLIdMixte <- paste0("SELECT ",traite_espace_list_col," FROM ", TableAggTteCategorie, " WHERE scan(code,15,'.') = _X' AND scan(code,11,'.') = 'F5A' AND tablename in ('D2','U3')")
      
    }  
    
    #Liste_TableIdMixte <- list()
    
    #secureDB::getQuery(ConnectionSecureDB, RequetesSQLIdMixte[1])
    #print(paste0("       CHARGEMENT TABLE MIXTE ", DebutNomMixte[1]))
    #for(j in 1:length(RequetesSQLIdMixte))
    #{
    #  print(paste0("       CHARGEMENT TABLE MIXTE ", DebutNomMixte[j]))
    #  
    #  Liste_TableIdMixte <- rlist::list.append(Liste_TableIdMixte, secureDB::getQuery(ConnectionSecureDB, RequetesSQLIdMixte[j]))
    #}
    
    RequeteSqlFinaleCAS <- paste0("create table ",tableMixteOut, " {options replication=0 replace=true} as ",RequetesSQLIdMixte,";")
    cas.fedSql.execDirect(ConnectionSecureDB,query=RequeteSqlFinaleCAS)
      
    #TableIdMixte <- traite_espace_chaine_caractere(data.table::rbindlist(Liste_TableIdMixte))
    TableIdMixte <- defCasTable(conn, tablename=tableMixteOut, caslib='casuser')
  }
  
  print("end lecture_tables_mixte monostream")
  print(Sys.time())
  return(TableIdMixte)
}

# ######################################################################################################
application_plan_parametrage_aggregation_code_serie_mensuel <-function(AggregationMensuelleTouteCategorie, PlanAggregationParametre, CASConnexion, appPlanAggOutCAS)
{
  print("application_plan_parametrage_aggregation_code_serie_mensuel monostream")
  print(Sys.time())
  
  #  nécessaire pour la comparaison des montants à la fin du code
  #AggregationMensuelleTouteCategorie = AggregationMensuelleTouteCategorie[, montant:=as.double(montant)]
  #PlanAggregationParametre[ , code_entree := corrige_double_point(code_entree)]
  #PlanAggregationParametre[ , code_sortie := corrige_double_point(code_sortie)]
  #AggregationMensuelleTouteCategorie <- data.table::data.table(AggregationMensuelleTouteCategorie)
  
  print(Sys.time())
  print("Renomage de variables et traitement d'espace dans les chaines de caratere")
  
  #AggregationMensuelleTouteCategorie[, code_entree := trimws(code)]
  
  print(Sys.time())
  print("Preparation de la jointure des tables PlanAggregationParametre, AggregationMensuelleTouteCategorie")
  
  #data.table::setkey(AggregationMensuelleTouteCategorie, code_entree)
  #data.table::setkey(PlanAggregationParametre, code_entree)

  # Creation index des tables CAS
  #cas.table.index(CASConnexion, casOut=list(name=AggregationMensuelleTouteCategorie, indexVars=c('code_entree'), replace=TRUE))
  #cas.table.index(CASConnexion, casOut=list(name=PlanAggregationParametre, indexVars=c('code_entree'), replace=TRUE))
    
  print(Sys.time())
  print("Jointure des tables PlanAggregationParametre, AggregationMensuelleTouteCategorie")
  #CalculAggregationCodeSerieMensuel <- merge(AggregationMensuelleTouteCategorie, PlanAggregationParametre, all.x=TRUE, allow.cartesian=TRUE)
    
  RequeteSQL <- paste0("select A.*,(A.montant*B.formule) as montantcuml from ", AggregationMensuelleTouteCategorie," as A left outer join ",PlanAggregationParametre," as B on (A.code=B.code_entree)");
  RequeteSqlFinaleCAS <- paste0("create table TMP_AGG {options replication=0 replace=true} as ",RequetesSQL,";")
    cas.fedSql.execDirect(ConnectionSecureDB,query=RequeteSqlFinaleCAS)

  print("Taille de la jointure")
  #print(dim(CalculAggregationCodeSerieMensuel))
  print(Sys.time())
  print("Calcul des montants ponderes cumules par code_sortie")
  
  # Aggregation de la table par code

  RequeteSQL <- paste0("
        select code as code_entree, OBS_STATUS,CONF_STATUS, Periode_deb, revision_deb, Periode_fin, revision_fin, sum(montantcuml) as montant 
        from TMP_AGG 
        group by code,OBS_STATUS,CONF_STATUS, Periode_deb, revision_deb, Periode_fin, revision_fin");
  RequeteSqlFinaleCAS <- paste0("create table ",appPlanAggOutCAS, " {options replication=0 replace=true} as ",RequetesSQL,";")
  
  cas.fedSql.execDirect(ConnectionSecureDB,query=RequeteSqlFinaleCAS)

  #CalculAggregationCodeSerieMensuel[, montantcuml := sum(montant*formule), by = code_sortie]
  
  ResultSortie <- CalculAggregationCodeSerieMensuel[!is.na(montantcuml), ]
  print(" TAILLE TABLE CalculAggregationCodeSerieMensuel")
  print(dim(unique(CalculAggregationCodeSerieMensuel)))
  print(Sys.time())
  ResultSortie$montant <- ResultSortie$montantcuml
  ResultFinal <- ResultSortie[, .(code_entree, code_sortie, montant, OBS_STATUS, CONF_STATUS,
                                  Periode_deb, revision_deb, Periode_fin, revision_fin)]
  
  Agregation_Code_Serie_Final <- unique(ResultFinal[,.(code_sortie, montant)])
  print(" TAILLE TABLE Agregation_Code_Serie_Final")
  print("end application_plan_parametrage_aggregation_code_serie_mensuel monostream")
  print(Sys.time())  
  print(dim(Agregation_Code_Serie_Final))
  
  return(unique(ResultSortie))
}
