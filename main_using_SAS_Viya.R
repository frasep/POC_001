# ######################################################################################################
library("dplyr")
library("glue")
library("stringr")
library("rlist")
library("data.table")
library("readr")
library("stringi")
library("swat")

source("C:\\my_local_data\\github\\POC_001\\Utils_using_SAS_Viya.R")

# ######################################################################################################
# Connection a l'environnement SAS Viya
cashost <- 'viya35.local.fr'
casuser <- 'sasdemo'
caspwd <- 'Lprzwb31CA'
# cashost <- '35.241.219.145'
# casuser <- 'sebastien'
# caspwd <- 'Sebastien2020'

conn <- swat::CAS(cashost, 5570, username=casuser, password=caspwd)
out <- cas.sessionProp.setSessOpt(conn, metrics=TRUE)

# ######################################################################################################
# Defini une librairie CAS pointant sur le repertoire contenant tous les fichiers CSV en entree
# On definie la source en DNFS, cela signifie que la lecture et ecriture des fichiers se fera en
# multitheading automatiquement

cas.table.addCaslib(conn,name="mycaslib", path="/data/data/BDF_SMALL_DB", dataSource={srcType="dnfs"})
import_all_csv_in_memory(conn, 'mycaslib','casuser')

#cas.table.tableInfo(conn,caslib='casuser')$TableInfo[,c('Name','Rows')]

cas_concat_all_tables(conn, "casuser","casuser","CONCAT_TAB")

concat_tab <- defCasTable(conn, tablename='CONCAT_TAB', caslib='casuser')
head(concat_tab)
summary(concat_tab)
#concat_tab$code

table_pays_zone <- defCasTable(conn, tablename='TABLEPAYSZONE', caslib='casuser')
head(table_pays_zone)


sauvegarder_cas_table(conn, 'casuser', 'CONCAT_TAB', 'casuser','concat_tab.parquet')

# Exemples d'utilisation des fonctions de base R profitant du moteur in-memory CAS

cas.table.loadTable(conn, casout=list(caslib='casuser',name='CONCAT_TAB_PARQUET'), caslib='casuser', path='concat_tab.parquet')
concat_tab_parquet <- defCasTable(conn, tablename='concat_tab_parquet', caslib = 'casuser')

cas.table.loadTable(conn, casout=list(caslib='casuser',name='CONCAT_TAB_PARQUET'), caslib='casuser', path='concat_tab.parquet')

summary(concat_tab_parquet)
nrow(concat_tab_parquet)
names(concat_tab_parquet)

# Transfert CAS table and convert it to R data frame

concat_tab_dt <- to.casDataFrame(concat_tab_parquet,obs=10000000)

dim(concat_tab_dt)

# ######################################################################################################

traitement_principal <- function(freq, RevFin, PeriodeFin, planAggregation,ReferencePiZones, Repertoire, ConnectionSecureDB)
{
  Frequence <- freq
  HeureDebut <- Sys.time()
  An = as.numeric(ExtractString(PeriodeFin, 1, 4))
  print("ETAPE DATA: LECTURE TABLE PI_ZONE")
  print(Sys.time())
  TablePiZone <- recuperation_traitement_table_code_pays(ReferencePiZones, ConnectionSecureDB)
  return(TablePiZone)
  # print(" TAILLE TABLE TablePaysZone")
  # print(dim(TablePiZone))
  # print(Sys.time())
  # print("LECTURE ET MISE EN FORME DES FICHIERS D'AGGREGATION CODE SERIE")
  # print(Sys.time())
  # planAggregation <- "PlanAggregation.csv"
  # planAgregationCodeSerie <- lecture_fichier_aggregation_codeSerie(planAggregation, connectionSecureDB)
  # # write_delim(planAgregationCodeSerie, "planAgregationCodeSerie.csv", delim=";")
  # print(" TAILLE TABLE planAgregationCodeSerie")
  # print(dim(planAgregationCodeSerie))
  # print(Sys.time())
  # print("JOINTURE PLAN D'AGGREGATION")
  # PlanAggregationParametre <- parametrage_aggregation_code_serie(planAgregationCodeSerie, TablePiZone, freq)
  # # write_delim(PlanAggregationParametre, "PlanAggregationParametre.csv", delim=";")
  # print("Taille plan d'agregation")
  # print(dim(PlanAggregationParametre))
  # print("ESTIMATION NOMBRE DE PERIODE ET DE DEBUT DE TRAITEMENT")
  # result_nbperiode_moisdebut <- detection_nbPeriode_moisDebut(freq, RevFin, PeriodeFin)
  # NbrePeriode <- result_nbperiode_moisdebut$nbper
  # MoisDebut <- as.numeric(result_nbperiode_moisdebut$moisDebut)
  # rm(result_nbperiode_moisdebut)
  
  # print("BOUCLE DU LES MOIS")
  # 
  # for(mois in MoisDebut:(MoisDebut+NbrePeriode-1))
  # {
  #   print(paste0("     Mois debut ", MoisDebut))
  #   print(paste0("     Mois fin ", (MoisDebut+NbrePeriode-1)))
  #   print(paste0("           Mois en cours de traitement: ", mois))
  #   
  #   print(Sys.time())
  #   
  #   print("                      lecture")
  #   AggregationMensuelleTouteCategorie <- lecture_tables_aggregation(freq, Mois, RevFin, PeriodeFin, Repertoire, ConnectionSecureDB)
  #   # write_delim(AggregationMensuelleTouteCategorie,
  #   #            "AggregationMensuelleTouteCategorie.csv", delim=";")
  #   print("Taille table d'agregation")
  #   
  #   print(dim(AggregationMensuelleTouteCategorie))
  #   #AggregationMensuelleTouteCategorie[, code := trimws(code)]
  #   
  #   TableIdMixte <- lecture_tables_mixte(freq, Mois, RevFin, PeriodeFin, Repertoire, ConnectionSecureDB)
  #   
  #   print(Sys.time())
  #   
  #   print("PARAMETRAGE CODE SERIE") 
  #   
  #   print("APPLICATION PARAMETRAGE CODE SERIE") 
  #   
  #   AggregationCodeSerieFinaleMensuelle <- application_plan_parametrage_aggregation_code_serie_mensuel(AggregationMensuelleTouteCategorie, PlanAggregationParametre)
  #   
  #   # Use write_delim to compare it to pandas
  #   # write_delim(AggregationCodeSerieFinaleMensuelle,
  #   #             "AggregationCodeSerieFinaleMensuelle.csv", delim=";")
  #   
  #   print("Taille AggregationCodeSerieFinaleMensuelle")
  #   
  #   print(dim(AggregationCodeSerieFinaleMensuelle))
  #   
  #   print(Sys.time())
  #   
  #   print("SAUVEGARDE DE LA TABLE SORTIE FINALE") 
  #   
  #   TableASauvegarder <-
  #     sauvegarde_donnees_par_categorie_fonctionnelle_et_par_aggregation_code_serie(
  #       AggregationCodeSerieFinaleMensuelle,
  #       AggregationMensuelleTouteCategorie,
  #       TableIdMixte, Freq, Mois, RevFin, An)
  #   
  #   print("FIN DE LA SAUVEGARDE DE LA TABLE SORTIE FINALE")
  #   print(Sys.time())
  #   print("DEBUT DE L'ECRITURE")
  #   
  #   # Use write_delim to compare it to pandas
  #   write_delim(TableASauvegarder, 'TableASauvegarder.csv', delim=";")
  #   
  #   print("Fin ecriture table finale")
  #   print(Sys.time())
  #   
  #   print(summary(TableASauvegarder))
  #   
  #   return(TableASauvegarder)
  #}
}

#### TEST



Repertoire <- "AC156203"
RevFin<- "SD10"
PeriodeFin <- "2018Q4"
freq <- "Q"
planAggregation <- "V4"
ReferencePiZones <- table_pays_zone
ConnectionSecureDB <- ""

HeureDebut <- Sys.time()

TableASauvegarder <-
  traitement_principal(freq, RevFin, PeriodeFin, planAggregation,ReferencePiZones, Repertoire, conn)

Duree <- Sys.time() - HeureDebut

print(Duree)


# Ferme la session SAS Viya en cours
cas.terminate(conn)
