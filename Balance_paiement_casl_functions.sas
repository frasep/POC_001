/* Lecture_table_agregation
/************************************************************************************************************************************/
/* Scan the input directory for .csv files, import them and strip blanks (begin and end) inside all string columns at the same time */
/* Import all tables including reference tables                                                                                     */

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

/************************************************************************************************************************************/
/* Prepare pizone table                                                                                                             */
/* ETAPE DATA: LECTURE TABLE PI_ZONE                                                                                                */

function prepare_pizone();
	fedsql.execdirect / query="
	create table casuser.pizones {options replication=0 replace=true} as 
	select distinct code_pays as pays from public.tablepayszone where code_pays is not null and trim(code_pays)<>''
	union
	select distinct code_zone as pays from public.tablepayszone where code_zone is not null and trim(code_zone)<>''
	union all
	select '_Z' as pays;";
	table.droptable / name="pizones" caslib="public" quiet=true;
	table.promote / sourcecaslib="casuser" name="pizones" target="pizones" targetcaslib="public";
end;

/************************************************************************************************************************************/
/* Prepare planaggregation table                                                                                                    */
/* Transposition code_entree et code_sortie                                                                                         */

function prepare_planagreggation(_inputCaslib, _inputTable,_outputCaslib, _outputTable);
	sql_transposition="create table casuser." || _outputTable || " {options replication=0 replace=true} as ";
	do i=1 to 19 by 1;
		sql_transposition=sql_transposition ||
		"select trim(code_entree_" || i || ") as code_entree, trim(code_sortie) as code_sortie, cast(formule_" || i || " as double) as formule from " || _inputCaslib || "." || _inputTable || " union ";
	end;
	sql_transposition=sql_transposition || "select trim(code_entree_20) as code_entree, trim(code_sortie) as code_sortie, cast(formule_20 as double) as formule from " || _inputCaslib || "." || _inputTable || ";";

	print sql_transposition;

	fedsql.execdirect / query=sql_transposition;
	
	table.droptable / name=_outputTable caslib=_outputCaslib quiet=true;
	table.promote / sourcecaslib="casuser" name=_outputTable target=_outputTable targetcaslib=_outputCaslib;
	table.index /  table={name=_outputTable caslib=_outputCaslib} casOut={name="codeEntreeInd" indexVars={"code_entree"} replace=True}; 
end;

/************************************************************************************************************************************/
/* Combine pizone and planaggregation_t for futur join                                                                              */

function parametrage_aggregation_code_serie(usedcaslib, outputcastab, pizones, planagregation_t);
	codeds=
	"data """ || outputcastab || """(replace=yes); set """ || planagregation_t || """(caslib=""" || usedcaslib || """ where=(code_entree <> '')); 
	enfants=code_entree;
	parents=code_sortie;
	cle = scan(enfants,4,'.');
	ind_ = scan(enfants,7,'.');
	refsec = scan(enfants,5,'.');
	countsec = scan(enfants,6,'.');
	keep enfants parents cle ind_ refsec countsec;
	run;";
	
	print codeds;
	dataStep.runCode / code=codeds;
end;



/*
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

*/

/************************************************************************************************************************************/
/* Get all information on imported tables and print it as output                                                                    */
function get_info_on_all_imported_files();
	table.tableinfo result=list_tab / caslib="public";
	print list_tab;
	do row over list_tab.tableinfo[1:list_tab.tableinfo.nrows];
		table.columninfo / table={caslib="public" name=row.Name} ;
	end;
end;

/************************************************************************************************************************************/
/* Concatenate two tables, cleanse code and create cle                                                                              */

function appendTable(inputcaslib, inputcastab, outputcaslib, outputcastab);
	codeds=
	"data """ || outputcastab || """(caslib=""" || outputcaslib || """ append=yes); set """ || inputcastab || """(caslib=""" || inputcaslib || """); 
	length cle $ 3;
	cle = scan(code,4,'.');
	keep code cle CONF_STATUS Periode_fin montant
	run;";
	
	print codeds;
	dataStep.runCode / code=codeds;
end;

/************************************************************************************************************************************/
/* Liste toutes les tables d'agregat et les concatene toutes en une seule en memoire contenat les champs nettoyes  */

function append_all_tables();
	table.tableinfo result=listtables / caslib="public";
	table.droptable / caslib="casuser" name="global_agg" quiet=true;
	do row over listtables.tableinfo[1:listtables.tableinfo.nrows];
		if !(upcase(row.Name) in {'PLANAGREGATION','TABLEPAYSZONE','PIZONES'}) then do;
			appendTable("public",row.name,"casuser","global_agg");
		end;
	end;
end;

function detection_nbperiode_moisDebut(freq, RevFin, PeriodeFin);
	nbper = NULL;
	Annee = (double)(substr((String)PeriodeFin, 1, 4));
	if (freq == "M") then 
	do;
		if( RevFin == "KI") then nbper = 1;
	    else if(substr((String)PeriodeFin,1,3) == "SD1") then nbper=3;
	    else do;
	      nbper=12;
	      moisDebut=PeriodeFin;
	    end;
	end;
	
	if (freq == "Q") then
	do;
	  nbper=1;
	  mm = (double)(substr((String)PeriodeFin, 6, 1));
	  if (mm != 4) then
	  do; 
	      mm = mm*3;
	      moisDebut = Annee || "0" || mm;
	  end;
	  else  moisDebut = Annee || "12";
	end;
	if (freq == "A") then	
	do;
	    nbper = 1;
	    moisDebut = PeriodeFin;
	end;
	return({"nbper" = nbper, "moisDebut" = moisDebut});
end;

/************************************************************************************************************************************/
/* Aggregate all data grouped by all colums except montant                                                                          */

function agregation_finale();
	sql_code='create table casuser.agg_final {options replication=0 replace=true} as
				select code, CONF_STATUS, sum(montant) as montant
				from CASUSER.GLOBAL_AGG
				group by code, CONF_STATUS;';
	fedsql.execdirect / query=sql_code;
	table.droptable / caslib="public" name="agg_final" quiet=true;
	table.promote / sourcecaslib="casuser" name="agg_final" target="AGG_FINAL" targetcaslib="public";
end;



/************************************************************************************************************************************/
/* snippets en tampon                                                                                                               */
/************************************************************************************************************************************/

		/* Version CAS Action aggregate */
		/*proc cas ;*/
		/*   aggregation.aggregate /*/
		/*      table={*/
		/*	  	 caslib="casuser",*/
		/*         name="global_agg",*/
		/*         groupBy={"code", "CONF_STATUS", "OBS_STATUS", "Periode_deb", "Periode_fin", "revision_deb", "revision_fin"},*/
		/*         vars={"montant"}*/
		/*      },*/
		/*      varSpecs={*/
		/*         {name='montant', summarySubset={'SUM'}, columnNames={'montant'}}*/
		/*     }*/
		/*     casout={name="agg_final", replace=True, replication=0} ;*/
		/*quit ;*/
		
		/* Version CAS Action summary + transpose */
		
		/*proc cas ;*/
		/*   simple.summary /*/
		/*      inputs={"montant"},*/
		/*      subSet={"SUM"},*/
		/*      table={*/
		/*          caslib="casuser",*/
		/*         name="global_agg",*/
		/*         groupBy={"code", "CONF_STATUS", "OBS_STATUS", "Periode_deb", "Periode_fin", "revision_deb", "revision_fin"},*/
		/*         vars={"montant"}*/
		/*      },*/
		/*      casout={caslib="casuser", name="agg_final_summary", replace=True, replication=0} ;*/
		/*quit ;*/
		/**/
		/*proc cas ;*/
		/*   transpose.transpose / */
		/*      table={*/
		/*         name='agg_final_summary',*/
		/*         caslib='casuser',*/
		/*         groupBy={"code", "CONF_STATUS", "OBS_STATUS", "Periode_deb", "Periode_fin", "revision_deb", "revision_fin"}*/
		/*      },*/
		/*      id={'_Column_'},*/
		/*      casOut={name='agg_final', caslib='casuser', replace=true},*/
		/*      transpose={'_Sum_'} ;*/
		/*quit ;*/
		


	/**************************************************************************************************************************************/
	/* Creation d'une vue pour ajouter des champs calcul�s avec l'extraction des codes nettoy�s par exemple on �vite ainsi la duplication */
	/* des donn�es en m�moire                                                                                                             */

/*	function create_global_view();*/
/*		table.view /*/
/*			replace=true*/
/*			caslib="casuser"*/
/*			name="global_agg_view"*/
/*			tables={{*/
/*					name="global_agg",*/
/*					caslib="casuser",*/
/*					computedVars={{name="cle"}},computedVarsProgram="cle = scan(code,4,'.');"}};*/
/*	end;*/
