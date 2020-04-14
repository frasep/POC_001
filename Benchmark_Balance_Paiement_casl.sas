/************************************************************************************************************************************/
/************************************************************************************************************************************/

cas mySession sessopts=(metrics=true);
caslib myCaslib datasource=(srctype="dnfs") path="/data/data/BDF_SMALL_DB" sessref=mySession subdirs;
*caslib myCaslib datasource=(srctype="dnfs") path="/SAS/BDF" sessref=mySession subdirs;
*libname myCaslib cas;

caslib _all_ assign;

proc cas;

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
/*
recuperation_traitement_table_code_pays <- function(ReferencePiZones, ConnectionSecureDB)
{
  CheminReferentielPays <-  paste0("/home/ardtr/appli/travail/traard1562/traard1562_D/commun/NAB/ref_pays_zone/", ReferencePiZones, "/ref_pays_zone.sas7bdat")
  TableCodePays <- data.table::data.table(read.csv2(ReferencePiZones))
  setorder(TableCodePays, code_pays)
  TablePays <- TableCodePays[, .(code_pays)]
  TableZone <- TableCodePays[, .(code_zone)]
  PiZone <- data.table::rbindlist(list(TablePays[, pays := code_pays], TableZone[, pays := code_zone]))
PiZone$cle <- rep("_Z", ncol(PiZone))
PiZone[, c("code_pays", "pays", "cle") := list(trimws(code_pays), trimws(pays), trimws(cle))]
setorder(PiZone, pays, cle)
print("end recuperation_traitement_table_code_pays monostream")
  print(Sys.time())
return(unique(PiZone))
}
*/

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
		keep code cle CONF_STATUS montant
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
	/* Main pipeline */
	/************************************************************************************************************************************/

	import_all_csv_files();
	prepare_pizone();
	get_info_on_all_imported_files();
	append_all_tables();
	*create_global_view();
	agregation_finale();

	table.tableinfo / caslib="casuser";
	table.tableinfo / caslib="public";

quit;

/*****************************************************************************/
/* Cloture session                                                           */
/*****************************************************************************/

cas mysession terminate;

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
