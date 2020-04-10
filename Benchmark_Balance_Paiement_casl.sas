/*****************************************************************************/
/*****************************************************************************/

cas mySession sessopts=(metrics=true);
*caslib myCaslib datasource=(srctype="dnfs") path="/data/data/BDF_SMALL_DB" sessref=mySession subdirs;
caslib myCaslib datasource=(srctype="dnfs") path="/SAS/BDF" sessref=mySession subdirs;
*libname myCaslib cas;

caslib _all_ assign;

proc cas;

	/* Lecture_table_agregation
	/************************************************************************************************************************************/
	/* Scan the input directory for .csv files, import them and strip blanks (begin and end) inside all string columns at the same time */
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
	/* Get all information on imported tables and print it as output                                                                    */
	function get_info_on_all_imported_files();
		table.tableinfo result=list_tab / caslib="public";
		print list_tab;
		do row over list_tab.tableinfo[1:list_tab.tableinfo.nrows];
			table.columninfo / table={caslib="public" name=row.Name} ;
		end;
	end;

	/************************************************************************************************************************************/
	/* Preparation table pays zone                                                                                                      */
	function prepare_pays_zone();
			
	end;

	/************************************************************************************************************************************/
	/* Concatenate two tables, cleanse code and create cle                                                                              */

	function appendTable(inputcaslib, inputcastab, outputcaslib, outputcastab);
		codeds="data """ || outputcastab || """(caslib=""" || outputcaslib || """ append=yes); set """ || inputcastab || """(caslib=""" || inputcaslib || """); length cle $ 3; cle = scan(code,4,'.'); run;";
		print codeds;
		dataStep.runCode / code=codeds;
	end;

	/************************************************************************************************************************************/
	/* Liste toutes les tables d'agregat et les concatene toutes en une seule en memoire contenat les champs nettoy�s  */

	function append_all_tables();
		table.tableinfo result=listtables / caslib="public";
		table.droptable / caslib="casuser" name="global_agg" quiet=true;
		do row over listtables.tableinfo[1:listtables.tableinfo.nrows];
			if !(upcase(row.Name) in {'PLANAGREGATION','TABLEPAYSZONE'}) then do;
				appendTable("public",row.name,"casuser","global_agg");
			end;
		end;
	end;


	
	/************************************************************************************************************************************/
	/* Aggregate all data grouped by all colums except montant                                                                          */

	function agregation_finale();
		sql_code='create table casuser.agg_final {options replication=0 replace=true} as
					select code, CONF_STATUS, OBS_STATUS, Periode_deb, Periode_fin, revision_deb, revision_fin, sum(montant) as montant
					from CASUSER.GLOBAL_AGG
					group by code, CONF_STATUS, OBS_STATUS, Periode_deb, Periode_fin, revision_deb, revision_fin;';
		fedsql.execdirect / query=sql_code;
		table.droptable / caslib="public" name=agg_final quiet=true;
		table.promote / sourcecaslib="casuser" name="agg_final" target="AGG_FINAL" targetcaslib="public";


	end;

	
	/************************************************************************************************************************************/
	/* Main pipeline */
	/************************************************************************************************************************************/

	import_all_csv_files();
	*get_info_on_all_imported_files();
	append_all_tables();
	*create_global_view();
	agregation_finale();
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