/************************************************************************************************************************************/
/************************************************************************************************************************************/

cas mySession sessopts=(metrics=true);
caslib myCaslib datasource=(srctype="dnfs") path="/data/data/BDF_SMALL_DB" sessref=mySession subdirs;
*caslib myCaslib datasource=(srctype="dnfs") path="/SAS/BDF" sessref=mySession subdirs;
*libname myCaslib cas;

caslib _all_ assign;

proc cas;
	%include "/data/github/POC_001/Balance_paiement_casl_functions.sas";
	/************************************************************************************************************************************/
	/* Main pipeline */
	/************************************************************************************************************************************/

	import_all_csv_files();
	*prepare_pizone();
	prepare_planagreggation("public", "PLANAGREGATION","public", "PLANAGREGATION_T");

	parametrage_aggregation_code_serie("public", "TESTPLANAG", "PIZONES", "planagregation_t");

	*get_info_on_all_imported_files();
	*append_all_tables();
	*create_global_view();
	*agregation_finale();

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
