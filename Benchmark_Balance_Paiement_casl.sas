/************************************************************************************************************************************/

cas mySession sessopts=(metrics=true);
caslib myCaslib datasource=(srctype="dnfs") path="/data/data/BDF_SMALL_DB" sessref=mySession subdirs;
*caslib myCaslib datasource=(srctype="dnfs") path="/SAS/BDF" sessref=mySession subdirs;
*libname myCaslib cas;

caslib _all_ assign;


proc cas;
	%include "/data/github/POC_001/Balance_paiement_casl_functions.sas";

	import_all_csv_files();
	prepare_pizone();
	prepare_planagreggation("public", "PLANAGREGATION","public", "PLANAGREGATION_T");
	parametrage_aggregation_code_serie("public", "TESTPLANAG", "PIZONES", "planagregation_t");

	*get_info_on_all_imported_files();
	*append_all_tables();
	*create_global_view();
	*agregation_finale();

	/* Sauvegarde de la table final au format parquet */
	table.save / caslib=incaslib name=incastab || ".parquet" table=incastab replace=true;

	table.tableinfo / caslib="public";
quit;


/*****************************************************************************/
/* Cloture session                                                           */
/*****************************************************************************/

cas mysession terminate;

