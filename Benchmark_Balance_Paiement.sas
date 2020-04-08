/*****************************************************************************/
/*****************************************************************************/

cas mySession sessopts=(metrics=true);
/*caslib myCaslib datasource=(srctype="dnfs") path="/data/data/BDF_SMALL_DB" sessref=mySession subdirs;*/
caslib myCaslib datasource=(srctype="dnfs") path="/SAS/BDF" sessref=mySession subdirs;
*libname myCaslib cas;

caslib _all_ assign;

/* Scan the input directory for .csv files */
proc cas;
	table.fileinfo result=listfiles / caslib="myCaslib";
	do row over listfiles.fileinfo[1:listfiles.fileinfo.nrows];
		if (index(row.Name,'.csv')<>0) then do;
			datafile=row.Name;
			tablename=scan(row.Name,1);
			table.droptable / caslib="public" name=tablename quiet=true;
			table.loadTable / 
				casout={caslib="public" name=tablename promote=true} 
				caslib="myCaslib" 
				path=datafile 
				importoptions={delimiter=";" filetype="csv" guessRows=10000 getnames=true varchars=true stripblanks=true nThreads=8};
/*			table.columninfo / table={caslib="public" name=tablename} ;*/
/*			table.tableinfo / caslib="public" name=tablename ;*/
		end;
	end;
quit;


/*****************************************************************************/
/* Concatenation de toutes les tables d'agregats                             */
/*****************************************************************************/

proc cas;
	function appendTable(inputcaslib, inputcastab, outputcaslib, outputcastab);
		codeds="data """ || outputcastab || """(caslib=""" || outputcaslib || """ append=yes); set """ || inputcastab || """(caslib=""" || inputcaslib || """); run;";
		print codeds;
		dataStep.runCode / code=codeds;
	end;

	/* Liste toutes les tables d'agregat et les concatene toutes en une seule en mémoire */

	table.tableinfo result=listtables / caslib="public";
	table.droptable / caslib="casuser" name="global_agg" quiet=true;
	do row over listtables.tableinfo[1:listtables.tableinfo.nrows];
		if !(upcase(row.Name) in {'PLANAGREGATION','TABLEPAYSZONE'}) then do;
			appendTable("public",row.name,"casuser","global_agg");
		end; 
	end;

quit;


/*****************************************************************************/
/* Nettoyage des codes                                                       */
/*****************************************************************************/



/*****************************************************************************/
/* Jointures                                                                 */
/*****************************************************************************/



/*****************************************************************************/
/* Aggreation                                                                */
/*****************************************************************************/

/* Version SQL */
proc fedsql sessref=mySession _method;
	create table casuser.agg_final
		{options replication=0 replace=true} as
	select code, CONF_STATUS, OBS_STATUS, Periode_deb, Periode_fin, revision_deb, revision_fin, sum(montant) as montant
		from CASUSER.GLOBAL_AGG
			group by code, CONF_STATUS, OBS_STATUS, Periode_deb, Periode_fin, revision_deb, revision_fin;
quit;


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


/* Version Datastep */
/*data casuser.agg_final_2(replace=yes);*/
/*	set casuser.global_agg;*/
/*	by code CONF_STATUS OBS_STATUS Periode_deb Periode_fin revision_deb revision_fin;*/
/*	retain sum_montant 0;*/
/*	sum_montant=sum_montant+montant;*/
/*	keep code CONF_STATUS OBS_STATUS Periode_deb Periode_fin revision_deb revision_fin sum_montant;*/
/*	if last.code then output;*/
/*run;*/

/*****************************************************************************/
/* Cloture session                                                           */
/*****************************************************************************/

cas mysession terminate;
