The nta_flask application requires accessed to a reduced set of data from EPA's DSSTox, ToxCast, and ExpoCast databases.
The four steps listed below are required to initialize the database and configure the application to use that database.

# Table Creation  
The application is currently set up to work with a PostGreSQL database.  Run the DDL in _ms1_batch_search_table_ddl.sql_ 
to create the table and properly index it.

    $ psql -h %host -p $port --dbname %databaseName --user %username < ms1_batch_search_table_ddl.sql 

# Data Extraction
As mentioned above, the data for this application comes from EPA databases.  Those databases are generally not accessible
to the public.  To generate the correct data once on the internal EPA data systems, the query in _data_extraction.sql_ 
must be run and the results exported into a tsv file using the following command.  You will be prompted for the password
associated with the %username input in the command.  If you would like the data generated for you, please contact 
(%someone at the EPA).  Warning this data extraction takes a long time (~1 day).

    $ mysql -h mysql-ip-m.epa.gov -u %username -p prod_dsstox <data_extraction.sql> ms1_data.tsv

# Data Upload
Login into PostGreSQL instance that will be used to service the nta_flask application.  Run the following command at
postgres command line prompt while in the database intended for use.

    # \COPY ms1_batch_search (msr_dsstox_compound_id, msr_monoisotopic_mass,msr_smiles,msr_mol_formula, dsstox_substance_id,preferred_name,casrn,jchem_inchi_key,acd_iupac_name,mol_formula,monoisotopic_mass,total_median,expocast_comptox_link,nhanes_comptox_link,assay_count_active,assay_count_total) from '%path2File' with delimiter E'\t' null as 'NULL' csv header 

    

 