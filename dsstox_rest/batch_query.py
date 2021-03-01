import psycopg2
import os
import pandas as pd
import logging
import time

import multiprocessing as mp

from flask import jsonify
from flask_restful import Resource, reqparse

user = os.environ.get('POSTGRES_USER')
pw = os.environ.get('POSTGRES_PW')
host = os.environ.get('POSTGRES_HOST')
dbname = os.environ.get('POSTGRES_DB')
port = os.environ.get('POSTGRES_PORT')

# request parser
parser = reqparse.RequestParser()
parser.add_argument('search_by')
parser.add_argument('query', action='append')
parser.add_argument('accuracy', type=float, required=False)

logger = logging.getLogger("nta_flask")
logger.setLevel(logging.INFO)


class DsstoxBatchSearch(Resource):

    def post(self, jobId="000000100000011"):
        """
        dsstox batch earch handler.
        :param jobId:
        :return: a dataframe of the result
        """
        logger.info("=========== NTA batch search received ===========")
        args = parser.parse_args()
        search_by = args['search_by']
        query = list(args['query'])
        logger.info("# of queries: {}".format(len(query)))
        result = None
        dbconn = psycopg2.connect(
            host=host,
            port=port,
            user=user,
            password=pw,
            database=dbname)
        if search_by == "mass":
            accuracy = args['accuracy']
            if accuracy is None:
                return jsonify({"Error": "If searching by mass, the 'accuracy' parameter must be provided in ppm"})
            result = self.mass_search(query, accuracy, dbconn)
            dbconn.close()
        elif search_by == "formula":
            result = self.formula_search(query, dbconn)
            dbconn.close()
        else:
            return jsonify({"Error": "search_by must be either  'mass' or 'formula'"})
        return result

    def mass_search(self, query, accuracy, dbconn):  # Note - accuracy comes in as ppm
        logger.info("=========== Searching ms1_data table ===========")
        query_list = [(float(i)) for i in query]
        results = pd.DataFrame()
        for massquery in query_list:
            max_mass = massquery + massquery * accuracy / 1000000
            min_mass = massquery - massquery * accuracy / 1000000
            sql = """Select '""" + str(massquery) + """' as "INPUT", msr_dsstox_compound_id as "DTXCID_INDIVIDUAL_COMPONENT", 
                msr_monoisotopic_mass as "MONOISOTOPIC_MASS_INDIVIDUAL_COMPONENT",
                msr_smiles as "SMILES_INDIVIDUAL_COMPONENT", dsstox_substance_id as "DTXSID", preferred_name as "PREFERRED_NAME", 
                casrn as "CASRN", jchem_inchi_key as "INCHIKEY", acd_iupac_name as "IUPAC_NAME", mol_formula as "MOLECULAR_FORMULA",
                monoisotopic_mass as "MONOISOTOPIC_MASS", total_median as "EXPOCAST_MEDIAN_EXPOSURE_PREDICTION_MG/KG-BW/DAY",
                expocast_comptox_link as "EXPOCAST", nhanes_comptox_link as "NHANES", data_sources as "DATA_SOURCES", 
                round(assay_count_active/assay_count_total*100,2) as "TOXCAST_PERCENT_ACTIVE", 
                assay_count_active || '/' || assay_count_total as "TOXCAST_NUMBER_OF_ASSAYS/TOTAL"
                FROM ms1_batch_search
                where msr_monoisotopic_mass BETWEEN """ + str(min_mass) + """ AND """ + str(max_mass) + """;"""
            results = results.append(pd.read_sql(sql, dbconn))
        logger.info("=========== Search complete ===========")
        results['MASS_DIFFERENCE'] = abs(results['INPUT'].astype(float) -
                                         results['MONOISOTOPIC_MASS_INDIVIDUAL_COMPONENT'].astype(float))
        results['FOUND_BY'] = 'Monoisotopic Mass'
        results = results.sort_values(by=['INPUT', 'DATA_SOURCES'], ascending=[True, False])
        results_db_dict = results.to_dict(orient='split')
        # logger.info(db_results)
        return jsonify({'results': results_db_dict})

    def formula_search(self, query, dbconn):
        logger.info("=========== Searching ms1_data table ===========")
        results = pd.DataFrame()
        for formulaquery in query:
            sql = """Select '""" + formulaquery + """' as "INPUT", msr_dsstox_compound_id as "DTXCID_INDIVIDUAL_COMPONENT", 
                msr_monoisotopic_mass as "MONOISOTOPIC_MASS_INDIVIDUAL_COMPONENT",
                msr_smiles as "SMILES_INDIVIDUAL_COMPONENT", dsstox_substance_id as "DTXSID", preferred_name as "PREFERRED_NAME", 
                casrn as "CASRN", jchem_inchi_key as "INCHIKEY", acd_iupac_name as "IUPAC_NAME", mol_formula as "MOLECULAR_FORMULA",
                monoisotopic_mass as "MONOISOTOPIC_MASS", total_median as "EXPOCAST_MEDIAN_EXPOSURE_PREDICTION_MG/KG-BW/DAY",
                expocast_comptox_link as "EXPOCAST", nhanes_comptox_link as "NHANES", data_sources as "DATA_SOURCES", 
                round(assay_count_active/assay_count_total*100,2) as "TOXCAST_PERCENT_ACTIVE", 
                assay_count_active || '/' || assay_count_total as "TOXCAST_NUMBER_OF_ASSAYS/TOTAL"
                FROM ms1_batch_search
                where msr_mol_formula = '""" + formulaquery + """';"""
            results = results.append(pd.read_sql(sql, dbconn))
        logger.info("=========== Search complete ===========")
        results['FOUND_BY'] = 'Exact Formula'
        results = results.sort_values(by=['INPUT', 'DATA_SOURCES'], ascending=[True, False])
        results_db_dict = results.to_dict(orient='split')
        # logger.info(db_results)
        return jsonify({'results': results_db_dict})

        # self.c = self.conn.cursor()
        # self.c.execute('ATTACH DATABASE ? AS expocast;', (EXPOCAST_PATH,))
        # self.c.execute('ATTACH DATABASE ? AS assay;', (ASSAY_PATH,))
        # self.c.execute('PRAGMA cache_size = -4000000;')  # approx 4 gb
        # self.c.execute('PRAGMA temp_store = MEMORY;')
        # col_names = ['INPUT', 'DTXSID', 'PREFERRED_NAME', 'CASRN', 'INCHIKEY', 'IUPAC_NAME', 'MOLECULAR_FORMULA',
        #              'MONOISOTOPIC_MASS', 'EXPOCAST_MEDIAN_EXPOSURE_PREDICTION_MG/KG-BW/DAY', 'EXPOCAST', 'NHANES',
        #              'DATA_SOURCES', 'TOXCAST_PERCENT_ACTIVE', 'TOXCAST_NUMBER_OF_ASSAYS/TOTAL']
        # db_results = pd.DataFrame()
        # query_list = [(i,) for i in query]
        # self.c.executemany('INSERT INTO search (formula) VALUES (?)', query_list)
        # logger.info("=========== Searching Dsstox DB ===========")
        # self.c.execute('''
        #                                     SELECT s.formula, gs.dsstox_substance_id, gs.preferred_name, gs.casrn, c.jchem_inchi_key,
        #                                     c.acd_iupac_name, c.mol_formula, c.monoisotopic_mass, em.Total_median,
        #                                     CASE em.Total_median
        #                                         WHEN ''
        #                                             THEN ''
        #                                         ELSE "https://comptox.epa.gov/dashboard/dsstoxdb/results?search=" || gs.dsstox_substance_id || "#exposure-predictions"
        #                                     END EXPOCAST,
        #                                     CASE em.inNHANES
        #                                         WHEN 'TRUE'
        #                                             THEN "https://comptox.epa.gov/dashboard/dsstoxdb/results?search=" || gs.dsstox_substance_id || "#monitoring-data"
        #                                           ELSE ''
        #                                     END NHANES,
        #                                     count(DISTINCT ss.fk_chemical_list_id)+1 as DATA_SOURCES,
        #                                     round(CAST(cac.assay_count_active AS FLOAT)/cac.assay_count_total*100,2) as TOXCAST_PERCENT_ACTIVE,
        #                                     cac.assay_count_active || "/" || cac.assay_count_total
        #                                     FROM search as s
        #                                     JOIN compounds as c ON s.formula == c.mol_formula
        #                                     JOIN generic_substance_compounds as gsc ON c.id = gsc.fk_compound_id
        #                                     JOIN generic_substances as gs ON gsc.fk_generic_substance_id = gs.id
        #                                     LEFT JOIN source_generic_substance_mappings as sgsm ON sgsm.fk_generic_substance_id = gs.id
        #                                     LEFT JOIN source_substances as ss ON ss.id = sgsm.fk_source_substance_id
        #                                     LEFT JOIN assay.chemical_assay_count as cac ON cac.chid = gs.id
        #                                     LEFT JOIN expocast.expocast_models as em ON em.casrn = gs.casrn
        #                                     GROUP BY c.id;
        #                                     ''')
        # logger.info("=========== Parsing results ===========")
        # t0 = time.process_time()
        # total_rows = 0
        # while True:
        #     arraysize = 500
        #     rows = self.c.fetchmany(arraysize)
        #     if not rows:
        #         break
        #     db_results = db_results.append(rows, ignore_index=True)
        #     total_rows = total_rows + arraysize
        #     # logger.info("Fetched {} rows".format(total_rows))
        # t1 = time.process_time()
        # logger.info("time for SQL query is :" + str(t1 - t0))
        # db_results.columns = col_names
        # logger.info("=========== Search complete ===========")
        # db_results['FOUND_BY'] = 'Exact Formula'
        # db_results = db_results.sort_values(by=['INPUT', 'DATA_SOURCES'], ascending=[True, False])
        # results_db_dict = db_results.to_dict(orient='split')
        # return jsonify({'results': results_db_dict})
