import sqlite3 as sql
import os
import pandas as pd
import logging

import multiprocessing as mp

from flask import jsonify
from flask_restful import Resource, reqparse

# request parser
parser = reqparse.RequestParser()
parser.add_argument('search_by')
parser.add_argument('query', action='append')
parser.add_argument('accuracy', type=float, required=False)

DSSTOX_PATH = os.path.abspath(os.path.join(os.path.dirname( __file__ ), '..', 'database/dsstox_reduced.db'))
EXPOCAST_PATH = os.path.abspath(os.path.join(os.path.dirname( __file__ ), '..', 'database/expocast.db'))
ASSAY_PATH = os.path.abspath(os.path.join(os.path.dirname( __file__ ), '..', 'database/assay_count.db'))

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
        db_connection = DsstoxDB(DSSTOX_PATH)
        if search_by == "mass":
            accuracy = args['accuracy']
            if accuracy is None:
                return jsonify({"Error": "If searching by mass, the 'accuracy' parameter must be provided in ppm"})
            result = db_connection.mass_search(query, accuracy)
            db_connection.close()
        elif search_by == "formula":
            result = db_connection.formula_search(query)
            db_connection.close()
        else:
            return jsonify({"Error": "search_by must be either  'mass' or 'formula'"})
        return result


class DsstoxDB:
    def __init__(self, path):
        self.conn = sql.connect(path)
        self.c = None
        logger.info("=========== Dsstox DB connection established ===========")

    def mass_search(self, query, accuracy):   # Note - accuracy comes in as ppm
        self.c = self.conn.cursor()
        self.c.execute('ATTACH DATABASE ? AS expocast;', (EXPOCAST_PATH,))
        self.c.execute('ATTACH DATABASE ? AS assay;', (ASSAY_PATH,))
        self.c.execute('PRAGMA cache_size = -4000000;') # approx 4 gb
        self.c.execute('PRAGMA temp_store = MEMORY;')
        db_results = pd.DataFrame(columns = ['INPUT', 'DTXCID_INDIVIDUAL_COMPONENT', 'MONOISOTOPIC_MASS_INDIVIDUAL_COMPONENT',
                                         'SMILES_INDIVIDUAL_COMPONENT', 'DTXSID', 'PREFERRED_NAME', 'CASRN',
                                         'INCHIKEY', 'IUPAC_NAME', 'MOLECULAR_FORMULA', 'MONOISOTOPIC_MASS',
                                             'EXPOCAST_MEDIAN_EXPOSURE_PREDICTION_MG/KG-BW/DAY', 'EXPOCAST', 'NHANES',
                                             'DATA_SOURCES', 'TOXCAST_PERCENT_ACTIVE','TOXCAST_NUMBER_OF_ASSAYS/TOTAL'])
        logger.info("=========== Searching Dsstox DB ===========")
        self.c.execute('CREATE TEMP TABLE search (mass REAL PRIMARY KEY);')
        query_list = [(float(i),) for i in query]
        self.c.executemany('INSERT INTO search (mass) VALUES (?)', query_list)
        cursor = self.c.execute('''
                                SELECT s.mass, c_suc.dsstox_compound_id, c_suc.monoisotopic_mass, c_suc.smiles, gs.dsstox_substance_id,
                                gs.preferred_name, gs.casrn, c.jchem_inchi_key, c.acd_iupac_name, c.mol_formula,
                                c.monoisotopic_mass, em.Total_median, 
                                CASE em.Total_median 
                                    WHEN ''
                                        THEN ''
                                    ELSE "https://comptox.epa.gov/dashboard/dsstoxdb/results?search=" || gs.dsstox_substance_id || "#exposure-predictions"
                                END EXPOCAST,
                                CASE em.inNHANES
                                    WHEN 'TRUE'
                                        THEN "https://comptox.epa.gov/dashboard/dsstoxdb/results?search=" || gs.dsstox_substance_id || "#monitoring-data"
                                      ELSE ''
                                END NHANES,
                                count(DISTINCT ss.fk_chemical_list_id)+1 as DATA_SOURCES,
                                round(CAST(cac.assay_count_active AS FLOAT)/cac.assay_count_total*100,2) as TOXCAST_PERCENT_ACTIVE,
                                cac.assay_count_active || "/" || cac.assay_count_total
                                FROM search as s 
                                JOIN compounds as c_suc ON c_suc.monoisotopic_mass BETWEEN s.mass - (? * (s.mass / 1000000)) AND s.mass + (? * (s.mass / 1000000))
                                JOIN compound_relationships as cr ON cr.fk_compound_id_successor = c_suc.id
                                JOIN compounds as c ON cr.fk_compound_id_predecessor = c.id
                                JOIN generic_substance_compounds as gsc ON c.id = gsc.fk_compound_id
                                JOIN generic_substances as gs ON gsc.fk_generic_substance_id = gs.id
                                LEFT JOIN source_generic_substance_mappings as sgsm ON sgsm.fk_generic_substance_id = gs.id
                                LEFT JOIN source_substances as ss ON ss.id = sgsm.fk_source_substance_id
                                LEFT JOIN assay.chemical_assay_count as cac ON cac.chid = gs.id
                                LEFT JOIN expocast.expocast_models as em ON em.casrn = gs.casrn 
                                WHERE cr.fk_compound_relationship_type_id == 2 AND c.has_defined_isotope == 0
                                GROUP BY s.mass, c.id;
                                /**ORDER BY DATA_SOURCES DESC;*//
                                ''', (accuracy,accuracy))
        logger.info("=========== Parsing results ===========")
        results = cursor.fetchmany(cursor.lastrowid)
        for row in results:
            ind = len(db_results.index)
            db_results.loc[ind] = row
        logger.info("=========== Search complete ===========")
        db_results['MASS_DIFFERENCE'] = abs(db_results['INPUT'].astype(float) -
                                                  db_results['MONOISOTOPIC_MASS_INDIVIDUAL_COMPONENT'].astype(float))
        db_results['FOUND_BY'] = 'Monoisotopic Mass'
        db_results = db_results.sort_values(by=['INPUT', 'DATA_SOURCES'], ascending=[True, False])
        results_db_dict = db_results.to_dict(orient='split')
        return jsonify({'results': results_db_dict})

    def formula_search(self, query):
        self.c = self.conn.cursor()
        self.c.execute('ATTACH DATABASE ? AS expocast;', (EXPOCAST_PATH,))
        self.c.execute('ATTACH DATABASE ? AS assay;', (ASSAY_PATH,))
        self.c.execute('PRAGMA cache_size = -4000000;')  # approx 4 gb
        self.c.execute('PRAGMA temp_store = MEMORY;')
        db_results = pd.DataFrame(
            columns=['INPUT', 'DTXSID', 'PREFERRED_NAME', 'CASRN', 'INCHIKEY', 'IUPAC_NAME', 'MOLECULAR_FORMULA',
                     'MONOISOTOPIC_MASS', 'EXPOCAST_MEDIAN_EXPOSURE_PREDICTION_MG/KG-BW/DAY', 'EXPOCAST', 'NHANES',
                     'DATA_SOURCES', 'TOXCAST_PERCENT_ACTIVE', 'TOXCAST_NUMBER_OF_ASSAYS/TOTAL'])
        self.c.execute('CREATE TEMP TABLE search (formula VARCHAR(255) PRIMARY KEY);')
        query_list = [(i,) for i in query]
        self.c.executemany('INSERT INTO search (formula) VALUES (?)', query_list)
        logger.info("=========== Searching Dsstox DB ===========")
        cursor = self.c.execute('''
                                            SELECT s.formula, gs.dsstox_substance_id, gs.preferred_name, gs.casrn, c.jchem_inchi_key, 
                                            c.acd_iupac_name, c.mol_formula, c.monoisotopic_mass, em.Total_median, 
                                            CASE em.Total_median 
                                                WHEN ''
                                                    THEN ''
                                                ELSE "https://comptox.epa.gov/dashboard/dsstoxdb/results?search=" || gs.dsstox_substance_id || "#exposure-predictions"
                                            END EXPOCAST,
                                            CASE em.inNHANES
                                                WHEN 'TRUE'
                                                    THEN "https://comptox.epa.gov/dashboard/dsstoxdb/results?search=" || gs.dsstox_substance_id || "#monitoring-data"
                                                  ELSE ''
                                            END NHANES,
                                            count(DISTINCT ss.fk_chemical_list_id)+1 as DATA_SOURCES,
                                            round(CAST(cac.assay_count_active AS FLOAT)/cac.assay_count_total*100,2) as TOXCAST_PERCENT_ACTIVE,
                                            cac.assay_count_active || "/" || cac.assay_count_total
                                            FROM search as s
                                            JOIN compounds as c ON s.formula == c.mol_formula
                                            JOIN generic_substance_compounds as gsc ON c.id = gsc.fk_compound_id
                                            JOIN generic_substances as gs ON gsc.fk_generic_substance_id = gs.id
                                            LEFT JOIN source_generic_substance_mappings as sgsm ON sgsm.fk_generic_substance_id = gs.id
                                            LEFT JOIN source_substances as ss ON ss.id = sgsm.fk_source_substance_id
                                            LEFT JOIN assay.chemical_assay_count as cac ON cac.chid = gs.id
                                            LEFT JOIN expocast.expocast_models as em ON em.casrn = gs.casrn
                                            GROUP BY c.id;
                                            ''')
        logger.info("=========== Parsing results ===========")
        results = cursor.fetchmany(cursor.lastrowid)
        for row in results:
            ind = len(db_results.index)
            db_results.loc[ind] = row
        logger.info("=========== Search complete ===========")
        db_results['FOUND_BY'] = 'Exact Formula'
        db_results = db_results.sort_values(by=['INPUT', 'DATA_SOURCES'], ascending=[True, False])
        results_db_dict = db_results.to_dict(orient='split')
        return jsonify({'results': results_db_dict})

    def close(self):
        if self.c:
            self.c.close()
        self.conn.close()

