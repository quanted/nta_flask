import sqlite3 as sql
import os
import pandas as pd
import logging
import json

from flask import jsonify
from flask_restful import Resource, reqparse

### request parser
parser = reqparse.RequestParser()
parser.add_argument('search_by')
parser.add_argument('query', action='append')
parser.add_argument('accuracy', type=float)

DSSTOX_PATH = os.path.abspath(os.path.join(os.path.dirname( __file__ ), '..', 'database/dss_tox_3.db'))
EXPOCAST_PATH = os.path.abspath(os.path.join(os.path.dirname( __file__ ), '..', 'database/expocast.db'))
ASSAY_PATH = os.path.abspath(os.path.join(os.path.dirname( __file__ ), '..', 'database/assay_count.db'))


class DsstoxBatchSearch(Resource):

    def post(self, jobId="000000100000011"):
        """
        dsstox batch earch handler.
        :param jobId:
        :return: a dataframe of the result
        """
        logging.info("=========== NTA batch search received ===========")
        args = parser.parse_args()
        search_by = args['search_by']
        query = args['query']
        print(query)
        accuracy = args['accuracy']
        result = None
        db_connection = Dsstox_DB(DSSTOX_PATH)
        if search_by == "mass":
            result = db_connection.mass_search(query, accuracy)
        return result


class Dsstox_DB:
    def __init__(self, path):
        self.conn = sql.connect(path)
        self.c = None
        logging.info("=========== Dsstox DB connection established ===========")

    def mass_search(self, query, accuracy):   # Note - accuracy is ppm
        self.c = self.conn.cursor()
        self.c.execute('ATTACH DATABASE ? AS expocast;', (EXPOCAST_PATH,))
        self.c.execute('ATTACH DATABASE ? AS assay;', (ASSAY_PATH,))
        db_results = pd.DataFrame(columns = ['INPUT', 'DTXCID_INDIVIDUAL_COMPONENT', 'MONOISOTOPIC_MASS_INDIVIDUAL_COMPONENT',
                                         'SMILES_INDIVIDUAL_COMPONENT', 'DTXSID', 'PREFERRED_NAME', 'CASRN',
                                         'INCHIKEY', 'IUPAC_NAME', 'MOLECULAR_FORMULA', 'MONOISOTOPIC_MASS',
                                             'EXPOCAST_MEDIAN_EXPOSURE_PREDICTION_MG/KG-BW/DAY', 'EXPOCAST', 'NHANES',
                                             'DATA_SOURCES', 'TOXCAST_PERCENT_ACTIVE','TOXCAST_NUMBER_OF_ASSAYS/TOTAL'])
        logging.info("=========== Searching Dsstox DB ===========")
        for i, mass in enumerate(query):
            #cursor2 = self.c.execute('''
            #        SELECT c.dsstox_compound_id, c.monoisotopic_mass, c.smiles, gs.dsstox_substance_id,
            #        gs.preferred_name, gs.casrn, c.jchem_inchi_key, c.acd_iupac_name, c.mol_formula
            #        FROM compounds as c, generic_substances as gs, generic_substance_compounds as gsc
            #        WHERE abs(c.monoisotopic_mass - ?) < ?
            #        AND gsc.fk_compound_id = c.id
            #        AND gsc.fk_generic_substance_id = gs.id;
            #        ''', (mass, accuracy))
            mass = float(mass)
            cursor = self.c.execute('''
                                SELECT c_suc.dsstox_compound_id, c_suc.monoisotopic_mass, c_suc.smiles, gs.dsstox_substance_id,
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
                                FROM compounds as c
                                JOIN generic_substance_compounds as gsc ON c.id = gsc.fk_compound_id
                                JOIN generic_substances as gs ON gsc.fk_generic_substance_id = gs.id
                                JOIN compound_relationships as cr ON cr.fk_compound_id_predecessor = c.id
                                JOIN compounds as c_suc ON c_suc.id = cr.fk_compound_id_successor
                                JOIN source_generic_substance_mappings as sgsm ON sgsm.fk_generic_substance_id = gs.id
                                JOIN source_substances as ss ON ss.id = sgsm.fk_source_substance_id
                                JOIN assay.chemical_assay_count as cac ON cac.chid = gs.id
                                JOIN expocast.expocast_models as em ON em.casrn = gs.casrn 
                                WHERE abs(c_suc.monoisotopic_mass - ?) < ? AND cr.fk_compound_relationship_type_id == 2
                                AND c.has_defined_isotope == 0
                                ORDER BY DATA_SOURCES DESC;
                                ''', (mass, accuracy))
            logging.info("=========== Parsing results ===========")
            for row in cursor:
                ind = len(db_results.index)
                print(ind)
                db_results.loc[ind] = (mass,) + row
        logging.info("=========== Search complete ===========")
        #db_results['INPUT'] = query
        db_results['MASS_DIFFERENCE'] = abs(db_results['INPUT'].astype(float) -
                                                  db_results['MONOISOTOPIC_MASS_INDIVIDUAL_COMPONENT'].astype(float))
        db_results['FOUND_BY'] = 'Monoisotopic Mass'
        results_db_dict = db_results.to_dict(orient='list')
        return jsonify(results_db_dict)

    def close(self):
        self.conn.close()
