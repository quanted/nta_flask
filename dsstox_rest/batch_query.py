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

DSSTOX_PATH = os.path.abspath(os.path.join(os.path.dirname( __file__ ), '..', 'database/dss_tox.db'))

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


    def mass_search(self, query, accuracy):
        self.c = self.conn.cursor()
        db_results = pd.DataFrame(columns = ['INPUT', 'DTXCID_INDIVIDUAL_COMPONENT', 'MONOISOTOPIC_MASS_INDIVIDUAL_COMPONENT',
                                         'SMILES_INDIVIDUAL_COMPONENT', 'DTXSID', 'PREFERRED_NAME', 'CASRN',
                                         'INCHIKEY', 'IUPAC_NAME', 'MOLECULAR_FORMULA'])
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
            cursor = self.c.execute('''
                                SELECT c.dsstox_compound_id, c.monoisotopic_mass, c.smiles, gs.dsstox_substance_id,
                                gs.preferred_name, gs.casrn, c.jchem_inchi_key, c.acd_iupac_name, c.mol_formula
                                FROM generic_substance_compounds as gsc
                                JOIN compounds as c ON c.id = gsc.fk_compound_id
                                JOIN generic_substances as gs ON gsc.fk_generic_substance_id = gs.id
                                WHERE abs(c.monoisotopic_mass - ?) < ?;
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
        db_results['FOUND_BY'] = 'Mass'
        results_db_dict = db_results.to_dict(orient='list')
        return jsonify(results_db_dict)

    def close(self):
        self.conn.close()
