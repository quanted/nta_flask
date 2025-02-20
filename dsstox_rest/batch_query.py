import psycopg2
import os
import pandas as pd
import logging
import time

import multiprocessing as mp

from flask import jsonify
from flask_restful import Resource, reqparse

user = os.environ.get("POSTGRES_USER")
pw = os.environ.get("AURORA_PW")
host = os.environ.get("POSTGRES_HOST")
dbname = os.environ.get("POSTGRES_DB")
port = os.environ.get("POSTGRES_PORT")

# request parser
parser = reqparse.RequestParser()
parser.add_argument("search_by")
parser.add_argument("query", action="append")
parser.add_argument("accuracy", type=float, required=False)

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
        search_by = args["search_by"]
        query = list(args["query"])
        logger.info("# of queries: {}".format(len(query)))
        result = None
        dbconn = psycopg2.connect(host=host, port=port, user=user, password=pw, database=dbname)
        if search_by == "mass":
            accuracy = args["accuracy"]
            if accuracy is None:
                return jsonify(
                    {
                        "Error": "If searching by mass, the 'accuracy' parameter must be provided in ppm"
                    }
                )
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
            sql = (
                """Select '"""
                + str(massquery)
                + """' as "INPUT", msr_dsstox_compound_id as "DTXCID_INDIVIDUAL_COMPONENT", 
                msr_monoisotopic_mass as "MONOISOTOPIC_MASS_INDIVIDUAL_COMPONENT",
                msr_smiles as "SMILES_INDIVIDUAL_COMPONENT", dsstox_substance_id as "DTXSID", preferred_name as "PREFERRED_NAME", 
                casrn as "CASRN", jchem_inchi_key as "INCHIKEY", acd_iupac_name as "IUPAC_NAME", mol_formula as "MOLECULAR_FORMULA",
                monoisotopic_mass as "MONOISOTOPIC_MASS", total_median as "EXPOCAST_MEDIAN_EXPOSURE_PREDICTION_MG/KG-BW/DAY",
                expocast_comptox_link as "EXPOCAST", nhanes_comptox_link as "NHANES", data_sources as "DATA_SOURCES", 
                round(assay_count_active/assay_count_total*100,2) as "TOXCAST_PERCENT_ACTIVE", 
                assay_count_active || '/' || assay_count_total as "TOXCAST_NUMBER_OF_ASSAYS/TOTAL"
                FROM ms1_batch_search
                where msr_monoisotopic_mass BETWEEN """
                + str(min_mass)
                + """ AND """
                + str(max_mass)
                + """;"""
            )
            results = pd.concat([results, pd.read_sql(sql, dbconn)])
        logger.info("=========== Search complete ===========")
        results["MASS_DIFFERENCE"] = abs(
            results["INPUT"].astype(float)
            - results["MONOISOTOPIC_MASS_INDIVIDUAL_COMPONENT"].astype(float)
        )
        results["FOUND_BY"] = "Monoisotopic Mass"
        results = results.sort_values(by=["INPUT", "DATA_SOURCES"], ascending=[True, False])
        results_db_dict = results.to_dict(orient="split")
        # logger.info(db_results)
        return jsonify({"results": results_db_dict})

    def formula_search(self, query, dbconn):
        logger.info("=========== Searching ms1_data table ===========")
        results = pd.DataFrame()
        for formulaquery in query:
            sql = (
                """Select '"""
                + formulaquery
                + """' as "INPUT", msr_dsstox_compound_id as "DTXCID_INDIVIDUAL_COMPONENT", 
                msr_monoisotopic_mass as "MONOISOTOPIC_MASS_INDIVIDUAL_COMPONENT",
                msr_smiles as "SMILES_INDIVIDUAL_COMPONENT", dsstox_substance_id as "DTXSID", preferred_name as "PREFERRED_NAME", 
                casrn as "CASRN", jchem_inchi_key as "INCHIKEY", acd_iupac_name as "IUPAC_NAME", mol_formula as "MOLECULAR_FORMULA",
                monoisotopic_mass as "MONOISOTOPIC_MASS", total_median as "EXPOCAST_MEDIAN_EXPOSURE_PREDICTION_MG/KG-BW/DAY",
                expocast_comptox_link as "EXPOCAST", nhanes_comptox_link as "NHANES", data_sources as "DATA_SOURCES", 
                round(assay_count_active/assay_count_total*100,2) as "TOXCAST_PERCENT_ACTIVE", 
                assay_count_active || '/' || assay_count_total as "TOXCAST_NUMBER_OF_ASSAYS/TOTAL"
                FROM ms1_batch_search
                where msr_mol_formula = '"""
                + formulaquery
                + """';"""
            )
            results = pd.concat([results, pd.read_sql(sql, dbconn)])
        logger.info("=========== Search complete ===========")
        results["FOUND_BY"] = "Exact Formula"
        results = results.sort_values(by=["INPUT", "DATA_SOURCES"], ascending=[True, False])
        results_db_dict = results.to_dict(orient="split")
        return jsonify({"results": results_db_dict})


class DsstoxMSRFormulas(Resource):
    def get(self):
        results = pd.DataFrame()
        dbconn = psycopg2.connect(host=host, port=port, user=user, password=pw, database=dbname)
        sql = """SELECT DISTINCT msr_mol_formula AS "MS-READY MOLECULAR FORMULA" FROM ms1_batch_search;"""
        results = pd.concat([results, pd.read_sql(sql, dbconn)])
        results_db_dict = results.to_dict(orient="split")
        del results_db_dict["index"]
        return jsonify({"results": results_db_dict})
