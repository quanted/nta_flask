import psycopg2
import os
import pandas as pd
import logging
import time

from flask import jsonify
from flask_restful import Resource, reqparse

user = os.environ.get('POSTGRES_USER')
pw = os.environ.get('AURORA_PW')
host = os.environ.get('POSTGRES_HOST')
dbname = os.environ.get('POSTGRES_DB')
port = os.environ.get('POSTGRES_PORT')

# request parser
parser = reqparse.RequestParser()
parser.add_argument('mass', type=float, required=True)
parser.add_argument('accuracy', type=float, required=True)
parser.add_argument('mode', type=str, required=True)

logger = logging.getLogger("nta_flask")
logger.setLevel(logging.INFO)


class MS2Search(Resource):

    def post(self, jobId="000000100000011"):
        """
        MS2 CFMID search handler.
        :param jobId:
        :return: a dataframe of the result
        """
        logger.info("=========== MS2 CFMID search received ===========")
        args = parser.parse_args()
        mass = args['mass']
        accuracy = args['accuracy']
        mode = args['mode']
        logger.info("Parent mass: {}".format(mass))
        dbconn = psycopg2.connect(
            host=host,
            port=port,
            user=user,
            password=pw,
            database='ms2_db')
        max_mass = mass + mass * accuracy / 1000000
        min_mass = mass - mass * accuracy / 1000000
        accuracy_condition = """job_peak.mass BETWEEN """ + str(min_mass) + """ AND """ + str(max_mass)
        query = """select dtxcid as "DTXCID", formula as "FORMULA", mass as "MASS", mz as "PMASS_x", intensity as "INTENSITY0C", energy as "ENERGY"
        from job_peak where """ + accuracy_condition + """ and type='""" + mode + """'order by "DTXCID","ENERGY", "INTENSITY0C" desc;"""
        chunks = list()
        for chunk in pd.read_sql(query, dbconn, chunksize=1000):
            chunks.append(chunk)
        dbconn.close()
        dbconn = None
        logger.info("num of chunks: {}".format(len(chunks)))
        full_df = pd.concat(chunks)
        logger.info('Rows of results: {}'.format(len(full_df)))
        if len(full_df) < 1:
            return jsonify({'results': 'none'})
        results_db_dict = full_df.to_dict(orient='split')
        return jsonify({'results': results_db_dict})
        