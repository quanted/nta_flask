# nta_flask
Flask restful API for the nta_app backend

This app is normally deployed as a submodule using docker-compose or kubernetes 
with the full QED application available at https://github.com/quanted/qed

# Development Deployment
For development purposes it can be deployed locally from the home directory of this repo by 
changing line 14 of flask_nta.py to import '**dsstox_rest**' rather than '**nta_flask.dsstox_rest**'

To deploy locally, run the following commands from the top directory in the repo

    $ export FLASK_APP=flask_nta.py
    $ flask run
    
The application will come up on http://127.0.0.1:5000/.  To test the application, POST the 
following request to http://127.0.0.1:5000/rest/ms1/batch/1

    {
       "search_by":"mass",
       "query": [
           500,
           600
       ],
       "accuracy":1
    }

# Data Configuration
In order for the service to be used, a database must be initialized.  For more on database initialization,
please review the markdown in _database/DATA_INITIALIZATION.md_.

To config the application for connection to the database, the following environment variables must be defined: 
POSTGRES_USER, POSTGRES_PW, POSTGRES_HOST, POSTGRES_DB