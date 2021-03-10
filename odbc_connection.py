"""
https://stackoverflow.com/questions/16280304/pyodbc-data-source-name-not-found-and-no-default-driver-specified
https://docs.databricks.com/integrations/bi/jdbc-odbc-bi.html

Download and install the Spark ODBC/JDBC driver
https://databricks.com/spark/odbc-driver-download

On a Mac, the odbc.ini and odbcinst.ini files are stored in the /etc directory

# Contents of odbcinst.ini - driver information...

[ODBC Drivers]
Simba Spark ODBC Driver = Installed

[Simba Spark ODBC Driver]
Driver = /Library/simba/spark/lib/libsparkodbc_sbu.dylib


# Contents of odbc.ini - Reference to driver and Databricks workspace information; some attributes
# that can be specified in this file...

[Databricks-Spark]
Driver=Simba Spark ODBC Driver
Server=adb-523423423423420.0.azuredatabricks.net
HOST=adb-523423423423420.0.azuredatabricks.net
PORT=443
SparkServerType=3
Schema=default
ThriftTransport=2
SSL=1
AuthMech=3
UID=token


The obdc.ini file above lists the parameters that are sources in the JDBC/ODBC connection string. It is fine for some
of these to be static values in this file, but others should be dynamic. For instance, a user may want the ability to 
connect to different clusters. These parameters can be passed as parameters directly via the query string and take precedent 
over the odbc.ini parameter values.

Lastly, be sure the set the follow environment variables in your ~/.bash_profile
export ODBCINI=/etc/odbc.ini
export ODBCSYSINI=/etc
export SIMBASPARKINI=/Library/simba/spark/lib/simba.sparkodbc.ini
"""

import pyodbc


def fetch_query(host_path, pat_token, query_text):
    """
    The function assumes the Server and HOST names, which correspond to a Databricks workspace,
    are set in the odbc.ini file. This function can then be used to issue queries against Clusters
    within the workspace by passing in the host_path (HTTP Path in JDBC/ODBC tab of Cluster UI)
    """

    conn = pyodbc.connect(f"DSN=Databricks-Spark; HTTPPath={host_path}; PWD={pat_token}", autocommit=True)
    cursor = conn.cursor()

    result = cursor.execute(query_text).fetchall()

    conn.close()

    return result






