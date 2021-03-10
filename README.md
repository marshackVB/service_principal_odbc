## Adding an Azure Service Principal to a Databricks Workspace and connecting to a cluster via the JDBC/ODBC connector

#### Steps
1. Download the Spark Simba Driver and configure the driver (tips in the odbc_connection.py file)  
2. Create an Azure Service Principal in Active Directory and generate a Secret for the SP 
3. Walk through the example.py file; review the documentation in the create_user.py file  
    - Add Service Principal to workspace
    - Generage AD token for the Service Principal
    - Use the AD token to generate a PAT token for the Service Principal
    - Use the Service Principal to authenticate via the JDBC/ODBC driver


Be sure to grant the Service Principal a Contributor role to the Databricks Workspace via the Acess control (IAM) view of the Workspace in Azure.