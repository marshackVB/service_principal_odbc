"""
An example walkthrough of creating a Service Principal user in a Databricks
workspace, generate a PAT token for the user, and issuing a query via the
JDBC/ODBC connector

Be sure to grant the Service Principal a Contributor role to the Databricks Workspace via 
the Acess control (IAM) view of the Workspace in Azure.
"""

from create_user import CreateServicePrincipalUser
from odbc_connection import fetch_query

# Databricks workspace id
workspace_id = 

# The administrators PAT token
admin_pat = 

# Service Principal information sources from Azure Active Directory
client_id = 
tenant_id = 
application_secret = 
display_name = 

# Create the Service Principal user
sp_user = CreateServicePrincipalUser(workspace_id, 
                                     client_id, 
                                     tenant_id, 
                                     application_secret, 
                                     display_name)

# Add the Service Principal to the workspace
add_to_workspace = sp_user.add_sp_to_workspace(admin_pat)

# Generate a PAT token for the Service Principal
sp_user_pat = sp_user.get_pat_token()
sp_user_pat

# Show Service Principals PAT tokens
sp_user.list_pat_tokens()

# List workspace Service Principal users
sp_user.list_principals(admin_pat)

# View workspace groups - this is necesary to get group ids, rather than group names
# for the Service Principal API
sp_user.get_group_id_mapping(admin_pat)

"""
Configure JDBC/ODBC connector
"""

# Host path sources from Databricks Cluster tab for ODBC/JDBC connector
# Example: sql/protocolv1/o/7844645611/000-245641-bari376
host_path = 

query = f"""SHOW DATABASES"""

results = fetch_query(host_path, sp_user_pat, query)
print(results)


# Remove Service Principal user from workspace, passign in Service Principals id
# sourced from Databricks Workspace
sp_user.remove_sp_from_workspace(admin_pat, id='9999999999')
sp_user.list_principals(admin_pat)