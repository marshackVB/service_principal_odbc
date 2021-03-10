
import requests

class CreateServicePrincipalUser():
    """
    A suite of utilities to create a Azure Databricks Service Principal user, generate an AD token for the user,
    then finally generate a PAT token for the user. This PAT token can be used to authenticate as the Service
    Principal user via the JDBC/ODBC connector. Using this technique, table ACLs can be applied to the Service
    Principal user or to a group of which the principal is a member

    Inputs:
    workspace_id: The Databricks workspace ID

    Inputs sources from Azure Active Directory:
    client_id: The client id of the Service Principal
    tenant_id: The tenant id of the Service Principal
    applicaiton_secret: The secret generated for the Service Principal
    display_name: The display name of the service principals

    Other attributes:
    azure_databricks_resource_id: A fixed value that corresponds to the Databricks service in Azure
    host: The URL of the Databricks workspace
    sp_url: The URL used for the SCIM API, which is frequently leveraged by the class
    ad_token_json: Results of AD token API request
    ad_token: The AD token generated for the Service Principal
    sp_user_json: Results of the SCIM API call used to create the Service Principal user
    sp_id: The Service Principals user id in Databricks

    Be sure to grant the Service Principal a Contributor role to the Databricks Workspace via 
    the Acess control (IAM) view of the Workspace in Azure.

    """
    def __init__(self, workspace_id, client_id, tenant_id, application_secret, display_name):
        self.workspace_id = workspace_id
        self.client_id = client_id
        self.tenant_id = tenant_id
        self.application_secret = application_secret
        self.display_name = display_name
        self.azure_databricks_resource_id = '2ff814a6-3304-4ab8-85cb-cd0e6f879c1d'
        self.host = f'https://{workspace_id}.azuredatabricks.net'
        self.sp_url = f"{self.host}/api/2.0/preview/scim/v2/ServicePrincipals"
        self.ad_token_json = None
        self.ad_token = None
        self.sp_user_json = None
        self.sp_id = None


    def add_sp_to_workspace(self, pat_token):
        """
        Add service principal to workspace using the SCIM API. Adding and managing
        Service Principal users must be done via API calls.

        It is possible to add the Service Principal user to one or more Groups
        in the below command by adding this "groups" syntax. Note that the groups
        "value" is not the Group's name; it is the Group's ID which is stored within
        Databricks and can be sourced by the below get_group_id_mapping method.

         "groups":[
                    {
                     "value":"58sdfsdf34"
                    }
                ],

        """

        create_sp_post = {"schemas":[
                            "urn:ietf:params:scim:schemas:core:2.0:ServicePrincipal"
                        ],
                        "applicationId": self.client_id,
                        "displayName":self.display_name,                 
                        "entitlements":[
                            {
                                "value":"allow-cluster-create"
                            }
                        ]
                        }

        headers = {
                "Authorization":f'Bearer {pat_token}',
                "Content-Type":"application/scim+json"
                }

        create_sp_user = requests.post(self.sp_url, headers=headers, json=create_sp_post, verify=True)

        self.sp_user_json = create_sp_user.json() 
        self.sp_id = self.sp_user_json['id']

        return self.sp_user_json


    def get_ad_token(self):
        """
        Get an Azure AD token that can be used to access the Databricks Rest API. This will allow
        The service princiapl user to launch Jobs, etc. It will also allow the Service Principal user
        to generate a PAT token using the PAT token API. The PAT token can then be used to authenticate
        to the JDBC/ODBC connector.
        """

        url = f'https://login.microsoftonline.com/{self.tenant_id}/oauth2/token' 

        header = {
            "Content-Type": "application/x-www-form-urlencoded"
        }

        data = {"grant_type": "client_credentials",
                "client_id": self.client_id,
                "resource": self.azure_databricks_resource_id,
                "client_secret": self.application_secret}

        self.ad_token_json = requests.get(url, headers=header, data=data).json()
        self.ad_token = self.ad_token_json['access_token']

        return self.ad_token


    def get_pat_token(self):
        """
        Get a PAT token using the Token API. Authentication is handled by the AD token previously
        generated
        """

        self.get_ad_token()

        url = f"{self.host}/api/2.0/token/create"

        headers = {
                "Authorization":f'Bearer {self.ad_token}',
                "Content-Type":"application/scim+json"
                }

        data = {
            "comment": "Test Service Principal Token"
            }

        return requests.post(url, headers=headers, json=data).json()['token_value']


    def list_principals(self, pat_token, display_name=None):
        """
        Get all Service Principal users in a workspace
        """

        headers = {
                "Authorization":f'Bearer {pat_token}',
                "Content-Type":"application/scim+json"
                }

        sp_entries = requests.get(self.sp_url, headers=headers, verify=False).json()

        if display_name:
            requested_sp = None
            
            for sp in sp_entries['Resources']:
                if sp.get('displayName', None) == display_name:
                    requested_sp = sp

            if requested_sp:
                return requested_sp
            else:
                print("Service Principal does not exist")

        else:
            return sp_entries


    def remove_sp_from_workspace(self, pat_token, id=None):
        """
        Remove Service Principal users from a workspace
        """

        if id:
            url = f"{self.sp_url}/{id}"
        else:
            url = f"{self.sp_url}/{self.sp_id}"
        

        headers = {
                        "Authorization":f'Bearer {pat_token}',
                        "Accept":"application/scim+json"
                    }

        r = requests.delete(url, headers=headers)
        return r


    def get_group_id_mapping(self, pat_token):
        """
        Get all group and group ids from workspace. In order to add a user to a group using
        the SCIM API, you want source the Group's ID from the workspace.
        """

        url = f'{self.host}/api/2.0/preview/scim/v2/Groups'

        headers = {
                    "Authorization":f'Bearer {pat_token}',
                    "Content-Type":"application/scim+json"
                    }

        all_group_data = requests.get(url, headers=headers).json()['Resources']

        group_id_mapping = {group['displayName']: group['id'] for group in all_group_data}

        return group_id_mapping


    def list_pat_tokens(self):
        """
        List all PAT tokens in the workspace
        """

        if not self.ad_token:
            self.get_ad_token()

        url = f"{self.host}/api/2.0/token/list"

        headers = {
                "Authorization":f'Bearer {self.ad_token}'
                }

        return requests.get(url, headers=headers).json()



