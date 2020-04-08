# pylint: disable=import-error
import requests
import os
from logging import warning

user_identity_url = os.environ["APIGATEWAY_URL"]

def get_branch_office_associate_list(email):
    endpoint = f'/ticketai/api/v1/identity/branch_office_associate_list'
    url = f'{user_identity_url}{endpoint}'
    params = {'uuid':email}

    branch_office_associate_list = requests.get(url, params)
    
    warning(branch_office_associate_list.json())

    constraint = {"user_id": {"$in": branch_office_associate_list.json()["data"]["values"]}}
    return constraint