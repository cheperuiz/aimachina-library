# pylint: disable=import-error
import requests
import os
from logging import warning

user_identity_url = os.environ["APIGATEWAY_URL"]

def get_branch_office_associate_list(user_id):
    if user_id == 'root':
        return {}
    endpoint = f'/ticketai/api/v1/identity/branch_office_associate_list'
    url = f'{user_identity_url}{endpoint}'
    params = {'uuid':user_id}

    branch_office_associate_list = requests.get(url, params)
    constraint = {"user_id": {"$in": branch_office_associate_list.json()["data"]["values"]}}
    return constraint