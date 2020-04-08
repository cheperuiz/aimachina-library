# pylint: disable=import-error
import requests
import os
from logging import warning

kratos_public_url = os.environ["KRATOS_PUBLIC_URL"]
kratos_admin_url = os.environ["KRATOS_ADMIN_URL"]


def get_kratos_user_identity(request, type='email'):
    ory_kratos_session_string = request.headers.get('Cookie')

    endpoint = f'/sessions/whoami'
    headers = {
        'Cookie': ory_kratos_session_string
    }
    url = f'{kratos_public_url}{endpoint}'

    kratos_user_identity = requests.get(url, headers=headers)

    return kratos_user_identity.json()["identity"]["traits"]["email"] if type == 'email' else kratos_user_identity.json()["identity"]["id"]