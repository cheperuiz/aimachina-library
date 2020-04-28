# pylint: disable=import-error
import requests
from os import environ
from logging import warning

kratos_public_url = environ["KRATOS_PUBLIC_URL"]
kratos_admin_url = environ["KRATOS_ADMIN_URL"]

def get_kratos_user_identity(request, type='email'):
    if 'ory_kratos_session' in request.cookies:
        ory_kratos_session_token = request.cookies.get('ory_kratos_session')
        endpoint = f'/sessions/whoami'
        headers = {
            'Cookie': f'ory_kratos_session={ory_kratos_session_token}'
        }
        url = f'{kratos_public_url}{endpoint}'

        kratos_user_identity = requests.get(url, headers=headers)
        return kratos_user_identity.json()["identity"]["traits"]["email"] if type == 'email' else kratos_user_identity.json()["identity"]["id"]
    elif environ['ENVIRONMENT'] == 'development':
        return environ['DEVELOPMENT_USER_ID']
    else:
        return 400