# pylint: disable=import-error
import asyncio
import aiohttp
import os

kratos_public_url = os.environ["KRATOS_PUBLIC_URL"]
kratos_admin_url = os.environ["KRATOS_ADMIN_URL"]


async def get_sessions_whoami(ory_kratos_session_string):
    endpoint = f'/sessions/whoami'
    headers = {
        'Cookie': ory_kratos_session_string
    }
    url = f'{kratos_public_url}{endpoint}'
    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=headers) as response:
            data = await response.json()
            return data

def get_kratos_user_identity(request, type='email'):
    ory_kratos_session_string = request.headers.get('Cookie')

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    kratos_user_identity = loop.run_until_complete(get_sessions_whoami(ory_kratos_session_string))
    return kratos_user_identity["identity"]["traits"]["email"] if type == 'email' else kratos_user_identity["identity"]["id"]