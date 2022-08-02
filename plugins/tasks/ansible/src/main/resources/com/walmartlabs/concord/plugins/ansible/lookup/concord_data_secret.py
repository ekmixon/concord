from __future__ import (absolute_import, division, print_function)

__metaclass__ = type

from ansible.errors import AnsibleError
from ansible.plugins.lookup import LookupBase

import os
import requests

class LookupModule(LookupBase):

    def run(self, terms, variables, **kwargs):

        argsLen = len(terms)
        if argsLen < 2:
            raise AnsibleError('Invalid lookup format. Expected: [orgName], secretName, storePassword')
        elif argsLen < 3:
            orgName = os.environ['CONCORD_CURRENT_ORG_NAME']
            secretName = terms[0]
            storePassword = terms[1]
        else:
            orgName = terms[0]
            secretName = terms[1]
            storePassword = terms[2]

        concordBaseUrl = os.environ['CONCORD_BASE_URL']
        concordInstanceId = os.environ['CONCORD_INSTANCE_ID']
        concordSessionToken = os.environ['CONCORD_SESSION_TOKEN']

        headers = {
            'X-Concord-SessionToken': concordSessionToken,
            'User-Agent': f'ansible (txId: {concordInstanceId})',
        }

        url = f'{concordBaseUrl}/api/v1/org/{orgName}/secret/{secretName}/data'
        multipartInputDict = {}
        if storePassword is not None:
            multipartInputDict = {'storePassword' : storePassword}
        else:
            # this to make sure that multipart header of post request is correctly set
            multipartInputDict = {'':''}

        r = requests.post(url, headers=headers, files=multipartInputDict)

        if r.status_code == requests.codes.not_found:
            raise AnsibleError(f'Secret {orgName}/{secretName} not found')

        if r.status_code != requests.codes.ok:
            resp = self.get_json(r)

            msg = f'Error accessing secret {orgName}/{secretName}: '
            if resp:
                try:
                    raise AnsibleError(msg + resp[0]['message'])
                except (IndexError, KeyError, TypeError):
                    pass

            if r.text:
                raise AnsibleError(msg + r.text)

            raise AnsibleError(f'{msg}Invalid server response: {str(r.status_code)}')

        return [str(r.text)]

    def get_json(self, r):
        try:
            return r.json()
        except ValueError:
            # no JSON returned
            return
