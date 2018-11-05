# -*- coding: utf-8 -*-
#!/usr/bin/env python
#
# Copyright (C) 2018 Toni Mas <antoni.mas@gmail.com>
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.


import requests
from auth import auth


class api:
    ''' Calls to dbrepo API '''

    @staticmethod
    def get(key):
        a = auth()
        mytoken = a.get_token()

        try:
            response = requests.get(
                a.dbrepo_url + '/api/' + key,
                headers={
                    'authorization': 'bearer ' + mytoken,
                    'content-type': "application/json"
                }
            )
            return response.json()
        except requests.exceptions.RequestException as e:
            print(e)

        return {}

    @staticmethod
    def post(key, data):
        a = auth()
        mytoken = a.get_token()

        try:
            response = requests.post(
                a.dbrepo_url + '/api/' + key,
                headers={
                    'authorization': 'bearer ' + mytoken,
                    'content-type': "application/json"
                },
                data=data
            )
            return response.json()
        except requests.exceptions.RequestException as e:
            print(e)

        return {}
