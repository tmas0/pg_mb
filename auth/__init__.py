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
from requests.auth import HTTPBasicAuth
import os
import sys


class auth:
    ''' Authentication api '''

    def __init__(self):
        self.user = None
        self.password = None
        self.dbrepo_url = None
        self.token = None
        try:
            self.user = os.getenv('DBREPO_USER', 'admin')
            self.password = os.getenv('DBREPO_PASSWORD', '1234')
            self.dbrepo_url = os.getenv('DBREPO_URL', 'localhost:8000')
        except os.error:
            print(
                """User or Password not set.
                   Use export=DBREPO_USER=your_username;
                   export=DBREPO_PASSWORD=your_password"""
            )
            sys.exit(1)

    def get_token(self, force=False):
        ''' Get user token '''
        if self.token is None or force:
            try:
                response = requests.post(
                    self.dbrepo_url + '/api/tokens',
                    auth=HTTPBasicAuth(self.user, self.password)
                )
                self.token = response.json()['token']
            except requests.exceptions.RequestException as e:
                print(e)
                sys.exit(1)

        return self.token
