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


from api import api


class database:
    ''' Clusters database management API '''

    production = 'production'
    staging = 'staging'
    development = 'development'
    daily = 'daily'
    weekly = 'weekly'
    monthly = 'monthly'

    def get_business():
        ''' Bussiness definitions '''

        try:
            return api.get('business')
        except Exception as e:
            print(e)

        return {}

    def get_clusters(cluster):
        ''' Get business clusters '''

        try:
            return api.get('cluster/' + str(cluster))
        except Exception as e:
            print(e)

        return {}

    def get_databases(cluster, environment=None):
        ''' Return this cluster databases '''
        if environment is None:
            environment = database.production

        try:
            return api.get('database/' + str(cluster) + '/' + environment)
        except Exception as e:
            print(e)

        return {}

    def get_config(business, rule):
        ''' Return configuration '''

        try:
            return api.get('rule/' + str(business) + '/' + rule)
        except Exception as e:
            print(e)

        return {}

    def verify(cluster, database):
        ''' Verify cluster and database exists. Return business'''
        try:
            return api.get(
                'database/verify/' +
                str(cluster) +
                '/' +
                str(database)
            )
        except Exception as e:
            print(e)

        return {}
