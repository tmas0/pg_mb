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


#from db import DbConnection
#from config import ConfigOptions, BROptions
import requests
from auth import auth
from api import api


class database:
    ''' Clusters database management API '''

    def __init__(self):
        ''' Initialitation method '''
        #self.config = ConfigOptions()
        #self.br = BROptions()
        #self.conn = DbConnection(self.config.dbuser, self.config.dbpassword, self.config.dbname, self.config.dbhost, self.config.dbport)
        self.production = 'production'
        self.staging = 'staging'
        self.development = 'development'
        self.daily = 'daily'
        self.weekly = 'weekly'
        self.monthly = 'monthly'

    def close(self):
        ''' Close database connection '''
        self.conn.close()

    def get_business(self):
        ''' Bussiness definitions '''

        try:
            return api.get('business')
        except Exception as e:
            print(e)

        return {}

    def get_clusters(self, cluster):
        ''' Get business clusters '''

        try:
            return api.get('cluster/' + str(cluster))
        except Exception as e:
            print(e)

        return {}

    def get_slave_node(self, cluster):
        ''' Return master host '''
        query = """SELECT n.name || '.' || b.domain
                   FROM dba.node n
                   INNER JOIN dba.cluster c ON c.id = n.cluster_id AND c.name = %s AND c.active IS TRUE
                   INNER JOIN dba.business b ON c.business_id = b.id AND b.active IS TRUE
                   WHERE n.active IS TRUE"""

        nodes = self.conn.query(query, (cluster,))

        for node in nodes:
            node = node[0]
            if self.is_slave(node):
                return node

        return False

    def is_slave(self, node):
        ''' Determine if node is slave '''
        dbcon = DbConnection(self.br.dbuser, self.br.dbpassword, self.br.dbname, node, self.br.dbport)

        is_master = dbcon.query("SELECT pg_is_in_recovery()")

        dbcon.close()

        if not is_master[0][0]:
            return True

        return False

    def get_databases(self, cluster, environment=None):
        ''' Return this cluster databases '''
        if environment is None:
            environment = self.production

        try:
            return api.get('database/' + str(cluster) + '/' + environment)
        except Exception as e:
            print(e)

        return {}

    def get_config(self, business, rule):
        ''' Return configuration '''

        try:
            return api.get('rule/' + str(business) + '/' + rule)
        except Exception as e:
            print(e)

        return {}

    def insert_recovery_state(self, cluster_id, database_id, state, stderr):
        ''' Insert recovery state '''
        self.conn.upsert("INSERT INTO recovery_history (cluster_id, database_id, state, info) VALUES (%s, %s, %s, %s)", (cluster_id, database_id, state, stderr))

    def insert_backup_state(self, cluster_id, database_id, scheduled, state, stderr, dumpsize=0,duration=0):
        ''' Insert backup state '''
        self.conn.upsert("INSERT INTO backup_history (cluster_id, database_id, scheduled, state, info, size, duration) VALUES (%s, %s, %s, %s, %s, %s, %s)", (cluster_id, database_id, scheduled, state, stderr, dumpsize, duration))

    def drop_and_create(self, cluster, database):
        ''' Drop and recreate database '''

        # Make a particular connection.
        dbcon = DbConnection(self.br.dbuser, self.br.dbpassword, self.br.dbname, cluster, self.br.dbport)

        dbcon.query("select pg_terminate_backend(pid) from pg_stat_activity where datname='%s'" % database)

        dbcon.ddl("DROP DATABASE IF EXISTS %s" % database)

        dbcon.ddl("CREATE DATABASE %s OWNER=postgres TEMPLATE=template0" % database)

        dbcon.close()

    def get_backup_status(self, scheduled, state=False, business_id=None):
        ''' Return backup status by business.
            By default, return failed backups from all business. '''

        gap = self.get_interval(scheduled)

        partwhere = "AND bh.timecreated > (now() - interval '1 " + gap + "')"

        if business_id == None:
            query = """SELECT date_trunc('minute', bh.timecreated)
                    , b.name as businessname
                    , c.name as clustername
                    , d.name as dbname
                    , bh.state as status
                    , bh.info as info
                   FROM dba.backup_history bh
                   INNER JOIN dba.cluster c ON c.id = bh.cluster_id AND c.active IS TRUE
                   INNER JOIN dba.business b ON c.business_id = b.id AND b.active IS TRUE
                   INNER JOIN dba.database d ON d.id = bh.database_id AND d.active IS TRUE
                   WHERE bh.state IS %s """ + partwhere
            return self.conn.query(query, (state,))
        else:
            query = """SELECT date_trunc('minute', bh.timecreated)
                    , b.name as businessname
                    , c.name as clustername
                    , d.name as dbname
                    , bh.state as status
                    , bh.info as info
                   FROM dba.backup_history bh
                   INNER JOIN dba.cluster c ON c.id = bh.cluster_id AND c.active IS TRUE
                   INNER JOIN dba.business b ON c.business_id = b.id AND b.active IS TRUE AND b.business_id = %s
                   INNER JOIN dba.database d ON d.id = bh.database_id AND d.active IS TRUE
                   WHERE bh.state IS %s """ + partwhere

        return self.conn.query(query, (business_id, state,))

    def get_recovery_status(self, scheduled, state=False, business_id=None):
        ''' Return recovery status by business.
            By default, return failed recoveries from all business. '''

        gap = self.get_interval(scheduled)

        partwhere = "AND rh.timecreated > (now() - interval '1 " + gap + "')"

        if business_id == None:
            query = """SELECT date_trunc('minute', rh.timecreated)
                    , b.name as businessname
                    , c.name as clustername
                    , d.name as dbname
                    , rh.state as status
                    , rh.info as info
                   FROM dba.recovery_history rh
                   INNER JOIN dba.cluster c ON c.id = rh.cluster_id AND c.active IS TRUE
                   INNER JOIN dba.business b ON c.business_id = b.id AND b.active IS TRUE
                   INNER JOIN dba.database d ON d.id = rh.database_id AND d.active IS TRUE
                   WHERE rh.state IS %s """ + partwhere
            return self.conn.query(query, (state,))
        else:
            query = """SELECT date_trunc('minute', rh.timecreated)
                    , b.name as businessname
                    , c.name as clustername
                    , d.name as dbname
                    , rh.state as status
                    , rh.info as info
                   FROM dba.recovery_history rh
                   INNER JOIN dba.cluster c ON c.id = rh.cluster_id AND c.active IS TRUE
                   INNER JOIN dba.business b ON c.business_id = b.id AND b.active IS TRUE AND b.business_id = %s
                   INNER JOIN dba.database d ON d.id = rh.database_id AND d.active IS TRUE
                   WHERE rh.state IS %s """ + partwhere
            return self.conn.query(query, (business_id, state,))

    def get_interval(self, passed):
        ''' Get interval for SQL query. Internal funcion. '''

        if passed == self.daily:
            return 'day'
        elif passed == self.weekly:
            return 'week'
        elif passed == self.monthly:
            return 'month'
        else:
            return 'day'

    def get_host_from_db(self, environment, database):
        ''' Get cluster host from database '''

        query = """SELECT c.domainprefix || '.' || coalesce(e.domainprefix, '') || c.name || '.' || b.domain AS domain
                   FROM dba.database bd
                   INNER JOIN dba.deployment d ON bd.id = d.database_id AND d.active IS TRUE
                   INNER JOIN dba.environment e ON d.environment_id = e.id AND e.name = %s AND e.active IS TRUE
                   INNER JOIN dba.cluster c ON c.id = d.cluster_id AND c.active IS TRUE
                   INNER JOIN dba.business b ON b.id = c.business_id and b.active IS TRUE
                   WHERE bd.name = %s
                      AND bd.active IS TRUE"""

        host = self.conn.query(query, (environment, database,))

        return host[0][0]
