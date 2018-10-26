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
from database import database
#from backup import Backup
#from recovery import Restore
#from mail import Mail
import sys
import argparse

if sys.version_info < (3, 5):
    raise SystemExit('ERROR: pg_mb needs at least python 3.5 to work')


def pg_batman(db, action):
    """
    Looping all clusters and databases.

    Action:
        0: Backup
        1: Restore
    TODO: Action is not boolean, because in the future, can incrementase the
    actions to execute.
    """

    # Get all business.
    companies = db.get_business()
    for business_id, business in companies:
        # Get backups directory for business configuration.
        backupdir = db.get_configuration(business, 'backupdir')

        # Backup manager.
        bck = Backup(backupdir)

        # Get all PostgreSQL clusters. Return QA domain. Does not change.
        clusters = db.get_clusters(business, db.production)
        for cluster_id, clustername, qadomain in clusters:
            # Get cluster databases.
            databases = db.get_databases(clustername)
            for database_id, database in databases:
                # Determine scheduled backup.
                schedulers = bck.get_scheduler()
                for scheduler in schedulers:
                    if action == 0:
                        # Run backup.
                        bck.dump(
                            scheduler,
                            business,
                            clustername,
                            cluster_id,
                            database,
                            database_id,
                            db
                        )

                    if action == 1:
                        # Get lastest daily backup.
                        backupfile = bck.get_last_backupfile(
                            business,
                            clustername,
                            bck.daily,
                            database
                        )

                        # If haven't backup, not restore.
                        if backupfile is not None:
                            res = Restore()

                            # Clean QA database.
                            db.drop_and_create(qadomain, database)

                            # Import lastest backup.
                            res.import_db(
                                qadomain,
                                cluster_id,
                                database,
                                database_id,
                                backupfile,
                                db
                            )
                        else:
                            db.insert_recovery_state(
                                cluster_id,
                                database_id,
                                False,
                                'Backup file not found'
                            )


def main():
    """
    Backup and you can Restore testing
    """

    p = argparse.ArgumentParser(
        prog='pg_mb',
        description='pg_mb tool. PostgreSQL Backup'
    )

    p.add_argument(
        '-v',
        '--version',
        action='version',
        version='1.0\n\npg_mb'
    )
    p.add_argument(
        '-t',
        '--test',
        help='Test backup on QA server.',
        default=True,
        action='store_true'
    )
    group = p.add_subparsers(help='Particular database backup')
    parser_db = group.add_parser('db', help='Particular database')
    parser_db.add_argument(
        '-c',
        '--cluster',
        required=True,
        help='Cluster name.'
    )
    parser_db.add_argument(
        '-d',
        '--database',
        required=True,
        help='Databases coma separated.'
    )

    options = p.parse_args()

    #testing = options.test

    db = database()

    # Process all backups.
    pg_batman(db, 0)

    # If testing, restore backups.
    #if testing:
    #    pg_batman(db, 1)

    # Send summary backup and recovery mail
    #m = Mail()
    #bck = Backup('')
    #schedulers = bck.get_scheduler()
    #for scheduler in schedulers:
    #    backup_rows = db.get_backup_status(scheduler)
    #    recovery_rows = db.get_recovery_status(scheduler)

    #    if len(backup_rows) > 0 or len(recovery_rows) > 0:
            # Make the email body.
    #        body =  m.prepare2send(backup_rows, recovery_rows)

            # Send email.
    #        m.send(body)

    # Close all database connections.
    #db.close()


if __name__ == '__main__':
    main()
