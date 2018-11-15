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


from database import database
from backup import backup
import sys
import argparse
import logging

if sys.version_info < (3, 5):
    raise SystemExit('ERROR: pg_mb needs at least python 3.5 to work')


def pg_mb(logger, action):
    """
    Looping all clusters and databases.

    Action:
        0: Backup
        1: Restore
    TODO: Action is not boolean, because in the future, can incrementase the
    actions to execute.
    """

    # Get all business.
    companies = database.get_business()
    logger.debug('Get all business')
    for business_id, business in companies['data']:
        logger.info('Process business: %s - %s' % (business_id, business))

        # Get backups directory for business configuration.
        backupdir = database.get_config(business_id, 'backupdir')
        backupdir = backupdir['data'][0] + '/' + business
        logger.info('Backup root directory: %s' % backupdir)

        # Get all PostgreSQL clusters. Return QA domain. Does not change.
        clusters = database.get_clusters(business_id)
        logger.debug('Get all clusters')
        for cluster_id, clustername in clusters['data']:
            logger.info('Process cluster: %s - %s' % (cluster_id, clustername))

            # Get cluster databases.
            logger.debug('Get all databases')
            databases = database.get_databases(cluster_id, database.production)
            for database_id, dbname in databases['data']:
                logger.info(
                    'Process database: %s - %s' % (database_id, dbname)
                )

                # Determine scheduled backup.
                logger.debug('Get schedulers')
                schedulers = backup.get_scheduler()
                for scheduler in schedulers:
                    logger.info('%s backup' % (scheduler.capitalize()))
                    if action == 0:
                        # Run backup.
                        backup.dump(
                            logger,
                            scheduler,
                            clustername,
                            cluster_id,
                            dbname,
                            database_id,
                            backupdir
                        )

        #            if action == 1:
        #                # Get lastest daily backup.
        #                backupfile = bck.get_last_backupfile(
        ##                    business,
        #                    clustername,
        #                    bck.daily,
        #                    database
        #                )

                        # If haven't backup, not restore.
        #                if backupfile is not None:
        #                    res = Restore()

                            # Clean QA database.
        #                    db.drop_and_create(qadomain, database)

                            # Import lastest backup.
        #                    res.import_db(
        #                        qadomain,
        #                        cluster_id,
        #                        database,
        #                        database_id,
        #                        backupfile,
        #                        db
        #                    )
        #                else:
        #                    db.insert_recovery_state(
        #                        cluster_id,
        #                        database_id,
        #                        False,
        #                        'Backup file not found'
        #                    )


def pg_cb(logger, cluster, db):
    """
    Custom backup: One cluster and one database.
    """

    # Verify cluster and database.
    data = database.verify(cluster, db)
    logger.debug('Verify result: %s' % data)
    if bool(data) or data is not None:

        # Get backup directory for business configuration.
        backupdir = database.get_config(data['business']['id'], 'backupdir')
        backupdir = backupdir['data'][0] + '/' + data['business']['name']
        logger.info('Backup root directory: %s' % backupdir)

        # Dump in special dir.
        custom_dir = 'manual_backup'
        logger.info(
            'Start manual backup: Database %s; Cluster: %s'
            % (db, cluster)
        )
        # Run backup.
        backup.dump(
            logger,
            custom_dir,
            cluster,
            data['cluster']['id'],
            db,
            data['database']['id'],
            backupdir
        )
        logger.info('Backup done')

    else:
        logger.error('Cluster or database cannot exists.')


def main():
    """
    Backup and you can Restore testing
    """

    p = argparse.ArgumentParser(
        prog='pg_mb',
        description='pg_mb tool. PostgreSQL Backup'
    )

    p.add_argument(
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
    p.add_argument(
        '-v',
        '--verbose',
        action="count",
        help='Verbose mode.',
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

    # Create logger
    logger = logging.getLogger('pg_mb')
    ch = logging.StreamHandler()
    if options.verbose is None:
        logger.setLevel(logging.ERROR)
        ch.setLevel(logging.ERROR)
    elif options.verbose == 1:
        logger.setLevel(logging.WARNING)
        ch.setLevel(logging.WARNING)
    elif options.verbose == 2:
        logger.setLevel(logging.INFO)
        ch.setLevel(logging.INFO)
    else:
        logger.setLevel(logging.DEBUG)
        ch.setLevel(logging.DEBUG)

    # Create formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    # Add formatter to ch
    ch.setFormatter(formatter)

    # Add ch to logger
    logger.addHandler(ch)
    #testing = options.test

    # Backup a particular database.
    if hasattr(options, 'database') is False:
        # Process all backups.
        pg_mb(logger, 0)
    else:
        pg_cb(logger, options.cluster, options.database)

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
