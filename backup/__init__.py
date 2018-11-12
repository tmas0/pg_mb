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


import datetime as dt
import os, time, errno
import shlex, subprocess
import shutil
from stat import S_ISREG, ST_CTIME, ST_MODE
from api import api


class backup:
    ''' Backup management '''
    daily = 'daily'
    weekly = 'weekly'
    monthly = 'monthly'
    retention_daily = 7
    retention_weekly = 5
    retention_monthly = 12

    def get_scheduler():
        ''' Return list of scheduler '''
        today = dt.datetime.today().weekday()
        dayofmonth = dt.datetime.today().day

        schedulers = []
        schedulers.append(backup.daily)
        if today == 6:
            schedulers.append(backup.weekly)

        if dayofmonth == 1:
            schedulers.append(backup.monthly)

        return schedulers

    def get_path(logger, basedir, cluster, scheduled, dbname):
        ''' Return full backup path '''
        # Make path.
        path = os.path.join(basedir, cluster, scheduled, dbname)
        logger.info('Backup path: %s' % path)

        # If not exists, create.
        if not os.path.exists(path):
            try:
                os.makedirs(path)
            except OSError as exc:  # Guard against race condition
                # If exception is not directory already exists.
                if exc.errno != errno.EEXIST:
                    raise
                else:
                    logger.debug('Directory %s already exists.' % path)
        return path

    def get_date():
        ''' Return date part for backup file '''
        return dt.datetime.now().strftime("%Y-%m-%d_%Hh%Mm")

    def get_filename(scheduled, dbname):
        ''' Return full filename '''
        return scheduled + '_' + dbname + '_' + backup.get_date() + '.sql'

    def get_backupfile(logger, basedir, cluster, scheduled, dbname):
        ''' Return full backup path and file '''
        return os.path.join(
            backup.get_path(
                logger,
                basedir,
                cluster,
                scheduled,
                dbname
            ),
            backup.get_filename(
                scheduled,
                dbname
            )
        )

    def get_oldest_backupfile(logger, cluster, scheduled, dbname, backupdir):
        ''' Return last created backupfile '''
        dirpath = backup.get_path(
            logger,
            backupdir,
            cluster,
            scheduled,
            dbname
        )
        logger.debug('get_oldest_backupfile, Path: %s' % dirpath)

        try:
            mtime = lambda f: os.stat(os.path.join(dirpath, f)).st_mtime
        except OSError:
            logger.critical('Cannot determine mtime')
            pass
        finally:
            mtime = None
        entries = list(sorted(os.listdir(dirpath), key=mtime))

        file = None
        if scheduled == backup.daily and len(entries) > backup.retention_daily:
            file = entries[0]
        if (scheduled == backup.weekly and
                len(entries) > backup.retention_weekly):
            file = entries[0]
        if (scheduled == backup.monthly and
                len(entries) > backup.retention_monthly):
            file = entries[0]

        if file is not None:
            return dirpath + '/' + file

        return None

    def backup_maintenance(logger, cluster, scheduled, dbname, backupdir):
        ''' Backup maintenance '''

        # Get backup file candidate to remove.
        obsolet_backupfile = backup.get_oldest_backupfile(
            logger,
            cluster,
            scheduled,
            dbname,
            backupdir
        )
        logger.debug('Obsolet backup: %s' % obsolet_backupfile)

        if obsolet_backupfile is not None:
            try:
                os.remove(obsolet_backupfile)
            except OSError:
                logger.error('Cannot remove %s file' % obsolet_backupfile)
                pass

    def dump(logger, scheduled, cluster, cluster_id, dbname, database_id, backupdir):
        ''' Dump database of one cluster '''

        # Determine full path and name for backupfile.
        backupfile = backup.get_backupfile(
            logger,
            backupdir,
            cluster,
            scheduled,
            dbname
        )
        logger.info('Backup file: %s' % backupfile)

        # Maintenance backup files.
        backup.backup_maintenance(
            logger,
            cluster,
            scheduled,
            dbname,
            backupdir
        )

        # On make daily backup. Others, copy only file.
        if scheduled == backup.daily:
            # Determine standby node from cluster
            try:
                standby = api.get('standby/' + str(cluster_id))
                logger.debug('Standby node: %s' % standby['data'])
            except Exception as e:
                print(e)

            standby = standby['data']

            # If standby node is down or if standalone topology.
            if not standby:
                standby = cluster

            # Get backup user.
            try:
                backup_user = os.getenv('PGMB_EDBUSER', 'postgres')
                backup_dbport = os.getenv('PGMB_EDBPORT', 5432)
            except os.error:
                print(
                    """User not set.
                       Use export=PGMB_EDBUSER=your_username;"""
                )

            command = 'pg_dump -U%s -h %s -p %s -Fc -d %s -f %s' % (
                backup_user,
                standby,
                backup_dbport,
                dbname,
                backupfile
            )
            logger.debug(command)

            command = shlex.split(command)

            start = dt.datetime.now()

            try:
                ps = subprocess.Popen(
                    command,
                    shell=False,
                    stdin=subprocess.PIPE,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE
                )
            except OSError as err:
                try:
                    data = {}
                    data['cluster_id'] = cluster_id
                    data['database_id'] = database_id
                    data['scheduled'] = scheduled
                    data['state'] = False
                    data['info'] = err
                    response = api.post('backup/logging', data)
                    logger.debug(
                        'Inserted backup error logging: %s' % response
                    )
                except Exception as e:
                    print('Cannot insert error log data')
                    print(e)
                    pass
                pass

            stdoutdata, stderrdata = ps.communicate()

            state = False
            if stderrdata is None or len(stderrdata) == 0:
                state = True
                stderrdata = None

            statinfo = os.stat(backupfile)
            dumpsize = statinfo.st_size

            end = dt.datetime.now()
            difference = end - start
            seconds = difference.total_seconds()

            try:
                data = {}
                data['cluster_id'] = cluster_id
                data['database_id'] = database_id
                data['scheduled'] = scheduled
                data['state'] = True
                data['size'] = dumpsize
                data['duration'] = int(seconds)
                response = api.post('backup/logging', data)
                logger.debug('Inserted backup logging: %s' % response)
            except Exception as e:
                print('Cannot insert log data')
                print(e)
                pass
        else:
            # Get daily lastest backup.
            dailybackupfile = backup.get_oldest_backupfile(
                logger,
                cluster,
                backup.daily,
                dbname,
                backupdir
            )

            try:
                shutil.copyfile(dailybackupfile, backupfile)
                try:
                    data = {}
                    data['cluster_id'] = cluster_id
                    data['database_id'] = database_id
                    data['scheduled'] = scheduled
                    data['state'] = state
                    data['info'] = stderrdata
                    response = api.post('backup/logging',)
                    logger.debug('Inserted backup logging: %s' % response)
                except Exception as err:
                    print(err)
                    pass
            except (IOError, shutil.Error) as e:
                try:
                    data = {}
                    data['cluster_id'] = cluster_id
                    data['database_id'] = database_id
                    data['scheduled'] = scheduled
                    data['state'] = False
                    data['info'] = err
                    response = api.post('backup/logging',)
                    logger.debug('Inserted backup logging: %s' % response)
                except Exception as e:
                    print(e)
                    pass
                pass
