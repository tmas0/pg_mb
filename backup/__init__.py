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


class backup:
    ''' Backup management '''
    daily = 'daily'
    weekly = 'weekly'
    monthly = 'monthly'

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

    def get_path(basedir, business, cluster, scheduled, dbname):
        ''' Return full backup path '''
        # Make path.
        path = os.path.join(basedir, business, cluster, scheduled, dbname)

        # If not exists, create.
        if not os.path.exists(path):
            try:
                os.makedirs(path)
            except OSError as exc:  # Guard against race condition
                if exc.errno != errno.EEXIST:
                    raise
        return path

    def get_backupfile(basedir, business, cluster, scheduled, dbname):
        ''' Return full backup path and file '''
        return os.path.join(
            backup.get_path(
                basedir,
                business,
                cluster,
                scheduled,
                dbname
            ),
            self.get_filename(
                scheduled,
                dbname
            )
        )

    def dump(scheduled, business, cluster, cluster_id, dbname, database_id, dbconn):
        ''' Dump database of one cluster '''

        # Determine full path and name for backupfile.
        backupfile = backup.get_backupfile(
            business,
            cluster,
            scheduled,
            dbname
        )

        # Maintenance backup files.
        self.backup_maintenance(business, cluster, scheduled, dbname)

        # On make daily backup. Others, copy only file.
        if scheduled == self.daily:
            # Determine standby node from cluster
            standby = dbconn.get_slave_node(cluster)
            # If standby node is down or if standalone topology.
            if not standby:
                standby = cluster

            command = 'pg_dump -U%s -h %s -p %s -Fc -d %s -f %s' % (self.backup.dbuser, standby, self.backup.dbport, dbname, backupfile)

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
            except OSError as e:
                dbconn.insert_backup_state(cluster_id, database_id, scheduled, False, e)
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

            dbconn.insert_backup_state(cluster_id, database_id, scheduled, state, stderrdata, dumpsize,int(seconds))
        else:
            # Get daily lastest backup.
            dailybackupfile = self.get_oldest_backupfile(business, cluster, self.daily, dbname)
            try:
                shutil.copyfile(dailybackupfile, backupfile)
                dbconn.insert_backup_state(cluster_id, database_id, scheduled, state, stderrdata)
            except (IOError, shutil.Error) as e:
                dbconn.insert_backup_state(cluster_id, database_id, scheduled, False, e)
                pass
