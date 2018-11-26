# pg_mb
Custom backup tool. Not PITR

**Backup tool who needs dbrepo for working**

## Installation

git clone https://github.com/tmas0/pg_mb.git

## Configuration

Set virtual environments variables
* DBREPO_URL
* DBREPO_USER
* DBREPO_PASSWORD
* PGMB_EDBUSER

## Usage

Backup all your databases:
```
/usr/bin/python3.5 /root/pg_mb/pg_mb.py
```

Manual backup
```
/usr/bin/python3.5 /root/pg_mb/pg_mb.py db -c <your_cluster> -d <your_database>
```