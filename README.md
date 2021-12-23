# OctoSQL MySQL Plugin

This plugin adds MySQL support to OctoSQL.

## Installation

```
octosql plugin install mysql
```

## Configuration

The available configuration variables are:
- host
- port
- user
- password
- database

An example octosql.yml file would be:
```yaml
databases:
  - name: mydb
    type: mysql
    config:
      host: localhost
      port: 3306
      database: mydatabase
      user: myuser
      password: mypassword
```

## Usage

After configuring a database as described above you can use tables from the configured database in your OctoSQL queries:
```
octosql "SELECT * FROM mydb.mytable" --describe
octosql "SELECT COUNT(*) FROM mydb.mytable"
```
