# esg-matching
The esg-matching is a library that is part of the Entity-Matching project developed by OS-Climate Foundation. 
Its main purpose is to provide methods to match entities from different data sources.

Currently, the library provides three main components:
- a database engine which can connect to a local Sql-lite database or an Oracle database elsewhere
- a file reader which can read data sources in csv format and load its content to a database
- a database-driven matcher which can perform exact matching based on database queries. Three types of matchings are provided: direct, residual and indirect matching.

## How to use the library

The following jupyter notebooks teaches how to use the library:

- [How to read data and load it to a database](notebooks/How-to Guide/Read data and load to a database)
- [How to use the DbEngine to execute SQL statements](notebooks/How-to Guide/Use DbEngine to execute SQL statements)
- [How to perform exact matching](notebooks/How-to Guide/Perform exact matching)
