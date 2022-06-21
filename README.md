# esg-matching

The esg-matching is a library that is part of the Entity-Matching project developed by OS-Climate Foundation. 
Its main purpose is to provide methods to match entities from different data sources.

Currently, the library provides three main components:
- a database engine which can connect to a local Sql-lite database or an Oracle database elsewhere
- a file reader which can read data sources in csv format and load its content to a database
- a database-driven matcher which can perform exact matching based on database queries. Three types of matchings are provided: direct, residual and indirect matching.

## How to use the library

The following jupyter notebooks teaches how to use the library:

- [How to read data and load it to a database](https://github.com/os-climate/esg-matching/tree/main/notebooks/How-to%20Guide/Read%20data%20and%20load%20to%20a%20database)
- [How to use the DbEngine to execute SQL statements](https://github.com/os-climate/esg-matching/tree/main/notebooks/How-to%20Guide/Use%20DbEngine%20to%20execute%20SQL%20statements)
- [How to perform exact matching](https://github.com/os-climate/esg-matching/tree/main/notebooks/How-to%20Guide/Perform%20exact%20matching)

