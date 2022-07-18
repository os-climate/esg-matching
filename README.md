# esg-matching

The esg-matching is a library that is part of the Entity-Matching project developed by OS-Climate Foundation. 
Its main purpose is to provide methods to match entities from different data sources.

Currently, the library provides three main components:
- a database engine which can connect to a local Sql-lite database or an Oracle database elsewhere
- a file reader which can read data sources in csv format and load its content to a database
- a database-driven matcher which can perform exact matching based on database queries. Three types of matchings are provided: direct, residual and indirect matching.

## Install from PyPi

```
pip install esg-matching
```