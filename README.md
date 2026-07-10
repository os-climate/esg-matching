[![FINOS - Archived](https://cdn.jsdelivr.net/gh/finos/contrib-toolbox@master/images/badge-archived.svg)](https://community.finos.org/docs/governance/lifecycle-stages/archived)

> [!WARNING]
> **This repository is archived and in a read-only state.**
> You are welcome to download, clone, or fork this code, but please be aware that it is no longer actively maintained and may contain bugs or security vulnerabilities.
>
> **Interested in reviving this project?** If you would like to restore development activities, please contact the team at info@os-climate.org.


> [!IMPORTANT]
> On June 26 2024, Linux Foundation announced the merger of its financial services umbrella, the Fintech Open Source Foundation ([FINOS](https://finos.org)), with OS-Climate, an open source community dedicated to building data technologies, modeling, and analytic tools that will drive global capital flows into climate change mitigation and resilience; OS-Climate projects are in the process of transitioning to the [FINOS governance framework](https://community.finos.org/docs/governance); read more on [finos.org/press/finos-join-forces-os-open-source-climate-sustainability-esg](https://finos.org/press/finos-join-forces-os-open-source-climate-sustainability-esg)

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