# JXT & JXT+ & JXT++

This repo implements the join query schemes, for example, JXT [1], JXT+ and JXT++, and shows the constructions and comparison of them. 

## Overview

- [Background](#background)
- [Features](#features)
- [Installation](#installation)
  - [Prerequisites](#prerequisites (Need Internet))
  - [Clone Repository](#clone-repository (Need Internet))
  - [Build](#build (Need Internet))
  - [Test](#Test (No need Internet))
- [Project structure](#Project structure)
  - [File tree](#File tree)
  - [Data Description](#Data Description)
- [Validate Results](#Validate Results)
- [Reuse Beyond the Paper ](#Reuse Beyond the Paper )
- [Contact Information](#contact-information)

## Background

The corresponding works i.e., JXT, JXT+, JXT++, solve the join query over encrypted database. The project aims to implement JXT, JXT+ and JXT++ by JAVA, and show the comparison of their storage overhead and query efficiency. 

## Features

- Applied Cryptography
- Encrypted Database
- Join Queries
- JAVA

## Installation

### Prerequisites (Need Internet)

- A computer (Recommended configuration 16 GB RAM)
- JDK version 1.8.0
- Maven 3
- Recommended an IDE (IntelliJ IDEA), which will save plenty of time.

### Clone Repository (Need Internet)

Download source code from [Scalable Equi-Join Queries over Encrypted Database (zenodo.org)](https://zenodo.org/records/12773647).

### Build (Need Internet)

Build the project using Maven:

```
mvn clean install
```

### Test (No need Internet)

Test to run the java files

```
src/test/java/JXT.java
src/test/java/JXTp.java
src/test/java/JXTpp.java
```

## Project structure

### File tree

```
MJXT/
├── pom.xml
├── README.md
├── data																								
├──	├── *										//Datasets
├── src
│   ├── main
│   │   └── java
│   │       ├── client																	
│   │       │   ├── Setup_JXT.java				//Setup algorithm of JXT
│   │       │   ├── Setup_JXTp.java				//Setup algorithm of JXT+
│   │       │   └── Setup_JXTpp.java			//Setup algorithm of JXT++
│   │       ├── server
│   │       │   ├── Server_JXT.java				//Search algorithm of JXT
│   │       │   ├── Server_JXTp.java			//Search algorithm of JXT+
│   │       │   ├── Server_JXTpp.java			//Search algorithm of JXT++
│   │       │   ├── Server_MJXT.java			//Search algorithm of MJXT
│   │       │   └── Server_MJXTp.java			//Search algorithm of MJXT+
│   │       └── utils
│   └── test
│       └── java
│           ├── JXT.java						//JXT scheme
│           ├── JXTp.java						//JXT+ scheme
│           ├── JXTpp.java						//JXT++ scheme
│           ├── M_JXT.java						//MJXT scheme
│           ├── M_JXTp.java						//MJXT+ scheme
│           ├── Table_Gen.java					//Generate tables
│           ├── Table_Gen_Entropy.java			//Generate tables with diff entropy
│           ├── Table_Gen_Lmax.java				//Generate tables with diff Lmax
│           ├── test_JXT.java					//batch test JXT scheme
│           ├── test_JXTp.java					//batch test JXT+ scheme
│           ├── test_JXTpp.java					//batch test JXT++ scheme
│           ├── test_JXTpp_Lmax.java			//batch test JXT++ scheme for diff Lmax
│           ├── test_Setup.java					//test setup time for three schemes
│           └── test_storage.java				//test storage overhead for three schemes
└── target (generated after build)
```

### Data Description

The tables have the naming rules, for example "table1_k5_j5_65536.csv" where "1" denotes the table index, "k5" and "j5" means that there are 5 columns (as attributes, not join attributes) and 5 columns (as join attributes), "65536" means the number of records. Particularly, there are some special tables, for example, "table1_k9_j1_65536_12.csv" where "12" denotes the entropy of join-attribute corresponding to queried attribute value is 12; "table1_k9_j1_65536_Lmax100.csv" where "Lmax100" means the Lmax for the join attribute is 100.

Note that all tables for experiments have been generated, and there is no need to generate new table for result. 

For "table\*\_k\*\_j\*_65536.csv", they have follwoing feature

|   Attribute value   | # records | Number of query result |
| :-----------------: | :-------: | :--------------------: |
| table1_keyword_0_0  |   1000    |          1000          |
| table1_keyword_0_1  |   2000    |          2000          |
| table1_keyword_0_2  |   3000    |          3000          |
|       ......        |  .......  |         ......         |
| table1_keyword_0_8  |   9000    |          9000          |
| table1_keyword_0_9  |   10000   |         10000          |
| table1_keyword_0_10 |   1000    |          100           |
| table1_keyword_0_11 |   1000    |          200           |
| table1_keyword_0_12 |   1000    |          300           |
|       ......        |  ......   |         ......         |
| table1_keyword_0_18 |   1000    |          900           |
| table1_keyword_0_19 |   1000    |          1000          |

For "table1_k9_j1_65536_\*.csv", they have follwoing feature

|        Table name         |  Attribute value   | # records | # occurrence for each join attribute |
| :-----------------------: | :----------------: | :-------: | :----------------------------------: |
| table1_k9_j1_65536_12.csv | table1_keyword_0_0 |    256    |                  16                  |
| table1_k9_j1_65536_14.csv | table1_keyword_0_0 |    256    |                  4                   |
| table1_k9_j1_65536_16.csv | table1_keyword_0_0 |    256    |                  1                   |

For "table1_k9_j1_65536_Lmax100.csv", they have follwoing feature

|  Attribute value   | # records | Number of query result | # occurrence for each join attribute |
| :----------------: | :-------: | :--------------------: | :----------------------------------: |
| table1_keyword_0_0 |   1000    |          1000          |                 100                  |

## Validate Results

The output of the experiments will validate the following claims:

- Figure 1 : run `src/test/java/test_storage.java` and set `join_column` from `1` to `5`.
- Figure 2 and Figure 3 : run `src/test/java/test_JXT.java`, `src/test/java/test_JXTp.java` and `src/test/java/test_JXTpp.java`, you will get lines for JXT, JXT+, JXT++. Note that the result of `keyword0`-`keyword9` belongs to Figure 2 which corresponds to its x-axis `1000`-`10000` , and `keyword10`-`keyword19` belongs to Figure 3 which corresponds to its x-axis `10%`-`100%`.
- Figure 4 : run `src/test/java/test_Setup.java` set `join_column` from `1` to `5`.
- Figure 5 :  run `src/test/java/JXTpp.java`, `src/test/java/M_JXT.java`, `src/test/java/M_JXTp.java` and set `table_num` from `2` to `6`.
- Figure 6 : run `src/test/java/test_JXTpp_Lmax.java`.
- Table 3 : run `src/test/java/test_storage.java` and set `join_column = 1`, set `condition` as `_16,_14,_12`.

## Reuse Beyond the Paper 

You can reuse the project by exchanging the dataset which you want to setup and search, but notice that you need to change the variants `key_colnum,join_column,record_num` and the path of your dataset (recommended that put your dataset in `/data/`).

## Contact Information

You can reach us at:

- Author: Kai Du, Jianfeng Wang
- Email: [KaiDu@xidian.edu.cn](mailto:KaiDu@xidian.edu.cn), [jfwang@xidian.edu.cn](mailto:jfwang@xidian.edu.cn)

# Reference

[1] Charanjit Jutla and Sikhar Patranabis. 2022. Efficient searchable symmetric encryption for join queries. In International Conference on the Theory and Application of Cryptology and Information Security. Springer, 304–333.