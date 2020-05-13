---
title: "How to connect to a Oracle"
permalink: /docs/user/mssql/
excerpt: "How to connect to MSSQL."
last_modified_at: 2020-05-13T12:37:48-04:00
redirect_from:
   - /theme-setup/
sidebar:
   nav: "userdocs"
---
{% include toc %}
{% include editme %}

# How to connect to a Microsoft SQL database via JDBC toolkit

This document describes a step by step instruction to connect to a Microsoft SQL database via JDBC toolkit and insert/select rows into/from database.

## 1 - Download the Microsoft SQL jdbc driver (sqljdbc_8.2.0.0_enu.tar.gz) from:
https://docs.microsoft.com/en-us/sql/connect/jdbc/download-microsoft-jdbc-driver-for-sql-server?view=sql-server-ver15

And copy `mssql-jdbc-8.2.0.jre11.jar` in `opt` directory of your SPL project.
```
tar -xzvf sqljdbc_8.2.0.0_enu.tar.gz
cp mssql-jdbc-8.2.0.jre11.jar $HOME/workspace/JdbcMssql/opt/
```
## 2 - Create a database in your MS SQL 
Login in your MS SQL sever, start the `sqlcmd` tool and create a database.

```
sqlcmd -U sa -P mssql_password
1> CREATE DATABASE STREAMS
2> GO
```
## 3- Create a SPL project in your Streams server
For example in $HOEM/workspace/JdbcMssql
```
$HOEM/workspace/JdbcMssql/application/JdbcMssql.spl
$HOEM/workspace/JdbcMssql/Makefile
$HOEM/workspace/JdbcMssql/opt/mssql-jdbc-8.2.0.jre11.jar
```
## 4- Create a Mekefile
```
.PHONY: all distributed clean 

JDBC_TOOLKIT_INSTALL = $(STREAMS_INSTALL)/toolkits/com.ibm.streamsx.jdbc

SPLC_FLAGS ?= -a
SPLC = $(STREAMS_INSTALL)/bin/sc

SPL_CMD_ARGS ?= --data-directory=data -t $(JDBC_TOOLKIT_INSTALL)
SPL_MAIN_COMPOSITE = application::JdbcMssql

all: distributed

distributed:
	JAVA_HOME=$(STREAMS_INSTALL)/java $(SPLC) $(SPLC_FLAGS) -M $(SPL_MAIN_COMPOSITE) $(SPL_CMD_ARGS)

clean: 
	$(SPLC) $(SPLC_FLAGS) -C -M $(SPL_MAIN_COMPOSITE)
	rm -rf output
```


## 5- Create a SPL file
The following SPL file demonstrates how to connect to a MS SQL database and insert/select 
data into/from database using JDBCRun operator.

There are 3 ways to specifies the SQL statement in a JDBCRun operator:

1- `statement`

This parameter specifies the value of any valid SQL or stored procedure statement. 

For example:
```
statement : "SELECT * FROM MSSQL_TABLE";
```

2- `statement` and `statementParamAttrs` parameters:

In this case the SQL `statement` has one or several '?' as placeholder.

And the `statementParamAttrs` specifies the value of statement parameters, that coming via Input stream. 

For example:
```
statement : "INSERT INTO MSSQL_TABLE (ID, NAME, AGE) VALUES (?, ?, ?)" ;
statementParamAttrs : "ID, NAME, AGE" ;
```

3- `statementAttr`

This parameter specifies the value of complete SQL or stored procedure statement that is from stream attribute (no parameter markers). 

In the following sample `dropTableStatement` creates a SQL statement in output port and JDBCRun gets 
this SQL statement in input port and forwards it via `statementAttr` parameter to the JDBC driver.

```
      stream<rstring drop_sql> dropTableStatement = Beacon()
        {
            param
                iterations : 1u ;
            output
                dropTableStatement : drop_sql = "DROP TABLE IF EXISTS MSSQL_TABLE";
        }
       stream<resultSchema> dropTable = JDBCRun(dropTableStatement)
         {
            param
                ...
                ...
                statementAttr : drop_sql;
        }
```

## SPL file
```
/*******************************************************************************
* Copyright (C) 2020 International Business Machines Corporation
* All Rights Reserved
*******************************************************************************/

/*******************************************************************************
 * JdbcMssql demonstrates how to connect to a MS SQL database and insert/select 
 * data into/from database using JDBCRun operator.
 * 
 * To connect to database, the following 5 parameters needed to be specified:
 * 1- The jdbc driver library (download the jdbc driver and store it in opt folder:
 * jdbcDriverLib: "opt/mssql-jdbc-8.2.0.jre8.jar";
 *
 * 2- The class name for mssql jdbc driver
 * jdbcClassName: "com.microsoft.sqlserver.jdbc.SQLServerDriver"
 *
 * 3- The database URL. (jdbc:sqlserver://<server:port;databaseName=db_name)
 * e.g.
 * jdbcUrl: "jdbc:sqlserver://my_mssql.ibm.com:1433;databaseName=STREAMS;"
 *
 * 4- The database user on whose behalf the connection is being made.
 * e.g.
 * jdbcUser: "sa";
 *
 * 5- The database user’s  password .
 * e.g.
 * jdbcPassword: "mssql_password"
 * 
 * In the SPL sample:
 * "dropTable" drop table if exists.
 * "createTable" create a new table via statementAttr parameter.
 * "insert" operator demonstrates how to run SQL statement with parameter markers
 * via statement/statementParamAttrs parameters.
 * "select" operator demonstrates how to run SQL statement from stream attribute via 
 * statement/statementParamAttrs parameters.
 * 
 *******************************************************************************/
namespace application ;

use com.ibm.streamsx.jdbc::* ;
use com.ibm.streamsx.jdbc.types::* ;

composite JdbcMssql
{

    param
        expression<rstring> $jdbcDriverLib : getSubmissionTimeValue("jdbcDriverLib", "opt/mssql-jdbc-8.2.0.jre8.jar") ;
        expression<rstring> $jdbcClassName : getSubmissionTimeValue("jdbcClassName", "com.microsoft.sqlserver.jdbc.SQLServerDriver") ;
        expression<rstring> $jdbcUrl : getSubmissionTimeValue("jdbcUrl", "jdbc:sqlserver://mymssql.ibm.com:1433;databaseName=STREAMS;") ;
        expression<rstring> $jdbcUser : getSubmissionTimeValue("jdbcUser", "sa") ;
        expression<rstring> $jdbcPassword : getSubmissionTimeValue("jdbcPassword", "mssql_password") ;

    type
        insertSchema = int32 ID, rstring NAME, int32 AGE ;
        selectSchema = int32 ID, rstring NAME, int32 AGE ;
        resultSchema = rstring RESULT;

    graph
 
        stream<rstring drop_sql> dropTableStatement = Beacon()
        {
            param
                iterations : 1u ;
                initDelay : 1.0;
            output
                dropTableStatement : drop_sql = "DROP TABLE IF EXISTS MSSQL_TABLE";
        }

        stream<resultSchema> dropTable = JDBCRun(dropTableStatement)
        {
            param
                jdbcDriverLib : $jdbcDriverLib ;
                jdbcClassName : $jdbcClassName ;
                jdbcUrl : $jdbcUrl ;
                jdbcUser : $jdbcUser ;
                jdbcPassword : $jdbcPassword ;
                statementAttr : drop_sql;
        }



        stream<rstring drop_sql> createTableStatement = Beacon()
        {
            param
                iterations : 1u ;
                initDelay : 3.0;
            output
                createTableStatement : create_sql = "CREATE TABLE MSSQL_TABLE (ID INTEGER, NAME CHAR(20), AGE INTEGER)";
        }

        stream<resultSchema> createTable = JDBCRun(createTableStatement)
         {
            param
                jdbcDriverLib : $jdbcDriverLib ;
                jdbcClassName : $jdbcClassName ;
                jdbcUrl : $jdbcUrl ;
                jdbcUser : $jdbcUser ;
                jdbcPassword : $jdbcPassword ;
                statementAttr : create_sql;
        }



        stream<insertSchema> createData = Beacon()
        {
            param
                iterations : 30u ;
                initDelay : 5.0;
            output
                createData : ID =(int32) IterationCount(), 
                            NAME ="NAME_" + (rstring)((int32) IterationCount() * 2),
                            AGE =((int32) IterationCount() * 4) ;
        }

        () as Print_creteData = Custom(createData)
        {
            logic
                onTuple createData : printStringLn("Created data : " + (rstring)createData);
        }

        stream<insertSchema> insert = JDBCRun(createData)
        {
            param
                jdbcDriverLib : $jdbcDriverLib ;
                jdbcClassName : $jdbcClassName ;
                jdbcUrl : $jdbcUrl ;
                jdbcUser : $jdbcUser ;
                jdbcPassword : $jdbcPassword ;
                statement : "INSERT INTO MSSQL_TABLE (ID, NAME, AGE) VALUES (?, ?, ?)" ;
                statementParamAttrs : "ID, NAME, AGE" ;
        }


        
        stream<selectSchema> creteSelectStreams = Beacon()
        {
            param
                iterations : 30u ;
                initDelay : 15.0;
            output
                creteSelectStreams : ID =(int32) IterationCount(), 
                            NAME ="NAME_" + (rstring)((int32) IterationCount() * 2),
                            AGE =((int32) IterationCount() * 4) ;
        }

        stream<selectSchema> select = JDBCRun(creteSelectStreams)
        {
            param
                jdbcDriverLib : $jdbcDriverLib ;
                jdbcClassName : $jdbcClassName ;
                jdbcUrl : $jdbcUrl ;
                jdbcUser : $jdbcUser ;
                jdbcPassword : $jdbcPassword ;
                statement : "SELECT ID, NAME, AGE FROM MSSQL_TABLE WHERE ID=? OR NAME = ? OR AGE = ?" ;
                statementParamAttrs : "ID, NAME, AGE" ;
        }


        () as PrintSelectedRows = Custom(select)
        {
            logic
                onTuple select : printStringLn("Selected rows " +(rstring) select) ;
        }

  
    }
```


