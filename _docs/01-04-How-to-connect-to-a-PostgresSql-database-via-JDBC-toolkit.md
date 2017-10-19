---
title: "How to connect to a PostgresSql"
permalink: /docs/user/postgressql/
excerpt: "How to connect to PostgresSql."
last_modified_at: 2017-10-19T12:37:48-04:00
redirect_from:
   - /theme-setup/
sidebar:
   nav: "userdocs"
---
{% include toc %}
{% include editme %}

## How to connect to a PostgresSql database via JDBC toolkit
This document describes a step by step instruction to connect to a PostgresSql database via JDBC toolkit.

### 1 Download the PostgresSql jdbc driver
https://jdbc.postgresql.org/download/postgresql-42.1.3.jar
create a opt directory in your SPL project directory
copy the driver jar library **postgresql-42.1.3.jar** in to opt directory

### 2 - Create a database in your postgresql server

login as root

     su - postgres
     -bash-4.1$ psql
     postgres=# CREATE DATABASE STREAMS OWNER postgres;
     postgres=# ALTER USER "postgres" WITH PASSWORD 'postpass';
     # create a sample table in your database
     streams=# CREATE TABLE streamsTable ( personid int, lastname varchar(255), firstname varchar(255), address      varchar(255), city varchar(255));
     # insert some rows into table
     streams=# INSERT INTO streamsTable VALUES (1234, 'myName', 'myFistName', 'Berliner Street', 'Berlin'

### 3 - Create a SPL project in your Streams server


    namespace application;
    use com.ibm.streamsx.jdbc::* ;
    // *******************************************************************************
    // JDBCPostgreSQL demonstrates how to connect to a PostgreSQL database and select data from a table using
    // JDBCRun operator.
    // Required Streams Version = 4.1.x.x
    // Required JDBC Toolkit Version = 1.2.0
    // https://github.com/IBMStreams/streamsx.jdbc
    // PostgreSQL jdbc driver version 42 or higher
    // https://jdbc.postgresql.org/download/postgresql-42.1.3.jar
    // To connect to database, the following parameters need to be specified:
    // jdbcDriverLib : the jdbc driver libraries (download the jdbc driver file from https://jdbc.postgresql.org/download/postgresql-42.1.3.jar
    // and store it in opt folder, e.g. opt/postgresql-42.1.3.jar )
    // jdbcClassName : the class name for PostgreSQL jdbc driver (org.postgresql.Driver)
    // jdbcUrl : the database URL. (e.g. jdbc:postgresql://<your postgresql IP address>/<your database name>)
    // dbcUser :  the database user on whose behalf the connection is being made.
    // jdbcPassword : the user’s password.
    // transactionSize : 2 
    // set the transactionSize > 1 for postgresql database
    // In the SPL sample:
    // "select" operator demonstrates how to run SQL statement from stream attribute via statementAttr parameter
    // In this sample the JDBCRun operator connect to the database and read all rows from test table and 
    // write them into data/output.txt
    // 
    // *******************************************************************************/
    composite JDBCPostgreSQL
    {
	  param

		expression<rstring> $jdbcDriverLib : getSubmissionTimeValue("jdbcDriverLib", "opt/postgresql-42.1.3.jar");
		expression<rstring> $jdbcClassName : getSubmissionTimeValue("jdbcClassName", "org.postgresql.Driver");
		expression<rstring> $jdbcUrl : getSubmissionTimeValue("jdbcUrl", "jdbc:postgresql://192.168.239.160/streams");
		expression<rstring> $jdbcUser : getSubmissionTimeValue("jdbcUser", "postgres");
		expression<rstring> $jdbcPassword : getSubmissionTimeValue("jdbcPassword", "postpass");
		expression<rstring> $statement : getSubmissionTimeValue("statement", "SELECT * from streamsTable");

	type
                // the postgres database deliver the select results with small capital letters
                // lastname is correct and not LastName 
		resultSchema = 	int32 		personid, 
				rstring 	lastname,
				rstring 	firstname,
				rstring	 	address,
				rstring 	city;
	
	graph

	        stream<rstring sql> pulse = Beacon() {
			param
				iterations : 1u ;
				initDelay : 2.0;
			output
				pulse : sql = "SELECT personid, lastname, firstname, address, city FROM streamsTable";

		}

		stream<resultSchema> select = JDBCRun(pulse){
			param
				jdbcDriverLib: $jdbcDriverLib;
				jdbcClassName: $jdbcClassName;
				jdbcUrl: $jdbcUrl;
				jdbcUser: $jdbcUser;
				jdbcPassword: $jdbcPassword;
				statement:  $statement;
                            //  statementAttr:    sql;	
                            // it is possible to get the select statement via input port
                            // or put it direct in statement operator     			
				transactionSize : 2;
		}

		() as SelectSink = FileSink(select)		                                                           
    		{                                                                                      
			logic
			state :
			{
				mutable int64 counter = 0;
			}

			onTuple select :
			{
				printStringLn((rstring)personid + "," + lastname + "," + firstname + "," + address + "," + city);
			}
      			
			param   
	      		file	: "output.txt";                                                                            
				format	: csv ; 
				flush	: 1u;		// flush the output file after 1 tuple 
   		} 	

    }
	


### 4 - Make the SPL application 
create a Makefile

and run make

     .PHONY: all distributed clean 
     JDBC_TOOLKIT_INSTALL = $(STREAMS_INSTALL)/toolkits/com.ibm.streamsx.jdbc
     SPLC_FLAGS ?= -a
     SPLC = $(STREAMS_INSTALL)/bin/sc
     SPL_CMD_ARGS ?= -t $(JDBC_TOOLKIT_INSTALL)
     SPL_MAIN_COMPOSITE = application::JDBCPostgreSQL

    all: distributed

    distributed:
	rm -rf output
	JAVA_HOME=$(STREAMS_INSTALL)/java $(SPLC) $(SPLC_FLAGS) -M $(SPL_MAIN_COMPOSITE) $(SPL_CMD_ARGS) --data-directory data

    clean: 
	$(SPLC) $(SPLC_FLAGS) -C -M $(SPL_MAIN_COMPOSITE)
	rm -rf output


