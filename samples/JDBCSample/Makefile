# Copyright (C)2015, International Business Machines Corporation and  
# others. All Rights Reserved.                                
.PHONY: all clean

STREAMS_JDBC_TOOLKIT ?= ../../com.ibm.streamsx.jdbc:${STREAMS_INSTALL}/toolkits/com.ibm.streamsx.jdbc

SPLC_FLAGS ?= -a --data-directory data -t ${STREAMS_JDBC_TOOLKIT}

SPLC = $(STREAMS_INSTALL)/bin/sc

SPL_CMD_ARGS ?=
SPL_COMPOSITE = com.ibm.streamsx.jdbc.sample.jdbcrun::JDBCRunSample
OUTPUT_DIR = output

all: distributed

data:
    mkdir data

standalone: data
	$(SPLC) $(SPLC_FLAGS) -T -M $(SPL_COMPOSITE) $(SPL_CMD_ARGS)

distributed: data
	$(SPLC) $(SPLC_FLAGS) -M $(SPL_COMPOSITE) --output-dir ./$(OUTPUT_DIR) $(SPL_CMD_ARGS)
   
clean:
	$(SPLC) $(SPLC_FLAGS) -C -M $(SPL_COMPOSITE)
	rm data/*.output


