# Copyright (C)2015,2016 International Business Machines Corporation and  
# others. All Rights Reserved.    
.PHONY: all clean 

TOOLKIT_NAME=com.ibm.streamsx.jdbc

ifdef STREAMS_BUILDSERVICE
    $(info Building using buildservice)
    ifeq ($(STREAMS_INSTALL),)
        $(error error: environment variable STREAMS_INSTALL has to be set)
    endif
    TOOLKIT_PATH = $(STREAMS_INSTALL)/toolkits
else
    $(info Building using environment settings)
    ifndef STREAMS_INSTALL
        $(error require streams environment STREAMS_INSTALL)
    endif
    # Fill in STREAMS_JDBC_TOOLKIT location here.
    STREAMS_JDBC_TOOLKIT ?= $(shell  ([ -e "../../$(TOOLKIT_NAME)/toolkit.xml" ] && echo ../../$(TOOLKIT_NAME)) ||\
	         ([ -e "../$(TOOLKIT_NAME)" ] && echo ../$(TOOLKIT_NAME)) ||\
	                 echo $(STREAMS_INSTALL)/toolkits/$(TOOLKIT_NAME))
    TOOLKIT_PATH = $(STREAMS_JDBC_TOOLKIT)
endif

SPLC_FLAGS ?= -a -t $(TOOLKIT_PATH)
SPLC = $(STREAMS_INSTALL)/bin/sc

SPL_CMD_ARGS ?=  
SPL_MAIN_COMPOSITE = com.ibm.streamsx.jdbc.sample.jdbcrun::JDBCRunSample
#SPL_MAIN_COMPOSITE = com.ibm.streamsx.jdbc.sample.jdbcrun::JDBCRunErrorPort

all: distributed

distributed:
	$(SPLC) $(SPLC_FLAGS) -M $(SPL_MAIN_COMPOSITE) $(SPL_CMD_ARGS)

clean: 
	$(SPLC) $(SPLC_FLAGS) -C -M $(SPL_MAIN_COMPOSITE)
	rm -rf output
