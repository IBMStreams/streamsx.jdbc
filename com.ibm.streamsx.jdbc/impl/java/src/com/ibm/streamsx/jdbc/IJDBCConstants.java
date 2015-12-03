/*******************************************************************************
 * Copyright (C) 2015 International Business Machines Corporation
 * All Rights Reserved
 *******************************************************************************/
package com.ibm.streamsx.jdbc;

public interface IJDBCConstants {
	
	// Definition on JDBC transaction isolation level
	public static final String TRANSACTION_READ_UNCOMMITTED = "TRANSACTION_READ_UNCOMMITTED";
	public static final String TRANSACTION_READ_COMMITTED = "TRANSACTION_READ_COMMITTED";
	public static final String TRANSACTION_REPEATABLE_READ = "TRANSACTION_REPEATABLE_READ";
	public static final String TRANSACTION_SERIALIZABLE = "TRANSACTION_SERIALIZABLE";

	// Definition on sqlFailureAction
	public static final String SQLFAILURE_ACTION_LOG = "log";
	public static final String SQLFAILURE_ACTION_ROLLBACK = "rollback";
	public static final String SQLFAILURE_ACTION_TERMINATE = "terminate";
	
	// Definition on reconnection
	public static final String RECONNPOLICY_NORETRY = "NoRetry";
	public static final String RECONNPOLICY_BOUNDEDRETRY = "BoundedRetry";
	public static final String RECONNPOLICY_INFINITERETRY = "InfiniteRetry";
	public static final int RECONN_BOUND_DEFAULT = 5;
	public static final double RECONN_INTERVAL_DEFAULT = 10;
	
	// SQL Error Code and SQLState default value
	public static final int SQL_ERRORCODE_SUCCESS = 0;
	public static final String SQL_STATE_SUCCESS = "00000";

}
