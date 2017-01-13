/*******************************************************************************
 * Copyright (C) 2015 International Business Machines Corporation
 * All Rights Reserved
 *******************************************************************************/
package com.ibm.streamsx.jdbc;

/* The class for JDBC SQL Status information
 */
public class JDBCSqlStatus {

	// SQL Code
	int sqlCode;
	
	// SQL State
	String sqlState = null;

	public int getSqlCode() {
		return sqlCode;
	}

	public void setSqlCode(int sqlCode) {
		this.sqlCode = sqlCode;
	}

	public String getSqlState() {
		return sqlState;
	}

	public void setSqlState(String sqlState) {
		this.sqlState = sqlState;
	}
	
}
