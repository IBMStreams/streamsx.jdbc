/*******************************************************************************
 * Copyright (C) 2015 International Business Machines Corporation
 * All Rights Reserved
 *******************************************************************************/
package com.ibm.streamsx.jdbc;

import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.concurrent.TimeUnit;


/* This class contains all the JDBC connection related information, 
 * creating maintaining and closing a connection to the JDBC driver
 * Execute, commit and rollback SQL statement
 */
public class JDBCConnectionHelper {
	
	// JDBC connection
	private Connection connection = null;
	
	// the class name for jdbc driver.
	private String jdbcClassName;
	// the database url, jdbc:subprotocol:subname.
	private String jdbcUrl;
	// the database user on whose behalf the connection is being made.
	private String jdbcUser = null;
	// the user's password.
	private String jdbcPassword = null;
	// the jdbc properties file.
	private String jdbcProperties = null;
	// the transaction isolation level at which statement runs.
	private String isolationLevel = IJDBCConstants.TRANSACTION_READ_UNCOMMITTED;
	// transactions are automatically committed or not.
	private boolean autoCommit = true;
	// The reconnection policy that would be applicable during initial/intermittent connection failures.
	// The valid values for this parameter are NoRetry, BoundedRetry and InfiniteRetry.
	// If not specified, it is set to BoundedRetry
	private String reconnectionPolicy = IJDBCConstants.RECONNPOLICY_BOUNDEDRETRY; 
	// The number of successive connection that will be attempted
	// If not present the default value is 5
	private int reconnectionBound = IJDBCConstants.RECONN_BOUND_DEFAULT;
	// The time period in seconds which it will be wait before trying to reconnect.
	// If not specified, the default value is 10.0.
	private double reconnectionInterval = IJDBCConstants.RECONN_INTERVAL_DEFAULT;
	
	// The statement
	Statement stmt = null;
	// The PreparedStatement for SQL statement with parameter markers
	PreparedStatement preparedStmt = null;
	
	// This constructor sets the jdbc connection information required
	public JDBCConnectionHelper(String jdbcClassName, String jdbcUrl,
			String jdbcUser, String jdbcPassword, String jdbcProperties, boolean autoCommit, String isolationLevel) {
		this.jdbcClassName = jdbcClassName;
		this.jdbcUrl = jdbcUrl;
		this.jdbcUser = jdbcUser;
		this.jdbcPassword = jdbcPassword;
		this.jdbcProperties = jdbcProperties;
		this.autoCommit = autoCommit;
		this.isolationLevel = isolationLevel;
	}

	// This constructor sets the jdbc connection information with reconnection policy
	public JDBCConnectionHelper(String jdbcClassName, String jdbcUrl,
			String jdbcUser, String jdbcPassword, String jdbcProperties, boolean autoCommit, String isolationLevel, 
			String reconnectionPolicy, int reconnectionBound, double reconnectionInterval) {
		this.jdbcClassName = jdbcClassName;
		this.jdbcUrl = jdbcUrl;
		this.jdbcUser = jdbcUser;
		this.jdbcPassword = jdbcPassword;
		this.jdbcProperties = jdbcProperties;
		this.autoCommit = autoCommit;
		this.isolationLevel = isolationLevel;
		this.reconnectionPolicy = reconnectionPolicy;
		this.reconnectionBound = reconnectionBound;
		this.reconnectionInterval = reconnectionInterval;
	}

	// getter for connect
	public Connection getConnection(){
		return connection;
	}
	
	// Create the JDBC connection
	public void createConnection() throws Exception{
		
        //Load class into memory
        Class.forName(jdbcClassName);
        
        // Load jdbc properties
        Properties jdbcConnectionProps = null;
        if (jdbcProperties != null){
			FileInputStream fileInput = new FileInputStream(jdbcProperties);
			jdbcConnectionProps = new Properties();
			jdbcConnectionProps.load(fileInput);
			fileInput.close();
        }   
        
        //Establish connection
		int nConnectionAttempts = 0;
		// Reconnection interval in milliseconds as specified in reconnectionInterval parameter
		final long interval = TimeUnit.MILLISECONDS.convert((long) reconnectionInterval, TimeUnit.SECONDS);
		
		while (!Thread.interrupted()) {
			// make a call to connect subroutine to create a connection
			// for each unsuccessful attempt increment the
			// nConnectionAttempts
			try {
				nConnectionAttempts ++;
				
		        if (jdbcConnectionProps != null){
		        	connection = DriverManager.getConnection(jdbcUrl, jdbcConnectionProps);
		        }else if (jdbcUser != null && jdbcPassword != null){
		        	connection = DriverManager.getConnection(jdbcUrl, jdbcUser, jdbcPassword);
		        }else{
		        	connection = DriverManager.getConnection(jdbcUrl);
		        }
				break;
			} catch (SQLException e) {
    			// If Reconnection Policy is NoRetry, throw SQLException
				if (reconnectionPolicy == IJDBCConstants.RECONNPOLICY_NORETRY) {
					throw e;
				}

				// If Reconnection Policy is BoundedRetry, reconnect until maximum reconnectionBound value
				if (reconnectionPolicy == IJDBCConstants.RECONNPOLICY_BOUNDEDRETRY) {
					if (nConnectionAttempts == reconnectionBound){
						//Throw SQLException if the connection attempts reach to maximum reconnectionBound value
						throw e;
					}else{
						// Sleep for specified wait period
						Thread.sleep(interval);
					}
				}
				// If Reconnection Policy is InfiniteRetry, reconnect
				if (reconnectionPolicy == IJDBCConstants.RECONNPOLICY_INFINITERETRY) {
					// Sleep for specified wait period
					Thread.sleep(interval);
				}

			}
		}
		
        connection.setAutoCommit(autoCommit);
        
        // set isolation level
        if (isolationLevel.equalsIgnoreCase(IJDBCConstants.TRANSACTION_READ_UNCOMMITTED)){
        	connection.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
        }
        if (isolationLevel.equalsIgnoreCase(IJDBCConstants.TRANSACTION_READ_COMMITTED)){
        	connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
        }
        if (isolationLevel.equalsIgnoreCase(IJDBCConstants.TRANSACTION_REPEATABLE_READ)){
        	connection.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
        }
        if (isolationLevel.equalsIgnoreCase(IJDBCConstants.TRANSACTION_SERIALIZABLE)){
        	connection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
        }
        
	}

	// Rest the JDBC connection
	public void resetConnection(String jdbcClassName, String jdbcUrl,
			String jdbcUser, String jdbcPassword, String jdbcProperties) throws Exception{
		this.jdbcClassName = jdbcClassName;
		this.jdbcUrl = jdbcUrl;
		this.jdbcUser = jdbcUser;
		this.jdbcPassword = jdbcPassword;
		this.jdbcProperties = jdbcProperties;
		
		// Close existing JDBC connection
		closeConnection();
		// Create new JDBC connection
		createConnection();
		
	}
	
	// Commit the transaction
	public void commit() throws SQLException{
		// Commit the transaction
		if (connection != null){
			connection.commit();
		}else{
			throw new SQLException("JDBC connection does not exist");
		}
	}

	// Execute the batch and commit the transaction
	public void commitWithBatchExecution() throws SQLException{
		// Execute the batch
		if (preparedStmt != null){
			preparedStmt.executeBatch();
		}
		if (stmt != null){
			stmt.executeBatch();
		}
		// Commit the transaction
		if (connection != null){
			connection.commit();
		}else{
			throw new SQLException("JDBC connection does not exist");
		}
	}
	
	public Statement getStatement() {
		return stmt;
	}

	public void setStatement(Statement stmt) {
		this.stmt = stmt;
	}

	public PreparedStatement getPreparedStatement() {
		return preparedStmt;
	}

	public void setPreparedStatement(PreparedStatement preparedStmt) {
		this.preparedStmt = preparedStmt;
	}

	// Roll back the transaction
	public void rollbackWithClearBatch() throws SQLException{
		// Clear the batch
		if (preparedStmt != null){
			preparedStmt.clearBatch();
		}
		if (stmt != null){
			stmt.clearBatch();
		}
		// Roll back the transaction
		if (connection != null){
			connection.rollback();
		}else{
			throw new SQLException("JDBC connection does not exist");
		}
	}

	// Clear the batch and Roll back the transaction
	public void rollback() throws SQLException{
		// Roll back the transaction
		if (connection != null){
			connection.rollback();
		}else{
			throw new SQLException("JDBC connection does not exist");
		}
	}

	// Close the JDBC connection
	public void closeConnection() throws SQLException{
		try{
			// Close Statement object
			if (stmt != null){
				stmt.close();
				stmt = null;
			}
			
			// Close PreparedStatement object
			if (preparedStmt != null){
				preparedStmt.close();
				preparedStmt = null;
			}
		}finally{
			if (connection != null){
				connection.close();
				connection = null;
			}
		}
	}

	// Initiate PreparedStatement
	public void initPreparedStatement(String statement) throws SQLException{
		if (preparedStmt != null){
			preparedStmt.close();
		}
		preparedStmt = connection.prepareStatement(statement);
	}

	// Execute the statement
	public ResultSet executeStatement(String statement) throws SQLException{
		
		if (connection == null){
			throw new SQLException("JDBC connection does not exist");
		}
        // Init Statement interface
		if (stmt == null){
			stmt = connection.createStatement();
		}
		
		ResultSet rs = null;
		// Execute the statement
		if (statement != null){
			if (stmt.execute(statement)){
				rs = stmt.getResultSet();
			}
		}
		
		return rs;
	}

	// Add batch for statement
	public void addStatementBatch(String statement) throws SQLException{
		
		if (connection == null){
			throw new SQLException("JDBC connection does not exist");
		}
        // Init Statement interface
		if (stmt == null){
			stmt = connection.createStatement();
		}
		
		// Add batch
		if (statement != null){
			stmt.addBatch(statement);
		}
	}

	// Execute batch for statement
	public void executeStatementBatch() throws SQLException{
		
		if (connection == null){
			throw new SQLException("JDBC connection does not exist");
		}
		// Execute the batch
		if (stmt != null){
			stmt.executeBatch();
		}
	}

	// Clear batch for statement
	public void clearStatementBatch() throws SQLException{
		
		if (connection == null){
			throw new SQLException("JDBC connection does not exist");
		}
		// Clear the batch
		if (stmt != null){
			stmt.clearBatch();
		}
	}
	
	// Execute the preparedStatement
	public ResultSet executePreparedStatement(StatementParameter[] stmtParameters) throws SQLException{
		
		if (connection == null){
			throw new SQLException("JDBC connection does not exist");
		}
		
		ResultSet rs = null;
		
		if (stmtParameters != null){
			for (int i=0; i< stmtParameters.length; i++){
				preparedStmt.setObject(i+1, stmtParameters[i].getSplValue());
			}
		}
		
		if (preparedStmt.execute()){
			rs = preparedStmt.getResultSet();
		}
		return rs;
	}

	// Add batch for preparedStatement
	public void addPreparedStatementBatch (StatementParameter[] stmtParameters) throws SQLException{
		
		if (connection == null){
			throw new SQLException("JDBC connection does not exist");
		}
		
		if (stmtParameters != null){
			for (int i=0; i< stmtParameters.length; i++){
				preparedStmt.setObject(i+1, stmtParameters[i].getSplValue());
			}
		}
		
		preparedStmt.addBatch();
	}

	// Execute batch for preparedStatement
	public void executePreparedStatementBatch () throws SQLException{
		
		if (connection == null){
			throw new SQLException("JDBC connection does not exist");
		}
		
		preparedStmt.executeBatch();
	}

	// Clear batch for preparedStatement
	public void clearPreparedStatementBatch () throws SQLException{
		
		if (connection == null){
			throw new SQLException("JDBC connection does not exist");
		}
		
		preparedStmt.clearBatch();
	}

}