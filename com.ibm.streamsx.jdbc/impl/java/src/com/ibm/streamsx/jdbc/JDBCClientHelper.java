/*******************************************************************************
 * Copyright (C) 2015-2018 International Business Machines Corporation
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
import java.util.logging.Logger;

import com.ibm.streams.operator.Attribute;
import com.ibm.streams.operator.Type.MetaType;
import com.ibm.streams.operator.logging.LogLevel;
import com.ibm.streams.operator.logging.LoggerNames;
import com.ibm.streams.operator.logging.TraceLevel;
import com.ibm.streams.operator.types.Blob;

/* This class contains all the JDBC connection related information,
 * creating maintaining and closing a connection to the JDBC driver
 * Execute, commit and roll back SQL statement
 */
public class JDBCClientHelper {
	// JDBC connection
	private Connection connection = null;
    // JDBC connection status
    private boolean connected = false;
    
    private static final String CLASS_NAME = "com.ibm.streamsx.jdbc.JDBCClientHelper";

	/**
	 * Create a logger specific to this class
	 */
	private static Logger LOGGER = Logger.getLogger(LoggerNames.LOG_FACILITY
			+ "." + CLASS_NAME);
    
	// logger for trace/debug information
	protected static Logger TRACE = Logger.getLogger("com.ibm.streamsx.jdbc");

	// the class name for jdbc driver.
	private String jdbcClassName;
	// the database url, jdbc:subprotocol:subname.
	private String jdbcUrl;
	// the database user on whose behalf the connection is being made.
	private String jdbcUser = null;
	// the user's password.
	private String jdbcPassword = null;
	private boolean sslConnection = false;
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
	private String pluginName = null;
	private int securityMechanism = -1;
	
	//The time in seconds to wait for the database operation used to validate the connection to complete. 
	private int checkConnectionTimeOut = 2;


	// The statement
	Statement stmt = null;
	// The PreparedStatement for SQL statement with parameter markers
	PreparedStatement preparedStmt = null;

	// This constructor sets the jdbc connection information with reconnection policy
	public JDBCClientHelper(String jdbcClassName, String jdbcUrl,
			String jdbcUser, String jdbcPassword, boolean sslConnection, String jdbcProperties, boolean autoCommit, String isolationLevel,
			String reconnectionPolicy, int reconnectionBound, double reconnectionInterval,
			String pluginName, int securityMechanism) {
		this.jdbcClassName = jdbcClassName;
		this.jdbcUrl = jdbcUrl;
		this.jdbcUser = jdbcUser;
		this.jdbcPassword = jdbcPassword;
		this.sslConnection = sslConnection;
		this.autoCommit = autoCommit;
		this.isolationLevel = isolationLevel;
		this.reconnectionPolicy = reconnectionPolicy;
		this.reconnectionBound = reconnectionBound;
		this.reconnectionInterval = reconnectionInterval;
		this.pluginName = pluginName;
		this.securityMechanism = securityMechanism;
	}

	// getter for connect
	public Connection getConnection(){
		return connection;
	}

	// Create the JDBC connection
	public synchronized void createConnection() throws Exception, SQLException{
		LOGGER.log(LogLevel.INFO, "createConnection \njdbcUser = " + jdbcUser + "\njdbcUrl  = " + jdbcUrl);
		// Attempt to create connection only when existing connection is invalid.
		if (!isConnected()){
	        //Load class into memory
	        Class.forName(jdbcClassName);

	        Properties jdbcConnectionProps = new Properties();
        	// pick up user and password if they are parameters
        	if (jdbcUser != null && jdbcPassword != null) {
				jdbcConnectionProps.put("user", jdbcUser);
				jdbcConnectionProps.put("password", jdbcPassword);
				// properties for phoenix jdbc properties.
				jdbcConnectionProps.put("avatica_user", jdbcUser);
				jdbcConnectionProps.put("avatica_password", jdbcPassword);
        	}
	        	
	        
	        // add sslConnection to properties
	        if (sslConnection) {
	        	jdbcConnectionProps.put("sslConnection","true");
	        	if (null != pluginName) {
	        		TRACE.log(TraceLevel.INFO, "pluginName = " + pluginName);
	        		jdbcConnectionProps.put("pluginName", pluginName);
	        	}
	        	if (-1 != securityMechanism) {
	        		TRACE.log(TraceLevel.INFO,"securityMechanism = " + securityMechanism);
	        		jdbcConnectionProps.put("securityMechanism", new String(""+securityMechanism+""));
	        	}
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
					DriverManager.setLoginTimeout(5);
					nConnectionAttempts ++;
					TRACE.log(TraceLevel.DEBUG,"JDBC connection attempt "+nConnectionAttempts);					
					if (jdbcConnectionProps != null){
	    				TRACE.log(TraceLevel.DEBUG,"JDBC connection -- props not null ");
	    				TRACE.log(TraceLevel.DEBUG,jdbcConnectionProps.toString());
		 	        	connection = DriverManager.getConnection(jdbcUrl, jdbcConnectionProps);
			        }else if (jdbcUser != null && jdbcPassword != null){
			        	TRACE.log(TraceLevel.DEBUG,"JDBC connection -- userid password exist "+jdbcUrl);
			        	connection = DriverManager.getConnection(jdbcUrl, jdbcConnectionProps);
			        }else{
			        	TRACE.log(TraceLevel.DEBUG,"JDBC connection -- using url only "+jdbcUrl);
			        	connection = DriverManager.getConnection(jdbcUrl,jdbcConnectionProps);
			        }
					break;
				} catch (SQLException e) {
					// output exception info into trace file if in debug mode
					TRACE.log(LogLevel.ERROR,"JDBC connect threw SQL Exception",e);				
					System.out.println("createConnection  SQLException  sqlCode: " + e.getErrorCode() + " sqlState: " + e.getSQLState() + " sqlMessage: " + e.getMessage());
					System.out.println("createConnection  reconnectionPolicy: " + reconnectionPolicy);
					System.out.println("createConnection  reconnectionBound: " + reconnectionBound);
					System.out.println("createConnection  reconnectionInterval: " + reconnectionInterval);
					System.out.println("createConnection  Connection Attempts: " + nConnectionAttempts);

					// If Reconnection Policy is NoRetry, throw SQLException
					if (reconnectionPolicy.equalsIgnoreCase(IJDBCConstants.RECONNPOLICY_NORETRY)) {
						throw e;
					}

					// If Reconnection Policy is BoundedRetry, reconnect until maximum reconnectionBound value
					if (reconnectionPolicy.equalsIgnoreCase(IJDBCConstants.RECONNPOLICY_BOUNDEDRETRY)) {
						if (nConnectionAttempts == reconnectionBound){
							//Throw SQLException if the connection attempts reach to maximum reconnectionBound value
							throw e;
						}else{
							// Sleep for specified wait period
							Thread.sleep(interval);
						}
					}
					// If Reconnection Policy is InfiniteRetry, reconnect
					if (reconnectionPolicy.equalsIgnoreCase(IJDBCConstants.RECONNPOLICY_INFINITERETRY)) {
						// Sleep for specified wait period
						Thread.sleep(interval);
					}

				}
			}
			LOGGER.log(LogLevel.INFO,"JDBC connectioned ");

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

	        // Set JDBC connection status as true
	        connected = true;
		LOGGER.log(LogLevel.INFO, "createConnection connected =" + connected);
	   }
	}

	// Check if JDBC connection is valid
	public synchronized boolean isValidConnection() throws SQLException{
		LOGGER.log(LogLevel.INFO,"JDBC connection validation");
		if (connection == null || !connection.isValid(checkConnectionTimeOut)){
			connected = false;
			LOGGER.log(LogLevel.INFO,"JDBC connection invalid ");
			return false;
		}
		LOGGER.log(LogLevel.INFO,"JDBC connection valid ");
		return true;
	}

	// Return JDBC connection status
	public boolean isConnected(){
 		return connected;
	}

	// Reset the JDBC connection with the same configuration information
	public synchronized void resetConnection() throws Exception{
		LOGGER.log(LogLevel.INFO,"JDBC connection resetting");
		if (!isConnected()){
			// Close existing JDBC connection
			closeConnection();
			// Create new JDBC connection
			createConnection();
		}
	}

	// Reset the JDBC connection
	public synchronized void resetConnection(String jdbcClassName, String jdbcUrl,
			String jdbcUser, String jdbcPassword, String jdbcProperties) throws Exception{
		this.jdbcClassName = jdbcClassName;
		this.jdbcUrl = jdbcUrl;
		this.jdbcUser = jdbcUser;
		this.jdbcPassword = jdbcPassword;

		// Set JDBC Connection Status as false
		connected = false;
		// Reset JDBC Connection
		resetConnection();
	}

	// Commit the transaction
	public void commit() throws SQLException{
		// Commit the transaction
		if (connection != null){
			connection.commit();
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
		}
	}

	// Clear the batch and Roll back the transaction
	public void rollback() throws SQLException{
		// Roll back the transaction
		if (connection != null){
			connection.rollback();
		}
	}

	// Close the JDBC connection
	public synchronized void closeConnection() throws SQLException{
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
			// Set JDBC Connection Status as false
			connected = false;
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

		// Execute the batch
		if (stmt != null){
			stmt.executeBatch();
		}
	}

	// Clear batch for statement
	public void clearStatementBatch() throws SQLException{

		// Clear the batch
		if (stmt != null){
			stmt.clearBatch();
		}
	}

	// Execute the preparedStatement
	public ResultSet executePreparedStatement(StatementParameter[] stmtParameters) throws SQLException{
		ResultSet rs = null;

		addParametersToStatement(stmtParameters);
		if (preparedStmt.execute()){
			rs = preparedStmt.getResultSet();
		}
		return rs;
	}

	// Add batch for preparedStatement
	public void addPreparedStatementBatch (StatementParameter[] stmtParameters) throws SQLException{

		addParametersToStatement(stmtParameters);
		preparedStmt.addBatch();
	}

	// add parameters to the statement 
	public void addParametersToStatement(StatementParameter[] stmtParameters) throws SQLException{
		
		if (stmtParameters != null){
			for (int i=0; i< stmtParameters.length; i++){
				Attribute attr = stmtParameters[i].getSplAttribute();
				
				// special handling for type Blob
				if (MetaType.BLOB == attr.getType().getMetaType()) {
					if (LOGGER.isLoggable(TraceLevel.DEBUG)) {
						LOGGER.log(TraceLevel.DEBUG, "Converting SPL:Blob to java.sql.Blob");
					}
					com.ibm.streams.operator.types.Blob splBlob = (com.ibm.streams.operator.types.Blob) stmtParameters[i].getSplValue();
					preparedStmt.setBlob(i+1, splBlob.getInputStream());
				} else {
					preparedStmt.setObject(i+1, stmtParameters[i].getSplValue());
				}
			}
		}		
	}
	
	// Execute batch for preparedStatement
	public void executePreparedStatementBatch () throws SQLException{

		preparedStmt.executeBatch();
	}

	// Clear batch for preparedStatement
	public void clearPreparedStatementBatch () throws SQLException{

		preparedStmt.clearBatch();
	}

}
