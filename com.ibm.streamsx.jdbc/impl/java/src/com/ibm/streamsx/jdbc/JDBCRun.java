/*******************************************************************************
 * Copyright (C) 2015-2018 International Business Machines Corporation
 * All Rights Reserved
 *******************************************************************************/
package com.ibm.streamsx.jdbc;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Logger;

import com.ibm.streams.operator.Attribute;
import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.OperatorContext.ContextCheck;
import com.ibm.streams.operator.OutputTuple;
import com.ibm.streams.operator.StreamSchema;
import com.ibm.streams.operator.StreamingData.Punctuation;
import com.ibm.streams.operator.StreamingInput;
import com.ibm.streams.operator.StreamingOutput;
import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.TupleAttribute;
import com.ibm.streams.operator.Type;
import com.ibm.streams.operator.Type.MetaType;
import com.ibm.streams.operator.compile.OperatorContextChecker;
import com.ibm.streams.operator.logging.LogLevel;
import com.ibm.streams.operator.logging.LoggerNames;
import com.ibm.streams.operator.logging.TraceLevel;
import com.ibm.streams.operator.meta.TupleType;
import com.ibm.streams.operator.model.InputPortSet;
import com.ibm.streams.operator.model.InputPorts;
import com.ibm.streams.operator.model.OutputPortSet;
import com.ibm.streams.operator.model.OutputPorts;
import com.ibm.streams.operator.model.Parameter;
import com.ibm.streams.operator.model.PrimitiveOperator;
import com.ibm.streams.operator.state.Checkpoint;
import com.ibm.streams.operator.state.ConsistentRegionContext;
import com.ibm.streams.operator.types.Blob;
import com.ibm.streams.operator.types.RString;
import com.ibm.streams.operator.types.Timestamp;
import com.ibm.streams.operator.types.ValueFactory;
import com.ibm.streams.operator.types.XML;
import com.ibm.streams.operator.model.OutputPortSet.WindowPunctuationOutputMode;

/** 
 * The JDBCRun operator runs a user-defined SQL statement that is based on an
 * input tuple. The statement is run once for each input tuple received. Result
 * sets that are produced by the statement are emitted as output stream tuples.
 */
@PrimitiveOperator(description = "The **JDBCRun** operator runs a user-defined SQL statement that is based on an input tuple.\\n\\n"
		+ " The statement is run once for each input tuple received.\\n\\n"
		+ " Result sets that are produced by the statement are emitted as output stream tuples.\\n\\n"
		+ " The **JDBCRun** operator is commonly used to update, merge, and delete database management system (DBMS) records.\\n\\n"
		+ " This operator is also used to retrieve records, create and drop tables, and to call stored procedures.\\n\\n"
		+ " it is strongly recommended before you begin with the implementation of a SPL application on JDBCRun operator, read the **documentation of database vendors**.\\n\\n"
		+ " Every database vendor delivers one or more JAR libraries as jdbc driver. Please check the version of your database server and the version of JDBC driver libraries.\\n\\n"
		+ " Behavior in a **consistent region**:\\n\\n"
		+ " The **JDBCRun** operator can be used in a consistent region. It cannot be the start operator of a consistent region.\\n\\n"
		+ " In a consistent region, the configured value of the transactionSize is ignored. Instead, database commits are performed (when supported by the DBMS) on consistent region checkpoints, and database rollbacks are performed on consistent region resets.\\n\\n"
		+ " On **drain**: If there are any pending statements, they are run. If the statement generates a result set and the operator has an output port, tuples are generated from the results and submitted to the output port. If the operator has an error output port and the statement generates any errors, tuples are generated from the errors and submitted to the error output port.\\n\\n"
		+ " On **checkpoint**: A database commit is performed.\\n\\n"
		+ " On **reset**: Any pending statements are discarded. A rollback is performed.\\n\\n"
		+ " The new version of toolkit 1.3.x. supports also `optional type`.\\n\\n"  
		+ " The SPL applications based on new JDBC toolkit and created with a new Streams that supports **optional type**"
		+ " are able to write/read 'null' to/from a `nullable` column in a table. ")

@InputPorts({ 
		@InputPortSet(cardinality = 1, description = "The `JDBCRun` operator has one required input port. When a tuple is received on the required input port, the operator runs an SQL statement."),
		@InputPortSet(cardinality = 1, optional = true, controlPort = true, description = "The `JDBCRun` operator has one optional input port. This port allows operator to change jdbc connection information at run time.") })
@OutputPorts({
		@OutputPortSet(cardinality = 1, description = "The `JDBCRun` operator has one required output port. The output port submits a tuple for each row in the result set of the SQL statement if the statement produces a result set. The output tuple values are assigned in the following order: 1. Columns that are returned in the result set that have same name from the output tuple 2. Auto-assigned attributes of the same name from the input tuple", windowPunctuationOutputMode=WindowPunctuationOutputMode.Generating),
		@OutputPortSet(cardinality = 1, optional = true, description = "The `JDBCRun` operator has one optional output port. This port submits tuples when an error occurs while the operator is running the SQL statement. The tuples deliver sqlCode, sqlStatus and sqlMessage. ") })


public class JDBCRun extends AbstractJDBCOperator {

	private static final String CLASS_NAME = "com.ibm.streamsx.jdbc.jdbcrun.JDBCRun";

	private static final CommitPolicy DEFAULT_COMMIT_POLICY = CommitPolicy.OnCheckpoint;

	/**
	 * Create a logger specific to this class
	 */
	private static Logger LOGGER = Logger.getLogger(LoggerNames.LOG_FACILITY
	        + "." + CLASS_NAME);  

	/**
	 * variable to hold the output port
	 */
	private StreamingOutput<OutputTuple> dataOutputPort;

	/**
	 * hasErrorPort signifies if the operator has error port defined or not
	 * assuming in the beginning that the operator does not have an error output
	 * port by setting hasErrorPort to false. further down in the code, if the
	 * number of output ports is 2, we set to true We send data to error output
	 * port only in case where hasErrorPort is set to true which implies that
	 * the operator instance has a error output port defined.
	 */
	private boolean hasErrorPort = false;

	/**
	 * The SQL statement from statement parameter is static, and JDBC
	 * PreparedStatement interface will be used for execution. The SQL statement
	 * from statementAttr parameter is dynamic, and JDBC Statement interface
	 * will be used for execution.
	 */
	private boolean isStaticStatement = false;

	/**
	 * Variable to specify error output port
	 */
	private StreamingOutput<OutputTuple> errorOutputPort;

	/**
	 * Define operator parameters
	 */

	// This parameter specifies the value of any valid SQL statement.
	private String statement;
	// This parameter specifies the value of statement parameters.
	private String statementParamAttrs;
	// StatementParameter arrays
	StatementParameter[] statementParamArrays = null;
	// This parameter specifies the value of SQL statement that is from stream
	// attribute (no parameter markers).
	private TupleAttribute<Tuple, String> statementAttr;

	// This parameter specifies the number of statements to execute as a batch.
	// The default transaction size is 1.
	private int batchSize = 1;
	// This parameter specifies the number of executions to commit per
	// transaction.
	// The default transaction size is 1 and transactions are automatically
	// committed.
	private int transactionSize = 1;
	// Transaction counter
	private int transactionCount = 0;
	// Execution Batch counter
	private int batchCount = 0;

	// commit interval -- used to commit every interval
	private int commitInterval = 0;
	private ScheduledFuture<?> commitThread;
	private Lock commitLock = new ReentrantLock();

	// This parameter points to an output attribute and returns true if the
	// statement produces result sets,
	// otherwise, returns false.
	private String hasResultSetAttr = null;
	private boolean hasResultSetValue = false;

	// This parameter points to an error output attribute and returns the SQL
	// status information.
	private String sqlStatusAttr = null;
	// sqlStatus attribute for data output port
	private String[] sqlStatusDataAttrs = null;
	// sqlStatus attribute for error output port
	private String[] sqlStatusErrorAttrs = null;
	// check connection
	private boolean checkConnection = false;
	private int idleSessionTimeOutMinute = 0;
	private int idleSessionTimeOutTimer = 0;
	
	
	private Thread checkConnectionThread = null;
	private Thread idleSessionTimeOutThread = null;
	
	

	private CommitPolicy commitPolicy = DEFAULT_COMMIT_POLICY;

	// Parameter commitPolicy
	@Parameter(name = "commitPolicy", optional = true, 
			description = "This parameter specifies the commit policy that should be used when the operator is in a consistent region. \\n\\n"
					+ "If set to *OnCheckpoint*, then commits will only occur during checkpointing. \\n\\n"
					+ "If set to *OnTransactionAndCheckpoint*, commits will occur during checkpointing as well as whenever the **transactionCount** or **commitInterval** are reached. \\n\\n"
					+ "The default value is *OnCheckpoint*.\\n\\n"
					+ "It is recommended that the *OnTransactionAndCheckpoint* value be set if the tables that the statements are being executed against can tolerate duplicate entries as these parameter value may cause the same statements to be executed if the operator is reset. \\n\\n"
					+ "It is also highly recommended that the **transactionCount** parameter not be set to a value greater than 1 when the policy is *onTransactionAndCheckpoint*, as this can lead to some statements not being executed in the event of a reset. \\n\\n"
					+ "This parameter is ignored if the operator is not in a consistent region. The default value for this parameter is *OnCheckpoint*.")
	public void setCommitPolicy(CommitPolicy commitPolicy) {
		this.commitPolicy = commitPolicy;
	}

	// Parameter statement
	@Parameter(name = "statement", optional = true, 
			description = "This parameter specifies the value of any valid SQL or stored procedure statement. The statement can contain parameter markers")
	public void setStatement(String statement) {
		this.statement = statement;
	}

	// Parameter statementParameters
	@Parameter(name = "statementParamAttrs", optional = true, 
			description = "This optional parameter specifies the value of statement parameters. The statementParameter value and SQL statement parameter markers are associated in lexicographic order. For example, the first parameter marker in the SQL statement is associated with the first statementParameter value.")
	public void setStatementParamAttrs(String statementParamAttrs) {
		this.statementParamAttrs = statementParamAttrs;

		String statementParamNames[] = statementParamAttrs.split(",");
		statementParamArrays = new StatementParameter[statementParamNames.length];
		for (int i = 0; i < statementParamNames.length; i++) {
			statementParamArrays[i] = new StatementParameter();
			statementParamArrays[i].setSplAttributeName(statementParamNames[i]);
		}
	}

	// Parameter statementAttr
	@Parameter(name = "statementAttr", optional = true, 
			description = "This parameter specifies the value of complete SQL or stored procedure statement that is from stream attribute (no parameter markers).")
	public void setStatementAttr(TupleAttribute<Tuple, String> statementAttr) {
		this.statementAttr = statementAttr;
	}

	// Parameter transactionSize
	@Parameter(name = "transactionSize", optional = true, 
			description = "This optional parameter specifies the number of executions to commit per transaction. The default transaction size is 1 and transactions are automatically committed.")
	public void setTransactionSize(int transactionSize) {
		this.transactionSize = transactionSize;
	}

	// Parameter batchSize
	@Parameter(name = "batchSize", optional = true, 
			description = "This optional parameter specifies the number of statement to execute as a batch. The default batch size is 1.")
	public void setBatchSize(int batchSize) {
		this.batchSize = batchSize;
	}

	// Parameter hasResultSetAttr
	@Parameter(name = "hasResultSetAttr", optional = true, 
			description = "This parameter points to an output attribute and returns true if the statement produces result sets, otherwise, returns false")
	public void setHasResultSetAttr(String hasResultSetAttr) {
		this.hasResultSetAttr = hasResultSetAttr;
	}

	// Parameter sqlStatusAttr
	@Parameter(name = "sqlStatusAttr", optional = true, 
			description = "This parameter points to one or more output attributes and returns the SQL status information, including SQL code (the error number associated with the SQLException) and SQL state (the five-digit XOPEN SQLState code for a database error)")
	public void setSqlStatusAttr(String sqlStatusAttr) {
		this.sqlStatusAttr = sqlStatusAttr;
	}

	// Parameter commitInterval
	@Parameter(name = "commitInterval", optional = true, 
			description = "This parameter sets a commit interval for the sql statements that are being processed and overrides the batchSize and transactionSize parameters. ")
	public void setCommitInterval(int commitInterval) {
		this.commitInterval = commitInterval;
	}

	// Parameter checkConnection
	@Parameter(name = "checkConnection", optional = true, 
			description = "This optional parameter specifies whether a **checkConnection** thread should be start. The thread checks periodically the status of JDBC connection. The JDBCRun sends in case of any connection failure a SqlCode and a message to SPL application.The default value is `false`.")
	public void setcheckConnection(boolean checkConnection) {
		this.checkConnection = checkConnection;
	}
	
	public boolean getCheckConnection() {
		return checkConnection;
	}
	
	// Parameter idleSessionTimeOut
	@Parameter(name = "idleSessionTimeOut", optional = true, 
			description = "This optional parameter specifies the Idle Session Timeout in minute. Once the idle time value is reached, the operator close the database connection. Th timer restarts after a new query.")
	public void setidleSessionTimeOut(int idleSessionTimeOut) {
		this.idleSessionTimeOutMinute = idleSessionTimeOut;
	}
	
	public int getidleSessionTimeOut() {
		return idleSessionTimeOutMinute;
	}
	
	
	/*
	 * The method checkErrorOutputPort validates that the stream on error output
	 * port contains the optional attribute of type which is the incoming tuple,
	 * and a JdbcSqlStatus_T which will contain the error message in order.
	 * @param checker
	 */
	@ContextCheck
	public static void checkErrorOutputPort(OperatorContextChecker checker) {
		OperatorContext context = checker.getOperatorContext();
		// Check if the operator has an error port defined
		if (context.getNumberOfStreamingOutputs() == 2) {
			StreamingOutput<OutputTuple> errorOutputPort = context.getStreamingOutputs().get(1);
			// The optional error output port can have no more than two
			// attributes.
			if (errorOutputPort.getStreamSchema().getAttributeCount() > 2) {
				LOGGER.log(LogLevel.ERROR, Messages.getString("JDBC_ATMOST_TWO_ATTR")); 

			}
			// The optional error output port must have at least one attribute.
			if (errorOutputPort.getStreamSchema().getAttributeCount() < 1) {
				LOGGER.log(LogLevel.ERROR, Messages.getString("JDBC_ATLEAST_ONE_ATTR")); 

			}
			// If two attributes are specified, the first attribute in the
			// optional error output port must be a tuple.
			if (errorOutputPort.getStreamSchema().getAttributeCount() == 2) {
				if (errorOutputPort.getStreamSchema().getAttribute(0).getType().getMetaType() != Type.MetaType.TUPLE) {
					LOGGER.log(LogLevel.ERROR, Messages.getString("JDBC_ERROR_PORT_FIRST_ATTR_TUPLE")); 

				}
			}
		}

	}

	@ContextCheck(compile = true, runtime = false)
	public static void checkCompileTimeConsistentRegion(OperatorContextChecker checker) {
		ConsistentRegionContext consistentRegionContext = checker.getOperatorContext()
				.getOptionalContext(ConsistentRegionContext.class);

		if (consistentRegionContext != null && consistentRegionContext.isStartOfRegion()) {
			checker.setInvalidContext(Messages.getString("JDBC_NO_CONSISTENT_REGION", "JDBCRun"), new String[] {}); 
		}
	}

	/**
	 * Do any necessary compile time checks. It calls the checker of the super
	 * class.
	 * 
	 * @param checker
	 */

	@ContextCheck(compile = true)
	public static void checkDeleteAll(OperatorContextChecker checker) {
		if (!checker.checkDependentParameters("jdbcDriverLib")){
//		if (!checker.checkDependentParameters("jdbcDriverLib", "jdbcUrl")){
			checker.setInvalidContext(Messages.getString("JDBC_URL_NOT_EXIST"), null);
		}

		// If checkConnection is set as parameter, idleSessionTimeOut can not be set
		checker.checkExcludedParameters("checkConnection", "idleSessionTimeOut");

	}
	
	@ContextCheck(compile = false, runtime = true)
	public static void checkParameterAttributes(OperatorContextChecker checker) {

		OperatorContext context = checker.getOperatorContext();

		if (checker.getOperatorContext().getNumberOfStreamingOutputs() > 0) {
			StreamingOutput<OutputTuple> dataPort = context.getStreamingOutputs().get(0);
			StreamSchema schema = dataPort.getStreamSchema();
			// Check hasResultSetAttr parameters at runtime
			if ((context.getParameterNames().contains("hasResultSetAttr"))) {
				if (schema.getAttribute(context.getParameterValues("hasResultSetAttr").get(0)) == null) {
	                LOGGER.log(LogLevel.ERROR, Messages.getString("JDBC_HASRSATTR_NOT_EXIST"), context.getParameterValues("hasResultSetAttr").get(0));  
					checker.setInvalidContext(Messages.getString("JDBC_HASRSATTR_NOT_EXIST") + context.getParameterValues("hasResultSetAttr").get(0), null);  
				}
			}
		}

		// Check sqlStatusAttr parameter
		if ((context.getParameterNames().contains("sqlStatusAttr"))) {
			String strSqlStatusAttr = context.getParameterValues("sqlStatusAttr").get(0);
			if (strSqlStatusAttr == null) {
			    LOGGER.log(LogLevel.ERROR, Messages.getString("JDBC_SQLSTATUSATTR_NOT_EXIST"), "null");  
			    checker.setInvalidContext(Messages.getString("JDBC_SQLSTATUSATTR_NOT_EXIST"), null); 
			}
			if (strSqlStatusAttr != null) {
				// Data port
				StreamingOutput<OutputTuple> dataPort = context.getStreamingOutputs().get(0);
				// Error port
				StreamingOutput<OutputTuple> errorPort = null;
				if (checker.getOperatorContext().getNumberOfStreamingOutputs() > 1) {
					errorPort = context.getStreamingOutputs().get(1);
				}

				String sqlStatus[] = strSqlStatusAttr.split(",");

				for (int i = 0; i < sqlStatus.length; i++) {
					String strSqlStatus = sqlStatus[i].trim();
					if (strSqlStatus.isEmpty()) {
	    			    LOGGER.log(LogLevel.ERROR, Messages.getString("JDBC_SQLSTATUSATTR_NOT_EXIST"), "null");  
	    			    checker.setInvalidContext(Messages.getString("JDBC_SQLSTATUSATTR_NOT_EXIST"), null); 
	   
					}
					if (!strSqlStatus.isEmpty()) {
						if (findSqlStatusAttr(dataPort, errorPort, strSqlStatus) == -1) {
							LOGGER.log(LogLevel.ERROR, "SQLSTATUSATTR_NOT_EXIST", strSqlStatus);
	    				    checker.setInvalidContext(Messages.getString("JDBC_SQLSTATUSATTR_NOT_EXIST") + strSqlStatus, null); 

						}
					}
				}
			}

		}

		if (!checker.getOperatorContext().getParameterValues("idleSessionTimeOut").isEmpty()) {
			if (Integer.valueOf(checker.getOperatorContext().getParameterValues("idleSessionTimeOut").get(0)) < 1) {
				LOGGER.log(LogLevel.ERROR, "The value of the idleSessionTimeOut parameter must be greater than zero");
				checker.setInvalidContext(Messages.getString("JDBC_INVALID_SESSION_TIMEOUT_VALUA"), null); 
			}
		}
	}

	// Find sqlStatusAttr on data port and error port
	// Return value: 0 - data port; 1 - error port; -1 - not found
	private static int findSqlStatusAttr(StreamingOutput<OutputTuple> dataPort, StreamingOutput<OutputTuple> errorPort,
			String strSqlStatus) {

		// Data port stream name
		String dataStreamName = dataPort.getName();
		// Error port stream name
		String errorStreamName = null;
		if (errorPort != null) {
			errorStreamName = errorPort.getName();
		}

		if (strSqlStatus.contains(".")) {
			// The attribute is a full qualified name
			String strs[] = strSqlStatus.split("\\.");
			if (strs.length > 1) {
				if (dataStreamName.equals(strs[0])) {
					StreamSchema schema = dataPort.getStreamSchema();
					Attribute attr = schema.getAttribute(strs[1]);
					if (attr != null) {
						return 0;
					}
				}
				if (errorStreamName != null && errorStreamName.equals(strs[0])) {
					StreamSchema schema = errorPort.getStreamSchema();
					Attribute attr = schema.getAttribute(strs[1]);
					if (attr != null) {
						return 1;
					}
				}
			}
		}

		if (!strSqlStatus.contains(".")) {
			// The attribute isn't a full qualified name
			// Find the first stream that contains the attribute name
			StreamSchema dataSchema = dataPort.getStreamSchema();
			if (dataSchema.getAttribute(strSqlStatus) != null) {
				return 0;
			}
			if (errorPort != null) {
				StreamSchema errorSchema = errorPort.getStreamSchema();
				if (errorSchema.getAttribute(strSqlStatus) != null) {
					return 1;
				}
			}
		}

		return -1;

	}

	
	/**
	 * Initialize this operator. Called once before any tuples are processed.
	 * 
	 * @param context
	 *            OperatorContext for this operator.
	 * @throws Exception
	 *             Operator failure, will cause the enclosing PE to terminate.
	 */
	@Override
	public synchronized void initialize(OperatorContext context) throws Exception {
		super.initialize(context);
		/*
		 * Set appropriate variables if the optional error output port is
		 * specified. Also set errorOutputPort to the output port at index 0
		 */
		if (context.getNumberOfStreamingOutputs() == 2) {
			hasErrorPort = true;
			errorOutputPort = getOutput(1);
		}
		
		
		if (checkConnection) {
			startCheckConnection(context);
		}

		if (idleSessionTimeOutMinute > 1) {
			startidleSessionTimeOutThread(context);
		}
		
		// set the data output port
		dataOutputPort = getOutput(0);

		// Initiate parameter sqlStatusAttr
		initSqlStatusAttr();

		// Initiate PreparedStatement
		initPreparedStatement();		
	}			
	
	
	
	/**
	 * startCheckConnection starts a thread to check the JDBC connection.
	 * In case of any connection problem it tries to create a new connection
	 * with reconnectionPolicy parameters.
	 * When the connection fails it return a sqlcode -1 to the SPL application.
	 * The SPL application has to use the 2. optional output port of JDBCRun operator.
	 * @param context
	 */
	public void startCheckConnection(OperatorContext context) {
		checkConnectionThread = context.getThreadFactory().newThread(new Runnable() {
			
		@Override
		public void run() {
			int i = 0;
			while(true)
			{
				// check the JDBC connection every 5 seconds 
				try        
				{
				    Thread.sleep(5000);
                    System.out.println("checkConnection " + i++);
				} 
				catch(InterruptedException ex) 
				{
				    Thread.currentThread().interrupt();
				}
				try 
				{
				if (!jdbcClientHelper.isValidConnection()) {	
                    System.out.println("JDBC connection is invalid ");					
					try 
					{
						// f connection files it tries to reset JDBC connection
						// it is depending to the reconnection policy parameters
						resetJDBCConnection();
					}
					catch (Exception e2) {
						if (!jdbcClientHelper.isValidConnection() && hasErrorPort){
		                    try 
							{
								// if connection files it sends a sqlcode = -1 to the error output port
		                        JDBCSqlStatus jSqlStatus = new JDBCSqlStatus();
		                        jSqlStatus.sqlCode = -1;
		                        jSqlStatus.sqlMessage = "Invalid Connection";
								// submit error message
								submitErrorTuple(errorOutputPort, null, jSqlStatus);
							}
							catch (Exception e1) {
								e1.printStackTrace();													
							}
	                    
						}   
					}
					
					}
				} catch (SQLException e3) {
				}	
			} // end while
		} // end of run()
		
		}); 
		
		// start checkConnectionThread
		checkConnectionThread.start();
	}

	
	/**
	 * startidleSessionTimeOutThread starts a thread to check the JDBC connection.
	 * @param context
	 */
	public void startidleSessionTimeOutThread(OperatorContext context) {
		idleSessionTimeOutThread = context.getThreadFactory().newThread(new Runnable() {

	
		@Override
		public void run() {
			while(true)
			{
				// check the JDBC connection every minute
				try        
				{
				    Thread.sleep(60000);
				    idleSessionTimeOutTimer ++;
 //                   System.out.println("idleSessionTimeOut " + idleSessionTimeOutMinute + " idleSessionTimeOutTimer " + idleSessionTimeOutTimer);
				} 
				catch(InterruptedException ex) 
				{
				    Thread.currentThread().interrupt();
				}

				try 
				{
					if (idleSessionTimeOutTimer > idleSessionTimeOutMinute){
				    	
						if (jdbcClientHelper.isValidConnection()) {	
							try 
							{
								// if connection is valid 
								// close the connection
							    Thread.sleep(1000);
								jdbcClientHelper.closeConnection();
			                    System.out.println("close connection idleSessionTimeOut " + idleSessionTimeOutMinute + " idleSessionTimeOutTimer " + idleSessionTimeOutTimer);
							}
					catch (Exception e2) {
						e2.printStackTrace();													
	                    
					}
				}
					}
				} catch (SQLException e3) {
					e3.printStackTrace();													
				}	
			} // end while
		} // end of run()
		
		}); 
		
		// start idleSessionTimeOutThread
		idleSessionTimeOutThread.start();
	}

	
	/**
	 * Process control port
	 * he port allows operator to change JDBC connection information at runtime
	 * The port expects a value with JSON format
	 * @param stream
	 * @param tuple
	 * @throws Exception
	 */
	@Override
	protected void processControlPort(StreamingInput<Tuple> stream, Tuple tuple) throws Exception {
		super.processControlPort(stream, tuple);
		// Initiate PreparedStatement
		initPreparedStatement();
	}

	// JDBC connection need to be auto-committed or not
	@Override
	protected boolean isAutoCommit() {
		if ((consistentRegionContext != null && commitPolicy == CommitPolicy.OnCheckpoint) || (transactionSize > 1)
				|| (commitInterval > 0)) {
			// Set automatic commit to false when transaction size is more than
			// 1 or it is a consistent region.
			return false;
		}
		return true;
	}

	// Process input tuple
	protected void processTuple(StreamingInput<Tuple> stream, Tuple tuple) throws Exception {

		idleSessionTimeOutTimer = 0;
		commitLock.lock();

		try {
			// Execute the statement
			ResultSet rs = null;
			if (isStaticStatement) {
				if (batchSize > 1) {
					batchCount++;
					jdbcClientHelper
							.addPreparedStatementBatch(getStatementParameterArrays(statementParamArrays, tuple));
					if (batchCount >= batchSize) {
						batchCount = 0;
						transactionCount++;
						jdbcClientHelper.executePreparedStatementBatch();
					}
				} else {
					transactionCount++;
					rs = jdbcClientHelper
							.executePreparedStatement(getStatementParameterArrays(statementParamArrays, tuple));
				}
			} else {
				String statementFromAttribute = statementAttr.getValue(tuple);
				if (statementFromAttribute != null && !statementFromAttribute.isEmpty()) {
					TRACE.log(TraceLevel.DEBUG, "Statement: " + statementFromAttribute);
					if (batchSize > 1) {
						batchCount++;
						jdbcClientHelper.addStatementBatch(statementFromAttribute);
						if (batchCount >= batchSize) {
							batchCount = 0;
							transactionCount++;
							jdbcClientHelper.executeStatementBatch();
						}
					} else {
						transactionCount++;
						rs = jdbcClientHelper.executeStatement(statementFromAttribute);
						TRACE.log(TraceLevel.DEBUG, "Transaction Count: " + transactionCount);
					}
				} else {
	                LOGGER.log(LogLevel.ERROR, Messages.getString("JDBC_SQL_STATEMENT_NULL")); 
				}
			}

			// Commit the transactions according to transactionSize
			if ((consistentRegionContext == null
					|| (consistentRegionContext != null 
					&& commitPolicy == CommitPolicy.OnTransactionAndCheckpoint))
					&& (transactionSize > 1) && (transactionCount >= transactionSize)) {
				TRACE.log(TraceLevel.DEBUG, "Transaction Commit...");
				transactionCount = 0;
				jdbcClientHelper.commit();
			}

			if (rs != null) {
				// Set hasReultSetValue
				if (rs.next()) {
					hasResultSetValue = true;
					TRACE.log(TraceLevel.DEBUG, "Has Result Set: " + hasResultSetValue);
					// Submit result set as output tuple
					submitOutputTuple(dataOutputPort, tuple, rs, null);
					while (rs.next()) {
						// Submit result set as output tuple
						submitOutputTuple(dataOutputPort, tuple, rs, null);
					}
					// Generate a window punctuation after all of the tuples are
					// submitted
					dataOutputPort.punctuate(Punctuation.WINDOW_MARKER);
				} else {
					hasResultSetValue = false;
					TRACE.log(TraceLevel.DEBUG, "Has Result Set: " + hasResultSetValue);
					// Submit output tuple
					submitOutputTuple(dataOutputPort, tuple, null, null);
				}
				// Close result set
				rs.close();

			} else {
				// Set reultSetCountAttr to 0 if the statement does not produce
				// result sets
				hasResultSetValue = false;
				// Submit output tuple without result set
				submitOutputTuple(dataOutputPort, tuple, null, null);

			}
		} catch (SQLException e) {
			// SQL Code & SQL State
			handleException(tuple, e);

		} finally {
			commitLock.unlock();
		}
	}

	/**
	 * handleException
	 * @param tuple
	 * @param e
	 * @throws Exception
	 * @throws SQLException
	 * @throws IOException
	 */
	private void handleException(Tuple tuple, SQLException e) throws Exception, SQLException, IOException {
		JDBCSqlStatus jSqlStatus = new JDBCSqlStatus();
		// System.out.println(" sqlCode: " + e.getErrorCode() + " sqlState: " + e.getSQLState() + " sqlMessage: " + e.getMessage());
      		
        String sqlMessage = e.getMessage();

		jSqlStatus.setSqlCode(e.getErrorCode());
		jSqlStatus.setSqlState(e.getSQLState());
		jSqlStatus.setSqlMessage(sqlMessage);

		TRACE.log(TraceLevel.DEBUG, "SQL Exception SQL Code: " + jSqlStatus.getSqlCode());
		TRACE.log(TraceLevel.DEBUG, "SQL Exception SQL State: " + jSqlStatus.getSqlState());
		TRACE.log(TraceLevel.DEBUG, "SQL Exception SQL Message: " + jSqlStatus.getSqlMessage());
		
		
		if (hasErrorPort) {
			// submit error message
			submitErrorTuple(errorOutputPort, tuple, jSqlStatus);
			 // System.out.println("First Exception    sqlCode: " + jSqlStatus.getSqlCode() + " sqlState: " + jSqlStatus.getSqlState() + " sqlMessage: " + jSqlStatus.getSqlMessage());
		    // get next Exception message and sqlCode and submit it to the error output.
			SQLException eNext = e.getNextException();
			if(eNext != null) {
				jSqlStatus.setSqlCode(eNext.getErrorCode());
				jSqlStatus.setSqlState(eNext.getSQLState());
	  			jSqlStatus.setSqlMessage(eNext.getMessage());
				submitErrorTuple(errorOutputPort, tuple, jSqlStatus);
				// System.out.println("Next Exception    sqlCode: " + jSqlStatus.getSqlCode() + " sqlState: " + jSqlStatus.getSqlState() + " sqlMessage: " + jSqlStatus.getSqlMessage());
			}

		}

		// Check if JDBC connection valid
		if (!jdbcClientHelper.isValidConnection()) {
			// sqlFailureAction need not process if JDBC Connection is not valid      
			throw e;
		}
		if (sqlFailureAction.equalsIgnoreCase(IJDBCConstants.SQLFAILURE_ACTION_LOG)) {
			TRACE.log(TraceLevel.DEBUG, "SQL Failure - Log...");
			// The error is logged, and the error condition is cleared
			if((e.toString() != null ) && (e.toString().length() > 0)){
				TRACE.log(TraceLevel.WARNING, Messages.getString("JDBC_SQL_EXCEPTION_WARNING"), new Object[] { e.toString() }); 
			}
          	// Commit the transactions according to transactionSize
			if ((consistentRegionContext == null
					|| (consistentRegionContext != null 
					&& commitPolicy == CommitPolicy.OnTransactionAndCheckpoint))
					&& (transactionSize > 1) && (transactionCount >= transactionSize)) {
				TRACE.log(TraceLevel.DEBUG, "Transaction Commit...");
				transactionCount = 0;
				jdbcClientHelper.commit();
			}
		} else if (sqlFailureAction.equalsIgnoreCase(IJDBCConstants.SQLFAILURE_ACTION_ROLLBACK)) {
			TRACE.log(TraceLevel.DEBUG, "SQL Failure - Roll back...");
           	LOGGER.log(LogLevel.ERROR, Messages.getString("JDBC_SQL_EXCEPTION_ERROR"), new Object[] { e.toString() }); 
			if (consistentRegionContext != null) {
				// The error is logged, and request a reset of the consistent
				// region.
				consistentRegionContext.reset();
			} else {
				if (batchSize > 1) {
					// Clear statement batch & roll back the transaction
					jdbcClientHelper.rollbackWithClearBatch();
					// Reset the batch counter
					batchCount = 0;
				} else {
					jdbcClientHelper.rollback();
				}
				// Reset the transaction counter
				transactionCount = 0;
			}
		} else if (sqlFailureAction.equalsIgnoreCase(IJDBCConstants.SQLFAILURE_ACTION_TERMINATE)) {
			TRACE.log(TraceLevel.DEBUG, "SQL Failure - Shut down...");
			shutdown();
			// The error is logged and the operator terminates.
			LOGGER.log(LogLevel.ERROR, "SQL_EXCEPTION_ERROR", new Object[] { e.toString() });
			if (batchSize > 1) {
				// Clear statement batch & Roll back the transaction
				jdbcClientHelper.rollbackWithClearBatch();
				// Reset the batch counter
				batchCount = 0;
			} else {
				// Roll back the transaction
				jdbcClientHelper.rollback();
			}
			// Reset transaction counter
			transactionCount = 0;
			shutdown();
		}
	}

	// Reset JDBC connection
	@Override
	protected void resetJDBCConnection() throws Exception {
		// Reset JDBC connection
		jdbcClientHelper.resetConnection();
		// Initiate PreparedStatement
		initPreparedStatement();
	}

	// Initiate PreparedStatement
	private void initPreparedStatement() throws SQLException {
		if (statement != null) {
			isStaticStatement = true;
			TRACE.log(TraceLevel.DEBUG, "Initializing PreparedStatement: " + statement);
			jdbcClientHelper.initPreparedStatement(statement);
			TRACE.log(TraceLevel.DEBUG, "Initializing PreparedStatement - Completed");
		}
	}

	// Initiate sqlStatusAttr
	private void initSqlStatusAttr() {
		if (sqlStatusAttr != null) {
			ArrayList<String> arrDataAttrs = new ArrayList<String>();
			ArrayList<String> arrErrorAttrs = new ArrayList<String>();

			String sqlStatus[] = sqlStatusAttr.split(",");
			for (int i = 0; i < sqlStatus.length; i++) {
				String strSqlStatus = sqlStatus[i].trim();
				if (!strSqlStatus.isEmpty()) {

					TRACE.log(TraceLevel.DEBUG, "SQL Status Attr: " + strSqlStatus);
					int found = findSqlStatusAttr(dataOutputPort, errorOutputPort, strSqlStatus);

					// Found sqlStatusAttr on data port
					if (found == 0) {
						if (strSqlStatus.contains(".")) {
							// The attribute is a full qualified name
							String strs[] = strSqlStatus.split("\\.");
							arrDataAttrs.add(strs[1]);
						} else {
							arrDataAttrs.add(strSqlStatus);
						}
						TRACE.log(TraceLevel.DEBUG, "The attribute added to data output stream: " + strSqlStatus);
					}
					if (found == 1) {
						if (strSqlStatus.contains(".")) {
							// The attribute is a full qualified name
							String strs[] = strSqlStatus.split("\\.");
							arrErrorAttrs.add(strs[1]);
						} else {
							arrErrorAttrs.add(strSqlStatus);
						}
						TRACE.log(TraceLevel.DEBUG, "The attribute added to error output stream: " + strSqlStatus);
					}

				}
			}

			// Convert to string array
			if (arrDataAttrs.size() > 0) {
				sqlStatusDataAttrs = new String[arrDataAttrs.size()];
				arrDataAttrs.toArray(sqlStatusDataAttrs);
			}
			if (arrErrorAttrs.size() > 0) {
				sqlStatusErrorAttrs = new String[arrErrorAttrs.size()];
				arrErrorAttrs.toArray(sqlStatusErrorAttrs);
			}
		}
	}

	// Return SPL value according to SPL type
	protected Object getSplValue(Attribute attribute, Tuple tuple) {

		Type splType = attribute.getType();
		int index = attribute.getIndex();
		
		if (splType.getMetaType() == MetaType.INT8)
			return tuple.getByte(index);
		if (splType.getMetaType() == MetaType.INT16)
			return tuple.getShort(index);
		if (splType.getMetaType() == MetaType.INT32)
			return tuple.getInt(index);
		if (splType.getMetaType() == MetaType.INT64)
			return tuple.getLong(index);

		if (splType.getMetaType() == MetaType.UINT8)
			return tuple.getByte(index);
		if (splType.getMetaType() == MetaType.UINT16)
			return tuple.getShort(index);
		if (splType.getMetaType() == MetaType.UINT32)
			return tuple.getInt(index);
		if (splType.getMetaType() == MetaType.UINT64)
			return tuple.getLong(index);

		if (splType.getMetaType() == MetaType.BLOB)
			return tuple.getBlob(index);

		if (splType.getMetaType() == MetaType.BOOLEAN)
			return tuple.getBoolean(index);

		if (splType.getMetaType() == MetaType.DECIMAL32)
			return tuple.getBigDecimal(index);
		if (splType.getMetaType() == MetaType.DECIMAL64)
			return tuple.getBigDecimal(index);
		if (splType.getMetaType() == MetaType.DECIMAL128)
			return tuple.getBigDecimal(index);

		if (splType.getMetaType() == MetaType.FLOAT32)
			return tuple.getFloat(index);
		if (splType.getMetaType() == MetaType.FLOAT64)
			return tuple.getDouble(index);

		if (splType.getMetaType() == MetaType.RSTRING)
			return tuple.getString(index);
		if (splType.getMetaType() == MetaType.USTRING)
			return tuple.getString(index);

		if (splType.getMetaType() == MetaType.TIMESTAMP)
			return tuple.getTimestamp(index).getSQLTimestamp();

		if (splType.getMetaType() == MetaType.XML)
			return tuple.getXML(index);

		// Task 39870 Update JDBC toolkit with respect to optional data type support
		// it compares the contain of SPL type delivers by SPL application with data types 
		// The "MetaType.OPTIONAL)" and "tuple.getOptional" was not used due of 
		// compatibility with older Streams Version without optional type
		if(splType.getLanguageType().toUpperCase().contains("OPTIONAL"))
			return tuple.getObject(index);
/*
		if (splType.getMetaType() == MetaType.OPTIONAL)
		{
			if ((tuple.getOptional(index, attribute.getType().getAsCompositeElementType()).isPresent()))
		    {
		    	return tuple.getOptional(index, attribute.getType().getAsCompositeElementType()).get();
		    }
		    else
		    	return null;
		}
*/					
		LOGGER.log(LogLevel.ERROR, Messages.getString("JDBC_SPL_TYPE_NOT_SUPPORT"), splType.getMetaType()); 
		return null;

	}

	// Create StatementParameter object arrays
	protected StatementParameter[] getStatementParameterArrays(StatementParameter[] stmtParameterArrays, Tuple tuple) {
		if (stmtParameterArrays != null) {
			for (int i = 0; i < stmtParameterArrays.length; i++) {
				TRACE.log(TraceLevel.DEBUG,
						"Parameter statementParameter Name: " + stmtParameterArrays[i].getSplAttributeName());
				Attribute attribute = tuple.getStreamSchema()
						.getAttribute(stmtParameterArrays[i].getSplAttributeName().trim());
				if (attribute == null) {
					LOGGER.log(LogLevel.ERROR, "STATEMENT_PARAMETER_NOT_EXIST",
							stmtParameterArrays[i].getSplAttributeName());
				} else {
					stmtParameterArrays[i].setSplAttribute(attribute);
					stmtParameterArrays[i].setSplValue(getSplValue(stmtParameterArrays[i].getSplAttribute(), tuple));
					TRACE.log(TraceLevel.DEBUG,
							"Parameter statementParameters Value: " + stmtParameterArrays[i].getSplValue());
				}
			}
		}
		return stmtParameterArrays;
	}

	// Submit output tuple according to result set
	protected void submitOutputTuple(StreamingOutput<OutputTuple> outputPort, Tuple inputTuple, ResultSet rs,
			JDBCSqlStatus jSqlStatus) throws Exception {

		OutputTuple outputTuple = outputPort.newTuple();

		// Pass all incoming attributes as is to the output tuple
		outputTuple.assign(inputTuple);

		// Get the schema for the output tuple type
		StreamSchema schema = outputTuple.getStreamSchema();

		// Assign hasResultSet value according to hasResultSetAttr parameter
		if (hasResultSetAttr != null) {
			TRACE.log(TraceLevel.DEBUG, "hasResultSet: " + hasResultSetValue);
			outputTuple.setBoolean(hasResultSetAttr, hasResultSetValue);
		}

		// Assign SQL status according to sqlStatusAttr parameter
		if (sqlStatusDataAttrs != null && sqlStatusDataAttrs.length > 0 && jSqlStatus != null) {
			TRACE.log(TraceLevel.DEBUG, "Assign SQL status information");
			for (int i = 0; i < sqlStatusDataAttrs.length; i++) {
				Attribute attr = schema.getAttribute(sqlStatusDataAttrs[i]);
				TupleType dTupleType = (TupleType) attr.getType();
				StreamSchema dSchema = dTupleType.getTupleSchema();
				// Create a tuple with desired value
				Map<String, Object> attrmap = new HashMap<String, Object>();
				attrmap.put("sqlCode", jSqlStatus.getSqlCode());
				if (jSqlStatus.getSqlState() != null)
					attrmap.put("sqlState", new RString(jSqlStatus.getSqlState()));
				if (jSqlStatus.getSqlMessage() != null) 
					attrmap.put("sqlMessage", new RString(jSqlStatus.getSqlMessage()));
				Tuple sqlStatusT = dSchema.getTuple(attrmap);
				// Assign the values to the output tuple
				outputTuple.setObject(sqlStatusErrorAttrs[i], sqlStatusT);
			}
		}

		// Assign values from result set
		if (rs != null) {
			ResultSetMetaData rsmd = rs.getMetaData();
			for (int i = 1; i <= rsmd.getColumnCount(); i++) {
				String columnName = rsmd.getColumnName(i);
				Attribute attr = schema.getAttribute(columnName);
				if (attr != null) {
					rs.getObject(i);
					if (!rs.wasNull()) {
						String splAttrName = attr.getName();
						String splType = (attr.getType().getLanguageType()).toUpperCase();
						// Task 39870 Update JDBC toolkit with respect to optional data type support
						// it compares the contain of SPL type delivers by SPL application with data types 
						// and assign value from result set
						// in this case it assign the int32 values for SPL type int32 or optional<int32>
						if (splType.contains("RSTRING")) outputTuple.setString(splAttrName, rs.getString(i));
						else if (splType.contains("USTRING")) outputTuple.setString(splAttrName, rs.getString(i));
						else if (splType.contains("INT8")) outputTuple.setByte(splAttrName, rs.getByte(i));
						else if (splType.contains("INT16")) outputTuple.setShort(splAttrName, rs.getShort(i));
						else if (splType.contains("INT32")) outputTuple.setInt(splAttrName, rs.getInt(i));
						else if (splType.contains("INT64")) outputTuple.setLong(splAttrName, rs.getLong(i));
						else if (splType.contains("UINT8")) outputTuple.setByte(splAttrName, rs.getByte(i));
						else if (splType.contains("UINT16")) outputTuple.setShort(splAttrName, rs.getShort(i));
						else if (splType.contains("UINT32")) outputTuple.setInt(splAttrName, rs.getInt(i));
						else if (splType.contains("UINT64")) outputTuple.setLong(splAttrName, rs.getLong(i));
						else if (splType.contains("FLOAT32")) outputTuple.setFloat(splAttrName, rs.getFloat(i));
						else if (splType.contains("FLOAT64")) outputTuple.setDouble(splAttrName, rs.getDouble(i));
						else if (splType.contains("DECIMAL32")) outputTuple.setBigDecimal(splAttrName, rs.getBigDecimal(i));
						else if (splType.contains("DECIMAL64")) outputTuple.setBigDecimal(splAttrName, rs.getBigDecimal(i));
						else if (splType.contains("DECIMAL128")) outputTuple.setBigDecimal(splAttrName, rs.getBigDecimal(i));
						else if (splType.contains("BLOB")) outputTuple.setBlob(splAttrName, ValueFactory.readBlob(rs.getBlob(i).getBinaryStream()));
						else if (splType.contains("TIMESTAMP")) outputTuple.setTimestamp(splAttrName, Timestamp.getTimestamp(rs.getTimestamp(i)));
						else if (splType.contains("XML")) outputTuple.setXML(splAttrName, (XML)rs.getSQLXML(i));
						else if (splType.contains("BOOLEAN")) outputTuple.setBoolean(splAttrName, rs.getBoolean(i));
						else LOGGER.log(LogLevel.ERROR, Messages.getString("JDBC_SPL_TYPE_NOT_SUPPORT"), splType); 
					}
				}
			}
		}
		// Submit result set as output tuple
		outputPort.submit(outputTuple);
	}

	// Submit error tuple
	protected void submitErrorTuple(StreamingOutput<OutputTuple> errorOutputPort, Tuple inputTuple,
			JDBCSqlStatus jSqlStatus) throws Exception {
		OutputTuple errorTuple = errorOutputPort.newTuple();

		TRACE.log(TraceLevel.DEBUG, "Submit error tuple...");

		// Assign SQL status according to sqlStatusAttr parameter
		if (sqlStatusErrorAttrs != null && sqlStatusErrorAttrs.length > 0 && jSqlStatus != null) {
			TRACE.log(TraceLevel.DEBUG, "Assign SQL status information");
			StreamSchema schema = errorTuple.getStreamSchema();
			for (int i = 0; i < sqlStatusErrorAttrs.length; i++) {
				Attribute attr = schema.getAttribute(sqlStatusErrorAttrs[i]);
				TupleType dTupleType = (TupleType) attr.getType();
				StreamSchema dSchema = dTupleType.getTupleSchema();
				// Create a tuple with desired value
				Map<String, Object> attrmap = new HashMap<String, Object>();
				attrmap.put("sqlCode", jSqlStatus.getSqlCode());
				TRACE.log(TraceLevel.DEBUG, "Submit error tuple, sql code: " + jSqlStatus.getSqlCode());
				if (jSqlStatus.getSqlState() != null) {
					attrmap.put("sqlState", new RString(jSqlStatus.getSqlState()));
					TRACE.log(TraceLevel.DEBUG, "Submit error tuple, sql state: " + jSqlStatus.getSqlState());
				}
				if (jSqlStatus.getSqlMessage() != null) {
					attrmap.put("sqlMessage", new RString(jSqlStatus.getSqlMessage()));
					TRACE.log(TraceLevel.DEBUG, "Submit error tuple, sql message: " + jSqlStatus.getSqlMessage());
				}
				Tuple sqlStatusT = dSchema.getTuple(attrmap);
				// Assign the values to the output tuple
				errorTuple.setObject(sqlStatusErrorAttrs[i], sqlStatusT);
			}
		}

		// Copy across all matching attributes.
		Tuple embeddedInputTuple = errorTuple.getTuple(0);
		if (embeddedInputTuple != null && inputTuple != null) {
			StreamSchema embeddedSchema = embeddedInputTuple.getStreamSchema();
			Tuple embeddedTuple = embeddedSchema.getTuple(inputTuple);
			errorTuple.setTuple(0, embeddedTuple);
		}

		// Submit error tuple to error output port
		errorOutputPort.submit(errorTuple);
	}

	@Override
	public synchronized void shutdown() throws Exception {

		if (batchSize > 1) {
			if (isStaticStatement) {
				jdbcClientHelper.clearPreparedStatementBatch();
				;
			} else {
				jdbcClientHelper.clearStatementBatch();
			}
		}

		if (commitThread != null) {
			commitThread.cancel(false);
		}

		// stop checkConnectionThread
		if (checkConnectionThread != null) {
			if (checkConnectionThread.isAlive()) {
				checkConnectionThread.interrupt();
			}
		}

		// stop idleSessionTimeOutThread
		if (idleSessionTimeOutThread != null) {
			if (idleSessionTimeOutThread.isAlive()) {
				idleSessionTimeOutThread.interrupt();
			}
		}

		
		super.shutdown();

	}

	@Override
	public void checkpoint(Checkpoint checkpoint) throws Exception {
		LOGGER.log(LogLevel.INFO, Messages.getString("JDBC_CR_CHECKPOINT"), checkpoint.getSequenceId()); 

		// Commit the transaction
		TRACE.log(TraceLevel.DEBUG, "Transaction Commit...");
		jdbcClientHelper.commit();
		transactionCount = 0;
		
		// Save current batch information
		if (batchSize > 1) {
			TRACE.log(TraceLevel.DEBUG, "Checkpoint batchCount: " + batchCount);
			checkpoint.getOutputStream().writeInt(batchCount);
			if (isStaticStatement) {
				TRACE.log(TraceLevel.DEBUG, "Checkpoint preparedStatement");
				checkpoint.getOutputStream().writeObject(jdbcClientHelper.getPreparedStatement());
			} else {
				TRACE.log(TraceLevel.DEBUG, "Checkpoint statement");
				checkpoint.getOutputStream().writeObject(jdbcClientHelper.getStatement());
			}
		}	
	}

	@Override
	public void reset(Checkpoint checkpoint) throws Exception {
		LOGGER.log(LogLevel.INFO, Messages.getString("JDBC_CR_RESET"), checkpoint.getSequenceId()); 

		// Roll back the transaction
		jdbcClientHelper.rollback();

		// Reset the batch information
		if (batchSize > 1) {
			batchCount = checkpoint.getInputStream().readInt();
			TRACE.log(TraceLevel.DEBUG, "Reset batchCount: " + batchCount);
			if (isStaticStatement) {
				jdbcClientHelper.setPreparedStatement((PreparedStatement) checkpoint.getInputStream().readObject());
				TRACE.log(TraceLevel.DEBUG, "Reset preparedStatement");
			} else {
				jdbcClientHelper.setStatement((Statement) checkpoint.getInputStream().readObject());
				TRACE.log(TraceLevel.DEBUG, "Reset statement");
			}
		}
	}

	@Override
	public void resetToInitialState() throws Exception {
		LOGGER.log(LogLevel.INFO, Messages.getString("JDBC_RESET_TO_INITIAL")); 
		if (batchSize > 1) {
			jdbcClientHelper.rollbackWithClearBatch();
			batchCount = 0;
		} else {
			jdbcClientHelper.rollback();
		}
	}

	/**
	 * allPortsReady
	 * @throws Exception
	 */
	@Override
	public void allPortsReady() throws Exception {
		if ((consistentRegionContext == null 
				|| (consistentRegionContext != null 
				&& commitPolicy == CommitPolicy.OnTransactionAndCheckpoint)) 
			&& commitInterval > 0) {
			commitThread = getOperatorContext().getScheduledExecutorService().scheduleAtFixedRate(new Runnable() {
				@Override
				public void run() {
					commitLock.lock();
					try {
						synchronized (this) {
							if (batchSize > 1) {
								batchCount = 0;
								if (isStaticStatement) {
									jdbcClientHelper.executePreparedStatementBatch();
								} else {
									jdbcClientHelper.executeStatementBatch();
								}
							}

							TRACE.log(TraceLevel.DEBUG, "Transaction Commit...");
							transactionCount = 0;
							jdbcClientHelper.commit();
						}
					} catch (SQLException e) {
						try {
							handleException(null, e);
						} catch (SQLException e1) {
							e1.printStackTrace();
						} catch (IOException e1) {
							e1.printStackTrace();
						} catch (Exception e1) {
							e1.printStackTrace();
						} finally {
							commitLock.unlock();
						}
					} finally {
						commitLock.unlock();
					}
				}
			}, 0, commitInterval, TimeUnit.MILLISECONDS);
		}

		super.allPortsReady();
	}
}
