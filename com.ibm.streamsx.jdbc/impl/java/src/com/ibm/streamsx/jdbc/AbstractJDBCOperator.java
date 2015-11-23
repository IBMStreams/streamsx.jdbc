/*******************************************************************************
 * Copyright (C) 2015 International Business Machines Corporation
 * All Rights Reserved
 *******************************************************************************/
package com.ibm.streamsx.jdbc;


import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.MalformedURLException;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Logger;

import com.ibm.json.java.JSONObject;
import com.ibm.streams.operator.AbstractOperator;
import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.OperatorContext.ContextCheck;
import com.ibm.streams.operator.StreamingData.Punctuation;
import com.ibm.streams.operator.StreamingInput;
import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.compile.OperatorContextChecker;
import com.ibm.streams.operator.logging.LoggerNames;
import com.ibm.streams.operator.logging.TraceLevel;
import com.ibm.streams.operator.model.Parameter;
import com.ibm.streams.operator.state.Checkpoint;
import com.ibm.streams.operator.state.ConsistentRegionContext;
import com.ibm.streams.operator.state.StateHandler;

/**
 * AbstractJDBCOperator provides the base class for all JDBC operators.
 */
public abstract class AbstractJDBCOperator extends AbstractOperator implements StateHandler{
	
	private static final String PACKAGE_NAME = "com.ibm.streamsx.jdbc";

	/**
	 * Create a logger specific to this class
	 */
	protected static Logger LOGGER = Logger.getLogger(LoggerNames.LOG_FACILITY
		+ "." + PACKAGE_NAME, "com.ibm.streamsx.jdbc.JDBCMessages");
	
	// logger for trace/debug information
	protected static Logger TRACE = Logger.getLogger(PACKAGE_NAME);
	
	/**
	 * Define operator parameters
	 */
	// This parameter specifies the jdbc driver lib.
	private String jdbcDriverLib;
	// This parameter specifies the class name for jdbc driver.
	private String jdbcClassName;
	// This parameter specifies the database url.
	private String jdbcUrl;
	// This parameter specifies the database user on whose behalf the connection is being made.
	private String jdbcUser;
	// This parameter specifies the user's password.
	private String jdbcPassword;
	// This parameter specifies the path name of the file that contains the jdbc connection properties.
	private String jdbcProperties;
	// This parameter specifies the transaction isolation level at which statement runs.
	// If omitted, the statement runs at level READ_UNCOMMITTED
	private String isolationLevel = IJDBCConstants.TRANSACTION_READ_UNCOMMITTED;
	// This parameter specifies the actions when SQL failure.
	protected String sqlFailureAction = IJDBCConstants.SQLFAILURE_ACTION_LOG;
	// This optional parameter reconnectionPolicy specifies the reconnection policy
	// that would be applicable during initial/intermittent connection failures.
	// The valid values for this parameter are NoRetry, BoundedRetry and InfiniteRetry.
	// If not specified, it is set to BoundedRetry with a reconnectionBound of 5
	// and a period of 60 seconds
	private String reconnectionPolicy = IJDBCConstants.RECONNPOLICY_BOUNDEDRETRY; 
	// This optional parameter reconnectionBound specifies the number of successive connection
	// that will be attempted for this operator.
	// It can appear only when the reconnectionPolicy parameter is set to BoundedRetry
	// and cannot appear otherwise.
	// If not present the default value is 5
	private int reconnectionBound = IJDBCConstants.RECONN_BOUND_DEFAULT;
	// This optional parameter reconnectionInterval specifies the time period in seconds which
	// the operator will be wait before trying to reconnect.
	// If not specified, the default value is 10.0.
	private double reconnectionInterval = IJDBCConstants.RECONN_INTERVAL_DEFAULT;

	// Create an instance of JDBCConnectionhelper
	protected JDBCConnectionHelper jdbcConnectionHelper;
	
	// Lock for JDBC connection reset
	private ReadWriteLock lock = new ReentrantReadWriteLock();  
	
	// consistent region context
    protected ConsistentRegionContext consistentRegionContext;

	//Parameter jdbcDriverLib
	@Parameter(optional = false, description="This required parameter specifies the jdbc driver lib and it must have exactly one value of type rstring.")
    public void setJdbcDriverLib(String jdbcDriverLib){
    	this.jdbcDriverLib = jdbcDriverLib;
    }

	//Parameter jdbcClassName
	@Parameter(optional = false, description="This required parameter specifies the class name for jdbc driver and it must have exactly one value of type rstring.")
    public void setJdbcClassName(String jdbcClassName){
    	this.jdbcClassName = jdbcClassName;
    }
	
	//Parameter jdbcUrl
	@Parameter(optional = false, description="This parameter specifies the database url and it must have exactly one value of type rstring.")
    public void setJdbcUrl(String jdbcUrl){
    	this.jdbcUrl = jdbcUrl;
    }
	
	//Parameter jdbcUser
	@Parameter(optional = true, description="This optional parameter specifies the database user on whose behalf the connection is being made. If the jdbcUser parameter is specified, it must have exactly one value of type rstring.")
    public void setJdbcUser(String jdbcUser){
    	this.jdbcUser = jdbcUser;
    }
	
	//Parameter jdbcPassword
	@Parameter(optional = true, description="This optional parameter specifies the user’s password. If the jdbcPassword parameter is specified, it must have exactly one value of type rstring.")
    public void setJdbcPassword(String jdbcPassword){
    	this.jdbcPassword = jdbcPassword;
    }

	//Parameter jdbcProperties
	@Parameter(optional = true, description="This optional parameter specifies the path name of the file that contains the jdbc connection properties.")
    public void setJdbcProperties(String jdbcProperties){
    	this.jdbcProperties = jdbcProperties;
    }

	//Parameter isolationLevel
	@Parameter(optional = true, description="This optional parameter specifies the transaction isolation level at which statement runs. If omitted, the statement runs at level READ_UNCOMMITTED.")
    public void setIsolationLevel(String isolationLevel){
    	this.isolationLevel = isolationLevel;
    }

	//Parameter sqlFailureAction
	@Parameter(optional = true, description="This optional parameter has values of log, rollback and terminate. If not specified, log is assumed. If sqlFailureAction is log, the error is logged, and the error condition is cleared. If sqlFailureAction is rollback, the error is logged, the transaction rolls back. If sqlFailureAction is terminate, the error is logged, the transaction rolls back and the operator terminates.")
    public void setSqlFailureAction(String sqlFailureAction){
    	this.sqlFailureAction = sqlFailureAction;
    }

	//Parameter reconnectionPolicy
	@Parameter(optional = true, description="This optional parameter specifies the policy that is used by the operator to handle database connection failures.  The valid values are: `NoRetry`, `InfiniteRetry`, and `BoundedRetry`. The default value is `BoundedRetry`. If `NoRetry` is specified and a database connection failure occurs, the operator does not try to connect to the database again.  The operator shuts down at startup time if the initial connection attempt fails. If `BoundedRetry` is specified and a database connection failure occurs, the operator tries to connect to the database again up to a maximum number of times. The maximum number of connection attempts is specified in the **reconnectionBound** parameter.  The sequence of connection attempts occurs at startup time. If a connection does not exist, the sequence of connection attempts also occurs before each operator is run.  If `InfiniteRetry` is specified, the operator continues to try and connect indefinitely until a connection is made.  This behavior blocks all other operator operations while a connection is not successful.  For example, if an incorrect connection password is specified in the connection configuration document, the operator remains in an infinite startup loop until a shutdown is requested.")
    public void setReconnectionPolicy(String reconnectionPolicy){
    	this.reconnectionPolicy = reconnectionPolicy;
    }

	//Parameter reconnectionBound
	@Parameter(optional = true, description="This optional parameter specifies the number of successive connection attempts that occur when a connection fails or a disconnect occurs.  It is used only when the **reconnectionPolicy** parameter is set to `BoundedRetry`; otherwise, it is ignored. The default value is `5`.")
    public void setReconnectionBound(int reconnectionBound){
    	this.reconnectionBound = reconnectionBound;
    }

	//Parameter reconnectionBound
	@Parameter(optional = true, description="This optional parameter specifies the amount of time (in seconds) that the operator waits between successive connection attempts.  It is used only when the **reconnectionPolicy** parameter is set to `BoundedRetry` or `InfiniteRetry`; othewise, it is ignored.  The default value is `10`.")
    public void setReconnectionInterval(double reconnectionInterval){
    	this.reconnectionInterval = reconnectionInterval;
    }

	/*
	 * The method checkParametersRuntime
	 */
	@ContextCheck(compile = false)
	public static void checkParametersRuntime(OperatorContextChecker checker) {
		
		OperatorContext context = checker.getOperatorContext();
		
		// Check reconnection related parameters at runtime
		if ((context.getParameterNames().contains("reconnectionBound"))) {
			// reconnectionBound value should be non negative.
			if (Integer.parseInt(context.getParameterValues("reconnectionBound").get(0)) < 0) {
    			LOGGER.log(TraceLevel.ERROR, "REC_BOUND_NEG");
				checker.setInvalidContext("reconnectionBound value {0} should be zero or greater than zero  ",
						new String[] { context.getParameterValues(
								"reconnectionBound").get(0) });
			}
			if (context.getParameterNames().contains("reconnectionPolicy")) {
				// reconnectionPolicy can be either InfiniteRetry, NoRetry,
				// BoundedRetry
				String strReconnectionPolicy = context.getParameterValues("reconnectionPolicy").get(0).trim();
				// reconnectionBound can appear only when the reconnectionPolicy
				// parameter is set to BoundedRetry and cannot appear otherwise
				if (! strReconnectionPolicy.equalsIgnoreCase(IJDBCConstants.RECONNPOLICY_BOUNDEDRETRY)) {
	    			LOGGER.log(TraceLevel.ERROR, "REC_BOUND_NOT_ALLOWED");
					checker.setInvalidContext("reconnectionBound {0} can appear only when the reconnectionPolicy parameter is set to BoundedRetry and cannot appear otherwise ",
							new String[] { context.getParameterValues(
									"reconnectionBound").get(0) });
				}
			}
		}

	}

	/*
	 * The method checkParameters
	 */
	@ContextCheck(compile = true)
	public static void checkParameters(OperatorContextChecker checker) {
		// If jdbcProperties is set as parameter, jdbcUser and jdbcPassword can not be set 
		checker.checkExcludedParameters("jdbcUser", "jdbcProperties");
		checker.checkExcludedParameters("jdbcPassword", "jdbcProperties");
		// check reconnection related parameters
		checker.checkDependentParameters("reconnecionInterval", "reconnectionPolicy");
		checker.checkDependentParameters("reconnecionBound", "reconnectionPolicy");
	}	

    /**
     * Initialize this operator. Called once before any tuples are processed.
     * @param context OperatorContext for this operator.
     * @throws Exception Operator failure, will cause the enclosing PE to terminate.
     */
	@Override
	public synchronized void initialize(OperatorContext context)
			throws Exception {
		super.initialize(context);
		TRACE.log(TraceLevel.DEBUG, "Operator " + context.getName() + " initializing in PE: " + context.getPE().getPEId() + " in Job: " + context.getPE().getJobId());
		
		// set up JDBC driver class path
		TRACE.log(TraceLevel.DEBUG, "Operator " + context.getName() + " setting up class path...");
		setupClassPath(context);
		TRACE.log(TraceLevel.DEBUG, "Operator " + context.getName() + " setting up class path - Completed");
		
		consistentRegionContext = context.getOptionalContext(ConsistentRegionContext.class);
		
		// Create the JDBC connection
		TRACE.log(TraceLevel.DEBUG, "Operator " + context.getName() + " setting up JDBC connection...");
		setupJDBCconnect();
		TRACE.log(TraceLevel.DEBUG, "Operator " + context.getName() + " Setting up JDBC connection - Completed");
	}

    /**
     * Process an incoming tuple that arrived on the specified port.
     * <P>
     * Copy the incoming tuple to a new output tuple and submit to the output port. 
     * </P>
     * @param inputStream Port the tuple is arriving on.
     * @param tuple Object representing the incoming tuple.
     * @throws Exception Operator failure, will cause the enclosing PE to terminate.
     */
    @Override
    public void process(StreamingInput<Tuple> inputStream, Tuple tuple)
            throws Exception {
    	
    	if(inputStream.isControl()) {
    		TRACE.log(TraceLevel.DEBUG, "Process control port...");
    		lock.writeLock().lock();
    		try{
    			processControlPort(inputStream, tuple);
    		}finally{
    			lock.writeLock().unlock();
    		}
			TRACE.log(TraceLevel.DEBUG, "Process control port - Completed");
		}else{
			TRACE.log(TraceLevel.DEBUG, "Process input tuple...");
			lock.readLock().lock();
			try{
				processTuple(inputStream, tuple);
			}finally{
				lock.readLock().unlock();
			}
			TRACE.log(TraceLevel.DEBUG, "Process input tuple - Completed");
		}
    	
    }
	
    // Process input tuple
    protected abstract void processTuple (StreamingInput<Tuple> stream, Tuple tuple) throws Exception;
	
    // Process control port
    // The port allows operator to change JDBC connection information at runtime
    // The port expects a value with JSON format
	protected void processControlPort(StreamingInput<Tuple> stream, Tuple tuple) throws Exception{
		
		String jsonString = tuple.getString(0);
		try {
			JSONObject jdbcConnections = JSONObject.parse(jsonString);
			String jdbcClassName = (String)jdbcConnections.get("jdbcClassName");
			String jdbcUrl = (String)jdbcConnections.get("jdbcUrl");
			String jdbcUser = (String)jdbcConnections.get("jdbcUser");
			String jdbcPassword = (String)jdbcConnections.get("jdbcPassword");
			String jdbcProperties = (String)jdbcConnections.get("jdbcProperties");
			
			// jdbcClassName is required
			if (jdbcClassName == null || jdbcClassName.trim().isEmpty()){
				LOGGER.log(TraceLevel.ERROR, "JDBCCLASSNAME_NOT_EXIST");
			}
			// jdbcUrl is required
			if (jdbcUrl == null || jdbcUrl.trim().isEmpty()){
				LOGGER.log(TraceLevel.ERROR, "JDBCURL_NOT_EXIST");
			}
			// if jdbcProperties is relative path, convert to absolute path
			if (jdbcProperties != null && !jdbcProperties.trim().isEmpty() && !jdbcProperties.startsWith(File.separator))
			{
				jdbcProperties = getOperatorContext().getPE().getApplicationDirectory() + File.separator + jdbcProperties;
			}
	        // Reset JDBC connection
			jdbcConnectionHelper.resetConnection(jdbcClassName, jdbcUrl, jdbcUser, jdbcPassword, jdbcProperties);
		}catch (FileNotFoundException e){
			LOGGER.log(TraceLevel.ERROR, "JDBCPROPERTIES_NOT_EXIST", jdbcProperties);
			throw e;
		}catch (Exception e){
			LOGGER.log(TraceLevel.ERROR, "RESET_CONNECTION_FAILED", e.toString());
			throw e;
		}			
	}
    
    /**
     * Process an incoming punctuation that arrived on the specified port.
     * @param stream Port the punctuation is arriving on.
     * @param mark The punctuation mark
     * @throws Exception Operator failure, will cause the enclosing PE to terminate.
     */
    @Override
    public void processPunctuation(StreamingInput<Tuple> stream,
    		Punctuation mark) throws Exception {
    	// For window markers, punctuate all output ports 
    	super.processPunctuation(stream, mark);
    }

    /**
     * Shutdown this operator.
     * @throws Exception Operator failure, will cause the enclosing PE to terminate.
     */
    public synchronized void shutdown() throws Exception {
        OperatorContext context = getOperatorContext();

        TRACE.log(TraceLevel.DEBUG, "Operator " + context.getName() + " shutting down in PE: " + context.getPE().getPEId() + " in Job: " + context.getPE().getJobId());
        
        // Roll back the transaction
        jdbcConnectionHelper.rollback();
        
        // close JDBC connection
        jdbcConnectionHelper.closeConnection();

        // Must call super.shutdown()
        super.shutdown();
        
    }

    // Set up JDBC driver class path
	private void setupClassPath(OperatorContext context) throws MalformedURLException{
		// if relative path, convert to absolute path
		if (!jdbcDriverLib.startsWith(File.separator))
		{
			jdbcDriverLib = getOperatorContext().getPE().getApplicationDirectory() + File.separator + jdbcDriverLib;
		}
		// JDBC driver path 
		context.addClassLibraries(new String[] {jdbcDriverLib});
		TRACE.log(TraceLevel.DEBUG, "Operator " + context.getName() + " JDBC Driver Lib: " + jdbcDriverLib);
	}

    
	// Set up JDBC connection
	private synchronized void setupJDBCconnect() throws Exception{
		
		// Initiate JDBCConnectionHelper instance
        TRACE.log(TraceLevel.DEBUG, "Create JDBC Connection, jdbcClassName: " + jdbcClassName);
        TRACE.log(TraceLevel.DEBUG, "Create JDBC Connection, jdbcUrl: " + jdbcUrl);
        TRACE.log(TraceLevel.DEBUG, "Create JDBC Connection, jdbcUser: " + jdbcUser);
        TRACE.log(TraceLevel.DEBUG, "Create JDBC Connection, jdbcPassword: " + jdbcPassword);
		// if jdbcProperties is relative path, convert to absolute path
		if (jdbcProperties != null && !jdbcProperties.trim().isEmpty() && !jdbcProperties.startsWith(File.separator))
		{
			jdbcProperties = getOperatorContext().getPE().getApplicationDirectory() + File.separator + jdbcProperties;
		}

		jdbcConnectionHelper = new JDBCConnectionHelper(jdbcClassName, jdbcUrl, jdbcUser, jdbcPassword, jdbcProperties, isAutoCommit(), isolationLevel, reconnectionPolicy, reconnectionBound, reconnectionInterval);

		jdbcConnectionHelper.createConnection();
	}

	// JDBC connection need to be auto-committed or not
	protected boolean isAutoCommit(){
        if (consistentRegionContext != null){
        	// Set automatic commit to false when it is a consistent region.
        	return false;
        }
		return true;
	}
	
	
	@Override
	public void close() throws IOException {
		LOGGER.log(TraceLevel.INFO, "CR_CLOSE");
	}

	@Override
	public void checkpoint(Checkpoint checkpoint) throws Exception {
		LOGGER.log(TraceLevel.INFO, "CR_CHECKPOINT", checkpoint.getSequenceId());

		jdbcConnectionHelper.commit();
	}

	@Override
	public void drain() throws Exception {
		LOGGER.log(TraceLevel.INFO, "CR_DRAIN");
	}

	@Override
	public void reset(Checkpoint checkpoint) throws Exception {
		LOGGER.log(TraceLevel.INFO, "CR_RESET", checkpoint.getSequenceId());
	
		jdbcConnectionHelper.rollback();
	}

	@Override
	public void resetToInitialState() throws Exception {
		LOGGER.log(TraceLevel.INFO, "RESET_TO_INITIAL");
		
		jdbcConnectionHelper.rollback();
	}

	@Override
	public void retireCheckpoint(long id) throws Exception {
		LOGGER.log(TraceLevel.INFO, "CR_RETIRE");
	}
}