/*******************************************************************************
 * Copyright (C) 2015-2018 International Business Machines Corporation
 * All Rights Reserved
 *******************************************************************************/
package com.ibm.streamsx.jdbc;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.MalformedURLException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Logger;

import com.ibm.json.java.JSONObject;
import com.ibm.streams.operator.AbstractOperator;
import com.ibm.streams.operator.Attribute;
import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.StreamSchema;
import com.ibm.streams.operator.OperatorContext.ContextCheck;
import com.ibm.streams.operator.StreamingData.Punctuation;
import com.ibm.streams.operator.Type.MetaType;
import com.ibm.streams.operator.StreamingInput;
import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.compile.OperatorContextChecker;
import com.ibm.streams.operator.logging.LoggerNames;
import com.ibm.streams.operator.logging.TraceLevel;
import com.ibm.streams.operator.logging.LogLevel;
import com.ibm.streams.operator.model.Parameter;
import com.ibm.streams.operator.state.Checkpoint;
import com.ibm.streams.operator.state.ConsistentRegionContext;
import com.ibm.streams.operator.state.StateHandler;

/**
 * AbstractJDBCOperator provides the base class for all JDBC operators.
 */
public abstract class AbstractJDBCOperator extends AbstractOperator implements StateHandler{

	private static final String PACKAGE_NAME = "com.ibm.streamsx.jdbc";
	private static final String CLASS_NAME = "com.ibm.streamsx.jdbc.AbstractJDBCOperator";

	/**
	 * Create a logger specific to this class
	 */
	private static Logger LOGGER = Logger.getLogger(LoggerNames.LOG_FACILITY
		+ "." + CLASS_NAME); 

	// logger for trace/debug information
	protected static Logger TRACE = Logger.getLogger(PACKAGE_NAME);

	/**
	 * Define operator parameters 
	 */
	// This parameter specifies the path and the filename of jdbc driver libraries in one comma separated string).
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
	// This parameter specifies the json string that contains the jdbc credentials username, password, jdbcurl.
	private String credentials;
	// This parameter specifies the transaction isolation level at which statement runs.
	// If omitted, the statement runs at level READ_UNCOMMITTED
	private String isolationLevel = IJDBCConstants.TRANSACTION_READ_UNCOMMITTED;
	// This parameter specifies the actions when SQL failure.
	protected String sqlFailureAction = IJDBCConstants.SQLFAILURE_ACTION_LOG;
	// This optional parameter reconnectionPolicy specifies the reconnection policy
	// that would be applicable during initial/intermittent connection failures.
	// The valid values for this parameter are NoRetry, BoundedRetry and InfiniteRetry.
	// If not specified, it is set to BoundedRetry.
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
	private String pluginName = null;
	private int securityMechanism = -1;

	// Create an instance of JDBCConnectionhelper
	protected JDBCClientHelper jdbcClientHelper;

	// Lock (fair mode) for JDBC connection reset
	private ReadWriteLock lock = new ReentrantReadWriteLock(true);

	// consistent region context
    	protected ConsistentRegionContext consistentRegionContext;
    
	// The name of the application config object
	private String appConfigName = null;

	// data from application config object
    	Map<String, String> appConfig = null;

    
 	// SSL parameters
 	private String keyStore;
 	private String trustStore;
 	private String keyStoreType = null;
 	private String trustStoreType = null;
 	private String keyStorePassword = null;
 	private String trustStorePassword = null;
 	private boolean sslConnection;

	//Parameter jdbcDriverLib
	@Parameter(name = "jdbcDriverLib", optional = false, 
			description = "This required parameter of type rstring specifies the path and the file name of jdbc driver librarirs with comma separated in one string. It is recommended to set the value of this parameter without slash at begin, like 'opt/db2jcc4.jar'. In this case the SAB file will contain the driver libraries.\\n\\n"
						+ "Please check the documentation of database vendors and download the latest version of jdbc drivers. ")
    public void setJdbcDriverLib(String jdbcDriverLib){
    	this.jdbcDriverLib = jdbcDriverLib;
    }

	//Parameter jdbcClassName
	@Parameter(name = "jdbcClassName", optional = false, 
			description = "This required parameter specifies the class name for jdbc driver and it must have exactly one value of type rstring.\\n\\n" 
	                     + "The jdbc class names are defined by database vendors: \\n\\n"
					     + "For example: \\n\\n "
	                     + "**DB2**        com.ibm.db2.jcc.DB2Driver \\n\\n"
					     + "**ORACLE**     oracle.jdbc.driver.OracleDriver\\n\\n"
	                     + "**PostgreSQL** org.postgresql.Driver")
    public void setJdbcClassName(String jdbcClassName){
    	this.jdbcClassName = jdbcClassName;
    }

	//Parameter jdbcUrl
	@Parameter(name = "jdbcUrl", optional = true, 
			description = "This parameter specifies the database url that JDBC driver uses to connect to a database and it must have exactly one value of type rstring. The syntax of jdbc url is specified by database vendors. For example, jdbc:db2://<server>:<port>/<database>\\n\\n"
			+ "  **jdbc:db2** indicates that the connection is to a DB2 for z/OS, DB2 for Linux, UNIX, and Windows.\\n\\n"
			+ "  **server**, the domain name or IP address of the data source.\\n\\n"
			+ "  **port**, the TCP/IP server port number that is assigned to the data source.\\n\\n"
			+ "  **database**, a name for the data source.\\n\\n"
			+ " For details about the jdbcUrl string please check the documentation of database vendors\\n\\n"
			+ " This parameter can be overwritten by the **credentials** and **jdbcProperties** parameters."
			)
    public void setJdbcUrl(String jdbcUrl){
    	this.jdbcUrl = jdbcUrl;
    }

	//Parameter jdbcUser
	@Parameter(name = "jdbcUser", optional = true, 
			description = "This optional parameter specifies the database user on whose behalf the connection is being made. If the **jdbcUser** parameter is specified, it must have exactly one value of type rstring.\\n\\n"
			+ "This parameter can be overwritten by the **credentials** and **jdbcProperties** parameters."
			)
    public void setJdbcUser(String jdbcUser){
    	this.jdbcUser = jdbcUser;
    }

	//Parameter jdbcPassword
	@Parameter(name = "jdbcPassword", optional = true, 
			description = "This optional parameter specifies the user’s password. If the jdbcPassword parameter is specified, it must have exactly one value of type rstring. "
			+ ". This parameter can be overwritten by the **credentials** and **jdbcProperties** parameters."
			)
    public void setJdbcPassword(String jdbcPassword){
    	this.jdbcPassword = jdbcPassword;
    }

	//Parameter jdbcProperties
	@Parameter(name = "jdbcProperties", optional = true, 
			description = "This optional parameter specifies the path name of the file that contains the jdbc connection properties: **user**, **password** and **jdbcUrl**. \\n\\n "
					+ "It supports also 'username' or 'jdbcUser' as 'user' and 'jdbcPassword' as 'password' and 'jdbcurl' as 'jdbcUrl'.")
    public void setJdbcProperties(String jdbcProperties){
    	this.jdbcProperties = jdbcProperties;
    }

	//Parameter credentials
	@Parameter(name = "credentials", optional = true, 
			description = "This optional parameter specifies the JSON string that contains the jdbc credentials: **username**, **password** and **jdbcurl** or **jdbcUrl**. \\n\\n"
			+ "This parameter can also be specified in an application configuration.")
    public void setcredentials(String credentials){
    	this.credentials = credentials;
    }

	
	//Parameter isolationLevel
	@Parameter(name = "isolationLevel", optional = true, 
			description = "This optional parameter specifies the transaction isolation level at which statement runs. If omitted, the statement runs at level **READ_UNCOMMITTED**.")
    public void setIsolationLevel(String isolationLevel){
    	this.isolationLevel = isolationLevel;
    }

	//Parameter sqlFailureAction
	@Parameter(name = "sqlFailureAction", optional = true, 
			description = "This optional parameter has values of log, rollback and terminate. If not specified, log is assumed. \\n\\n"
					+ "If sqlFailureAction is **log**, the error is logged, and the error condition is cleared. \\n\\n"
					+ "If sqlFailureAction is **rollback**, the error is logged, the transaction rolls back. \\n\\n"
					+ "If sqlFailureAction is **terminate**, the error is logged, the transaction rolls back and the operator terminates.")
    public void setSqlFailureAction(String sqlFailureAction){
    	this.sqlFailureAction = sqlFailureAction;
    }

	//Parameter reconnectionPolicy
	@Parameter(name = "reconnectionPolicy", optional = true, 
			description = "This optional parameter specifies the policy that is used by the operator to handle database connection failures.  The valid values are: **NoRetry**, **InfiniteRetry**, and **BoundedRetry**. \\n\\n"
					    + "The default value is **BoundedRetry**. If **NoRetry** is specified and a database connection failure occurs, the operator does not try to connect to the database again.  \\n\\n"
					    + "The operator shuts down at startup time if the initial connection attempt fails. If **BoundedRetry** is specified and a database connection failure occurs, the operator tries to connect to the database again up to a maximum number of times. \\n\\n"
					    + "The maximum number of connection attempts is specified in the **reconnectionBound** parameter.  The sequence of connection attempts occurs at startup time. If a connection does not exist, the sequence of connection attempts also occurs before each operator is run. \\n\\n"
					    + "If **InfiniteRetry** is specified, the operator continues to try and connect indefinitely until a connection is made.  This behavior blocks all other operator operations while a connection is not successful.  \\n\\n"
					    + "For example, if an incorrect connection password is specified in the connection configuration document, the operator remains in an infinite startup loop until a shutdown is requested.")
    public void setReconnectionPolicy(String reconnectionPolicy){
    	this.reconnectionPolicy = reconnectionPolicy;
    }

	//Parameter reconnectionBound
	@Parameter(name = "reconnectionBound", optional = true, 
			description = "This optional parameter specifies the number of successive connection attempts that occur when a connection fails or a disconnect occurs.  It is used only when the **reconnectionPolicy** parameter is set to **BoundedRetry**; otherwise, it is ignored. The default value is **5**.")
    public void setReconnectionBound(int reconnectionBound){
    	this.reconnectionBound = reconnectionBound;
    }

	//Parameter reconnectionBound
	@Parameter(name = "reconnectionInterval", optional = true, 
			description = "This optional parameter specifies the amount of time (in seconds) that the operator waits between successive connection attempts.  It is used only when the **reconnectionPolicy** parameter is set to `BoundedRetry` or `InfiniteRetry`; othewise, it is ignored.  The default value is `10`.")
    public void setReconnectionInterval(double reconnectionInterval){
    	this.reconnectionInterval = reconnectionInterval;
    }

	//Parameter sslConnection
	@Parameter(name = "sslConnection", optional = true, 
			description = "This optional parameter specifies whether an SSL connection should be made to the database. When set to `true`, the **keyStore**, **keyStorePassword**, **trustStore** and **trustStorePassword** parameters can be used to specify the locations and passwords of the keyStore and trustStore. The default value is `false`.")
	public void setSslConnection(boolean sslConnection) {
		this.sslConnection = sslConnection;
	}

	public boolean isSslConnection() {
		return sslConnection;
	}

	// Parameter keyStore
	@Parameter(name = "keyStore" , optional = true, 
			description = "This optional parameter specifies the path to the keyStore. If a relative path is specified, the path is relative to the application directory. The **sslConnection** parameter must be set to `true` for this parameter to have any effect.")
	public void setKeyStore(String keyStore) {
		this.keyStore = keyStore;
	}

	public String getKeyStore() {
		return keyStore;
	}

	// Parameter keyStoreType
	@Parameter(name = "keyStoreType" , optional = true, 
			description = "This optional parameter specifies the type of the keyStore file, for example 'PKCS12'. The **sslConnection** parameter must be set to `true` for this parameter to have any effect.")
	public void setKeyStoreType(String keyStoreType) {
		this.keyStoreType = keyStoreType;
	}

	public String getKeyStoreType() {
		return keyStoreType;
	}
	
	// Parameter trustStoreType
	@Parameter(name = "trustStoreType" , optional = true, 
			description = "This optional parameter specifies the type of the trustStore file, for example 'PKCS12'. The **sslConnection** parameter must be set to `true` for this parameter to have any effect.")
	public void setTrustStoreType(String trustStoreType) {
		this.trustStoreType = trustStoreType;
	}

	public String getTrustStoreType() {
		return trustStoreType;
	}
	
	// Parameter keyStorePassword
	@Parameter(name = "keyStorePassword", optional = true, 
			description = "This parameter specifies the password for the keyStore given by the **keyStore** parameter. The **sslConnection** parameter must be set to `true` for this parameter to have any effect. This parameter can also be specified in an application configuration.")
	public void setKeyStorePassword(String keyStorePassword) {
		this.keyStorePassword = keyStorePassword;
	}

	public String getKeyStorePassword() {
		return keyStorePassword;
	}

	// Parameter trustStore
	@Parameter(name = "trustStore", optional = true, 
			description = "This optional parameter specifies the path to the trustStore. If a relative path is specified, the path is relative to the application directory. The **sslConnection** parameter must be set to `true` for this parameter to have any effect.")
	public void setTrustStore(String trustStore) {
		this.trustStore = trustStore;
	}

	public String getTrustStore() {
		return trustStore;
	}

	// Parameter trustStorePassword
	@Parameter(name = "trustStorePassword", optional = true, 
			description = "This parameter specifies the password for the trustStore given by the **trustStore** parameter. The **sslConnection** parameter must be set to `true` for this parameter to have any effect. This parameter can also be specified in an application configuration.")
	public void setTrustStorePassword(String trustStorePassword) {
		this.trustStorePassword = trustStorePassword;
	}

	public String getTrustStorePassword() {
		return trustStorePassword;
	}

	// Parameter appConfigName
	@Parameter(name = "appConfigName", optional = true,
			description = "Specifies the name of the application configuration that contains JDBC connection related configuration parameters. "
			+ " The 'credentials', 'keyStorePassword' and 'trustStorePassword' parameter can be set in an application configuration. "
			+ " If a value is specified in the application configuration and as operator parameter, the application configuration parameter value takes precedence. "
		)
		public void setAppConfigName(String appConfigName) {
			this.appConfigName = appConfigName;
		}	

	// Parameter pluginName
	@Parameter(name = "pluginName", optional = true, description = "Specifies the name of security plugin. The **sslConnection** parameter must be set to `true` for this parameter to have any effect.")
	public void setPluginName(String pluginName) {
		this.pluginName = pluginName;
	}
	
	// Parameter securityMechanism
	@Parameter(name = "securityMechanism", optional = true, description = "Specifies the value of securityMechanism as Integer. The **sslConnection** parameter must be set to `true` for this parameter to have any effect.")
	public void setSecurityMechanism(int securityMechanism) {
		this.securityMechanism = securityMechanism;
	}
		
	/*
	 * The method checkParametersRuntime
	 */
	@ContextCheck(compile = false)
	public static void checkParametersRuntime(OperatorContextChecker checker) {

		OperatorContext context = checker.getOperatorContext();

		String strReconnectionPolicy = "";
		if (context.getParameterNames().contains("reconnectionPolicy")) {
			// reconnectionPolicy can be either InfiniteRetry, NoRetry,
			// BoundedRetry
			strReconnectionPolicy = context.getParameterValues("reconnectionPolicy").get(0).trim();
			if (!(strReconnectionPolicy.equalsIgnoreCase(IJDBCConstants.RECONNPOLICY_NORETRY)
					|| strReconnectionPolicy.equalsIgnoreCase(IJDBCConstants.RECONNPOLICY_BOUNDEDRETRY)
				    || strReconnectionPolicy.equalsIgnoreCase(IJDBCConstants.RECONNPOLICY_INFINITERETRY))) {
				LOGGER.log(LogLevel.ERROR, "reconnectionPolicy has to be set to InfiniteRetry or NoRetry or BoundedRetry");
				checker.setInvalidContext("reconnectionPolicy has to be set to InfiniteRetry or NoRetry or BoundedRetry", new String[] { context.getParameterValues(
						"reconnectionPolicy").get(0) });
		
			}
		}
		
		// Check reconnection related parameters at runtime
		if ((context.getParameterNames().contains("reconnectionBound"))) {
			// reconnectionBound value should be non negative.
			if (Integer.parseInt(context.getParameterValues("reconnectionBound").get(0)) < 0) {
    			LOGGER.log(LogLevel.ERROR, Messages.getString("JDBC_REC_BOUND_NEG")); 
				checker.setInvalidContext(Messages.getString("JDBC_REC_BOUND_NOT_ZERO"), 
						new String[] { context.getParameterValues(
								"reconnectionBound").get(0) });
			}
			if (context.getParameterNames().contains("reconnectionPolicy")) {
				// reconnectionPolicy can be either InfiniteRetry, NoRetry,
				// BoundedRetry
				 strReconnectionPolicy = context.getParameterValues("reconnectionPolicy").get(0).trim();
				// reconnectionBound can appear only when the reconnectionPolicy
				// parameter is set to BoundedRetry and cannot appear otherwise
				if (! strReconnectionPolicy.equalsIgnoreCase(IJDBCConstants.RECONNPOLICY_BOUNDEDRETRY)) {
	    			LOGGER.log(LogLevel.ERROR, Messages.getString("JDBC_REC_BOUND_NOT_ALLOWED")); 
					checker.setInvalidContext(Messages.getString("JDBC_REC_BOUND_NOT_SET_RETRY"), 
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
		// If statement is set as parameter, statementAttr can not be set
		checker.checkExcludedParameters("statement", "statementAttr");
		// If jdbcProperties is set as parameter, jdbcUser, jdbcPassword and jdbcUrl can not be set
		checker.checkExcludedParameters("jdbcUser", "jdbcProperties");
		checker.checkExcludedParameters("jdbcPassword", "jdbcProperties");
		checker.checkExcludedParameters("jdbcUrl", "jdbcProperties");

		// If credentials is set as parameter, jdbcUser, jdbcPassword and jdbcUrl can not be set.
		checker.checkExcludedParameters("jdbcUser", "credentials");
		checker.checkExcludedParameters("jdbcPassword", "credentials");
		checker.checkExcludedParameters("jdbcUrl", "credentials");
		checker.checkExcludedParameters("credentials", "jdbcUrl");

		// If credentials is set as parameter, jdbcProperties can not be set
		checker.checkExcludedParameters("jdbcProperties", "credentials");
		
		// check reconnection related parameters
		checker.checkDependentParameters("reconnecionInterval", "reconnectionPolicy");
		checker.checkDependentParameters("reconnecionBound", "reconnectionPolicy");

		// check parameters jdbcUrl jdbcUser and jdbcPassword
		OperatorContext context = checker.getOperatorContext();
		if ((!context.getParameterNames().contains("credentials"))
				&& (!context.getParameterNames().contains("appConfigName"))
				&& (!context.getParameterNames().contains("jdbcUrl"))
				&& (!context.getParameterNames().contains("jdbcProperties"))) {
					checker.setInvalidContext(Messages.getString("JDBC_MISSING_JDBC_CRED_PARAM", "jdbcUrl", "jdbcUrl"), null);
			}				

		if ((!context.getParameterNames().contains("credentials"))
				&& (!context.getParameterNames().contains("appConfigName"))				
				&& (!context.getParameterNames().contains("jdbcProperties"))
				&& (!context.getParameterNames().contains("jdbcUser"))) {
					checker.setInvalidContext(Messages.getString("JDBC_MISSING_JDBC_CRED_PARAM", "jdbcUser", "jdbcUser"), null);
			}				
		if ((!context.getParameterNames().contains("credentials"))
				&& (!context.getParameterNames().contains("appConfigName"))				
				&& (!context.getParameterNames().contains("jdbcProperties"))
				&& (!context.getParameterNames().contains("jdbcPassword"))) {
					checker.setInvalidContext(Messages.getString("JDBC_MISSING_JDBC_CRED_PARAM", "jdbcPassword", "jdbcPassword"), null);
			}				

	}

	
	@ContextCheck
	public static void checkControlPortInputAttribute(OperatorContextChecker checker) {
		OperatorContext context = checker.getOperatorContext();

		if(context.getNumberOfStreamingInputs() == 2) {
			StreamSchema schema = context.getStreamingInputs().get(1).getStreamSchema();

			//the first attribute must be of type rstring
			Attribute jsonAttr = schema.getAttribute(0);

			//check if the output attribute is present where the result will be stored
			if(jsonAttr != null && jsonAttr.getType().getMetaType() != MetaType.RSTRING) {
				LOGGER.log(LogLevel.ERROR, Messages.getString("JDBC_WRONG_CONTROLPORT_TYPE"), jsonAttr.getType()); 
				checker.setInvalidContext();
			}
		}
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

		loadAppConfig(context);

		if (isSslConnection()) {
			if (context.getParameterNames().contains("keyStore")) {
				System.setProperty("javax.net.ssl.keyStore", getAbsolutePath(getKeyStore()));
				if (null != getKeyStoreType()) {
					System.setProperty("javax.net.ssl.keyStoreType", getKeyStoreType());
				}
			}
			if (null != getKeyStorePassword())
				System.setProperty("javax.net.ssl.keyStorePassword", getKeyStorePassword());
			if (context.getParameterNames().contains("trustStore")) {
				System.setProperty("javax.net.ssl.trustStore", getAbsolutePath(getTrustStore()));
				if (null != getTrustStoreType()) {
					System.setProperty("javax.net.ssl.trustStoreType", getTrustStoreType());
				}
			}
			if (null != getTrustStorePassword())
				System.setProperty("javax.net.ssl.trustStorePassword", getTrustStorePassword());
		}
		TRACE.log(TraceLevel.DEBUG," propperties: " + System.getProperties().toString());
		TRACE.log(TraceLevel.DEBUG, "Operator " + context.getName() + " initializing in PE: " + context.getPE().getPEId() + " in Job: " + context.getPE().getJobId());   //$NON-NLS-3$

		// set up JDBC driver class path
		TRACE.log(TraceLevel.DEBUG, "Operator " + context.getName() + " setting up class path...");
		if(!setupClassPath(context)){
			TRACE.log(TraceLevel.ERROR, "Operator " + context.getName() + " setting up class path failed.");
//			throw new FileNotFoundException();
			throw new IOException();
		}
		TRACE.log(TraceLevel.DEBUG, "Operator " + context.getName() + " setting up class path - Completed.");

		consistentRegionContext = context.getOptionalContext(ConsistentRegionContext.class);

		// Create the JDBC connection
		TRACE.log(TraceLevel.DEBUG, "Operator " + context.getName() + " setting up JDBC connection...");
		setupJDBCConnection();
		TRACE.log(TraceLevel.DEBUG, "Operator " + context.getName() + " Setting up JDBC connection - Completed");
	}

	/**
	 * read the application config into a map
	 * @param context the operator context 
	 */
	protected void loadAppConfig(OperatorContext context) {
		
		// if no appconfig name is specified, create empty map
		if (appConfigName == null) {
			appConfig = new HashMap<String,String>();
			return;
		}
		
		appConfig = context.getPE().getApplicationConfiguration(appConfigName);
		if (appConfig.isEmpty()) {
			LOGGER.log(LogLevel.WARN, "Application config not found or empty: " + appConfigName);
		}
		
		for (Map.Entry<String, String> kv : appConfig.entrySet()) {
		   	TRACE.log(TraceLevel.DEBUG, "Found application config entry: " + kv.getKey() + "=" + kv.getValue());
		}
		
		if (null != appConfig.get("credentials")){
			credentials = appConfig.get("credentials");
		}
		if (null != appConfig.get("keyStorePassword")){
			keyStorePassword = appConfig.get("keyStorePassword");
		}
		if (null != appConfig.get("trustStorePassword")){
			trustStorePassword = appConfig.get("trustStorePassword");
		}
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
			// Acquire write lock to reset the JDBC Connection
    		lock.writeLock().lock();
    		try{
    			processControlPort(inputStream, tuple);
    		}finally{
    			lock.writeLock().unlock();
    		}
			TRACE.log(TraceLevel.DEBUG, "Process control port - Completed");
		}else{
			TRACE.log(TraceLevel.DEBUG, "Process input tuple...");

			// Reset JDBC connection if JDBC connection is not valid
			if (!jdbcClientHelper.isConnected()){
	    		TRACE.log(TraceLevel.DEBUG, "JDBC Connection is not valid");
				try {
					// Acquire write lock to reset the JDBC Connection
					lock.writeLock().lock();
					// Reset JDBC connection
					resetJDBCConnection();
				}finally {
					lock.writeLock().unlock();
				}
				TRACE.log(TraceLevel.DEBUG, "JDBC Connection reset - Completed");
			}

			// Acquire read lock to process SQL statement
			lock.readLock().lock();
			try{
				processTuple(inputStream, tuple);
			}catch (Exception e){
				if((e.toString() != null ) && (e.toString().length() > 0)){
					LOGGER.log(LogLevel.ERROR, Messages.getString("JDBC_CONNECTION_FAILED_ERROR"), new Object[]{e.toString()}); 
				}
        		// Check if JDBC connection valid
	        	if (jdbcClientHelper.isValidConnection()){
	        		// Throw exception for operator to process if JDBC connection is valid
	        		throw e;
	        	}
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

		try{
			JSONObject jdbcConnections = JSONObject.parse(jsonString);
			String jdbcClassName = (String)jdbcConnections.get("jdbcClassName");
			String jdbcUrl = (String)jdbcConnections.get("jdbcUrl");
			String jdbcUser = (String)jdbcConnections.get("jdbcUser");
			String jdbcPassword = (String)jdbcConnections.get("jdbcPassword");
			String jdbcProperties = (String)jdbcConnections.get("jdbcProperties");
			String credentials = (String)jdbcConnections.get("credentials");

			// jdbcClassName is required
			if (jdbcClassName == null || jdbcClassName.trim().isEmpty()){
				LOGGER.log(LogLevel.ERROR, Messages.getString("JDBC_CLASS_NAME_NOT_EXIST")); 
			}
			// if jdbcProperties is relative path, convert to absolute path
			if (jdbcProperties != null && !jdbcProperties.trim().isEmpty())
			{
				getProperties(jdbcProperties);
			}

			if (credentials != null && !credentials.trim().isEmpty())
			{
				getCredentials(credentials);
			}

			// jdbcUrl is required
			if (jdbcUrl == null || jdbcUrl.trim().isEmpty()){
				LOGGER.log(LogLevel.ERROR, Messages.getString("JDBC_URL_NOT_EXIST")); 
			}
			
			// Roll back the transaction
			jdbcClientHelper.rollbackWithClearBatch();
	        // Reset JDBC connection
			jdbcClientHelper.resetConnection(jdbcClassName, jdbcUrl, jdbcUser, jdbcPassword, jdbcProperties);
		}catch (FileNotFoundException e){
			LOGGER.log(LogLevel.ERROR, Messages.getString("JDBC_PROPERTIES_NOT_EXIST"), new Object[]{jdbcProperties}); 
			throw e;
		}catch (SQLException e){
			LOGGER.log(LogLevel.ERROR, Messages.getString("JDBC_RESET_CONNECTION_FAILED"), new Object[]{e.toString()}); 
			throw e;
		}
	}

    /**
     * Process an incoming punctuation that arrived on the specified port.
     * @param stream Port the punctuation is arriving on.
     * @param mark The punctuation mark
     * @throws Exception Operator failure, will cause the enclosing PE to terminate.
     */
	public void processPunctuation(StreamingInput<Tuple> stream,
    		Punctuation mark) throws Exception {
    	// Window markers are not forwarded
    	// Window markers are generated on data port (port 0) after a statement
    	// error port (port 1) is punctuation free
		if (mark == Punctuation.FINAL_MARKER) {
			super.processPunctuation(stream, mark);
		}
    }

    /**
     * Shutdown this operator.
     * @throws Exception Operator failure, will cause the enclosing PE to terminate.
     */
    public synchronized void shutdown() throws Exception {
        OperatorContext context = getOperatorContext();

        TRACE.log(TraceLevel.DEBUG, "Operator " + context.getName() + " shutting down in PE: " + context.getPE().getPEId() + " in Job: " + context.getPE().getJobId());   //$NON-NLS-3$

        // Roll back the transaction
        if (sqlFailureAction == "rollback"){
        	jdbcClientHelper.rollback();
        }
        // close JDBC connection
        jdbcClientHelper.closeConnection();

        // Must call super.shutdown()
        super.shutdown();

    }
  
	// Set up JDBC driver class path
	private boolean setupClassPath(OperatorContext context) throws MalformedURLException{	

		String libDir = jdbcDriverLib;
				
		if (jdbcDriverLib.lastIndexOf(File.separator) > 0) {
			libDir = jdbcDriverLib.substring(0, jdbcDriverLib.lastIndexOf(File.separator));
		}
		TRACE.log(TraceLevel.INFO, "Operator " + context.getName() + "setupClassPath " + jdbcDriverLib + " " + libDir);
		 	
		String jarDir = libDir;
		File f = new File(libDir);
		if (!f.isAbsolute()) {
			File appDir = getOperatorContext().getPE().getApplicationDirectory();
			TRACE.log(TraceLevel.INFO, "Operator " + context.getName() + "extending relative path '" + libDir + "' by the '" + appDir + "' directory");
			jarDir = appDir +  File.separator + libDir;
		}

		
		File jarDirectory = new File(jarDir);
		// Check if directory exists.
		if(!jarDirectory.exists())
		{
			TRACE.log(TraceLevel.ERROR, "Operator " + context.getName() + " ERROR: jdbcDriverLib " + jarDir + " does'nt exists or it is empty.");
			return false;
		}
		
		// Check if directory contains files
		File[] files = new File(jarDir).listFiles();
		if (files.length == 0){
			TRACE.log(TraceLevel.ERROR, "Operator " + context.getName() + " ERROR: jdbcDriverLib directory " + jarDir + "is empty.");
			return false;
		}
		
		// If this pathname does not denote a directory, then listFiles() returns null. 
		// Search in the "opt" directory and add all jar files to the class path. 
		boolean jarFileFound = false;
		for (File file : files) {
			if (file.isFile()) {
					String jarFile = jarDir + File.separator + file.getName();
					// check if the file is a JAR file
					if (jarFile.endsWith(".jar")){
						jarFileFound = true;
						TRACE.log(TraceLevel.INFO, "Operator " + context.getName() + "setupClassPath " + jarFile);
						context.addClassLibraries(new String[] {jarFile});
				}
			}
		}
		if (!jarFileFound){
			TRACE.log(TraceLevel.ERROR, "Operator " + context.getName() + " ERROR: No JAR file found in jdbcDriverLib directory: " + jarDir);
			return false;
		}
		else {
			TRACE.log(TraceLevel.DEBUG, "Operator " + context.getName() + " JDBC Driver Lib: " + jdbcDriverLib);
		}
		return true;
	}

	// Set up JDBC connection
	private synchronized void setupJDBCConnection() throws Exception{

		// Initiate JDBCConnectionHelper instance
        TRACE.log(TraceLevel.DEBUG, "Create JDBC Connection, jdbcClassName: " + jdbcClassName);
        TRACE.log(TraceLevel.DEBUG, "Create JDBC Connection, jdbcUrl: " + jdbcUrl);
		try{
			// if jdbcProperties is relative path, convert to absolute path
			if (jdbcProperties != null && !jdbcProperties.trim().isEmpty())
			{
				getProperties(jdbcProperties);
			}

			if (credentials != null && !credentials.trim().isEmpty()) {
				getCredentials(credentials);
			}
			
			// jdbcUrl is required
			if (jdbcUrl == null || jdbcUrl.trim().isEmpty()){
				LOGGER.log(LogLevel.ERROR, Messages.getString("JDBC_URL_NOT_EXIST")); 
			}
			
			// check if the database is a Db2 add etrieveMessagesFromServerOnGetMessage=true to URL to get SQL erroe message
			if ( (jdbcUrl.contains("db2")) && (!jdbcUrl.contains("retrieveMessagesFromServerOnGetMessage") ))
			{
				jdbcUrl = jdbcUrl +":retrieveMessagesFromServerOnGetMessage=true;";
			}
						
			jdbcClientHelper = new JDBCClientHelper(jdbcClassName, jdbcUrl, jdbcUser, jdbcPassword, sslConnection, jdbcProperties, isAutoCommit(), isolationLevel, reconnectionPolicy, reconnectionBound, reconnectionInterval, pluginName, securityMechanism);

			jdbcClientHelper.createConnection();
        }catch (FileNotFoundException e){
        	LOGGER.log(LogLevel.ERROR, Messages.getString("JDBC_PROPERTIES_NOT_EXIST"), new Object[]{jdbcProperties}); 
    		throw e;
    	}catch (SQLException e){
    		LOGGER.log(LogLevel.ERROR, Messages.getString("JDBC_CONNECTION_FAILED_ERROR"), new Object[]{e.toString()}); 
    		throw e;
    	}
	}


	// read properties file and set user name, password and jdbcUrl.
	public void getProperties(String jdbcProperties) throws IOException {
		try {
				// if jdbcProperties is relative path, convert to absolute path
				if (!jdbcProperties.startsWith(File.separator))
				{
					jdbcProperties = getOperatorContext().getPE().getApplicationDirectory() + File.separator + jdbcProperties;
				}
				
				System.out.println("JDBC Properties file from Operator '" + getOperatorContext().getName() + "' : " + jdbcProperties);

				Properties jdbcConnectionProps = new Properties();
				FileInputStream fileInput = new FileInputStream(jdbcProperties);
				jdbcConnectionProps.load(fileInput);
				fileInput.close();

				// It supports 'user' or 'username' or 'jdbcUser' 			
				jdbcUser = jdbcConnectionProps.getProperty("user");
				if (null == jdbcUser){
						jdbcUser = jdbcConnectionProps.getProperty("username");
					}
					if (null == jdbcUser){
						jdbcUser = jdbcConnectionProps.getProperty("jdbcUser");
						if (null == jdbcUser){
							LOGGER.log(LogLevel.ERROR, "'user' or 'username' is not defined in property file: " + jdbcProperties); 
							throw new Exception(Messages.getString("'jdbcUser' is required to create JDBC connection."));
						}
				}
		        
				// It supports password or jdbcPassword 			
				jdbcPassword = jdbcConnectionProps.getProperty("password");
				if (null == jdbcPassword){
					jdbcPassword = jdbcConnectionProps.getProperty("jdbcPassword");
					if (null == jdbcPassword){
						LOGGER.log(LogLevel.ERROR, "'password' or jdbcPassword' is not defined in property file: " + jdbcProperties); 
						throw new Exception(Messages.getString("'jdbcPassword' is required to create JDBC connection."));
					}
				}
                // It supports jdbcUrl and jdbcurl 			
				jdbcUrl = jdbcConnectionProps.getProperty("jdbcUrl");
				if (null == jdbcUrl){
					jdbcUrl = jdbcConnectionProps.getProperty("jdbcurl");
					if (null == jdbcUrl){
						LOGGER.log(LogLevel.ERROR, "'jdbcUrl' or 'jdbcurl' is not defined in property file: " + jdbcProperties); 
						throw new Exception(Messages.getString("JDBC_URL_NOT_EXIST"));
					}
				}

			} catch (Exception ex) {
				     ex.printStackTrace();
		}
	} 


	// read credentials  and set user name, password and jdbcUrl.
	public void getCredentials(String credentials) throws IOException {
		String jsonString = credentials;

		try {
			JSONObject obj = JSONObject.parse(jsonString);			
			jdbcUser = (String)obj.get("username");
			if (jdbcUser == null || jdbcUser.trim().isEmpty()){
				LOGGER.log(LogLevel.ERROR, Messages.getString("'jdbcUser' is required to create JDBC connection.")); 
				throw new Exception(Messages.getString("'jdbcUser' is required to create JDBC connection."));
			}
		 
			jdbcPassword = (String)obj.get("password");
			if (jdbcPassword == null || jdbcPassword.trim().isEmpty()){
				LOGGER.log(LogLevel.ERROR, Messages.getString("'jdbcPassword' is required to create JDBC connection.")); 
				throw new Exception(Messages.getString("'jdbcPassword' is required to create JDBC connection."));
			}
		
			jdbcUrl = (String)obj.get("jdbcurl");
			if (jdbcUrl == null || jdbcUrl.trim().isEmpty()){
			    jdbcUrl = (String)obj.get("jdbcUrl");
			}			
			// jdbcUrl is required
			if (jdbcUrl == null || jdbcUrl.trim().isEmpty()){
				LOGGER.log(LogLevel.ERROR, Messages.getString("JDBC_URL_NOT_EXIST")); 
				throw new Exception(Messages.getString("JDBC_URL_NOT_EXIST"));
			}
			System.out.println("jdbcUrl from credentials in Operator '" + getOperatorContext().getName() + "' :" + jdbcUrl);
			
			} catch (Exception ex) {
			         ex.printStackTrace();
		}
	} 
	

	// Reset JDBC connection
	protected void resetJDBCConnection() throws Exception{
		// Reset JDBC connection
		jdbcClientHelper.resetConnection();

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
		LOGGER.log(LogLevel.INFO, Messages.getString("JDBC_CR_CLOSE")); 
	}

	@Override
	public void checkpoint(Checkpoint checkpoint) throws Exception {
		LOGGER.log(LogLevel.INFO, Messages.getString("JDBC_CR_CHECKPOINT"), checkpoint.getSequenceId()); 

		jdbcClientHelper.commit();
	}

	@Override
	public void drain() throws Exception {
		LOGGER.log(LogLevel.INFO, Messages.getString("JDBC_CR_DRAIN")); 
	}

	@Override
	public void reset(Checkpoint checkpoint) throws Exception {
		LOGGER.log(LogLevel.INFO, Messages.getString("JDBC_CR_RESET"), checkpoint.getSequenceId()); 

		jdbcClientHelper.rollback();
	}

	@Override
	public void resetToInitialState() throws Exception {
		LOGGER.log(LogLevel.INFO, Messages.getString("JDBC_RESET_TO_INITIAL")); 

		jdbcClientHelper.rollback();
	}

	@Override
	public void retireCheckpoint(long id) throws Exception {
		LOGGER.log(LogLevel.INFO, Messages.getString("JDBC_CR_RETIRE")); 
	}
	protected String getAbsolutePath(String filePath) {
		if (filePath == null)
			return null;

		Path p = Paths.get(filePath);
		if (p.isAbsolute()) {
			return filePath;
		} else {
			File f = new File(getOperatorContext().getPE().getApplicationDirectory(), filePath);
			return f.getAbsolutePath();
		}
	}
}
