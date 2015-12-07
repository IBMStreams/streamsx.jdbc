/*******************************************************************************
 * Copyright (C) 2015 International Business Machines Corporation
 * All Rights Reserved
 *******************************************************************************/
package com.ibm.streamsx.jdbc;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
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
import com.ibm.streams.operator.types.XML;

/**
 * The JDBCRun operator runs a user-defined SQL statement that is based on an input tuple. 
 * The statement is run once for each input tuple received. 
 * Result sets that are produced by the statement are emitted as output stream tuples. 
 */
@PrimitiveOperator(description="The `JDBCRun` operator runs a user-defined SQL statement that is based on an input tuple." + 
		" The statement is run once for each input tuple received." +  
		" Result sets that are produced by the statement are emitted as output stream tuples." +
		" The `JDBCRun` operator is commonly used to update, merge, and delete database management system (DBMS) records." + 
		" This operator is also used to retrieve records, create and drop tables, and to call stored procedures." +
		" # Behavior in a consistent region" +
		" The `JDBCRun` operator can be used in a consistent region. It cannot be the start operator of a consistent region." +
		" In a consistent region, the configured value of the transactionSize is ignored. Instead, database commits are performed (when supported by the DBMS) on consistent region checkpoints, and database rollbacks are performed on consistent region resets." +
		" On drain: If there are any pending statements, they are run. If the statement generates a result set and the operator has an output port, tuples are generated from the results and submitted to the output port. If the operator has an error output port and the statement generates any errors, tuples are generated from the errors and submitted to the error output port." +
		" On checkpoint: A database commit is performed." +
		" On reset: Any pending statements are discarded. A rollback is performed.")
@InputPorts({@InputPortSet(cardinality=1, description="The `JDBCRun` operator has one required input port. When a tuple is received on the required input port, the operator runs an SQL statement."), 
			@InputPortSet(cardinality=1, optional=true, controlPort = true, description="The `JDBCRun` operator has one optional input port. This port allows operator to change jdbc connection information at run time.")})
@OutputPorts({@OutputPortSet(cardinality=1, description="The `JDBCRun` operator has one required output port. The output port submits a tuple for each row in the result set of the SQL statement if the statement produces a result set. The output tuple values are assigned in the following order: 1. Columns that are returned in the result set that have same name from the output tuple 2. Auto-assigned attributes of the same name from the input tuple"), 
			@OutputPortSet(cardinality=1, optional=true, description="The `JDBCRun` operator has one optional output port. This port submits tuples when an error occurs while the operator is running the SQL statement.")})
public class JDBCRun extends AbstractJDBCOperator{
	
	private static final String CLASS_NAME = "com.ibm.streamsx.jdbc.jdbcrun.JDBCRun";

	/**
	 * Create a logger specific to this class
	 */
	private static Logger LOGGER = Logger.getLogger(LoggerNames.LOG_FACILITY
			+ "." + CLASS_NAME, "com.ibm.streamsx.jdbc.JDBCMessages");

	/**
	 *  variable to hold the output port
	 */
	private StreamingOutput<OutputTuple> dataOutputPort;
	
	/** 
	 * hasErrorPort signifies if the operator has error port defined or not
	 * assuming in the beginning that the operator does not have an error output
	 * port by setting hasErrorPort to false.
	 * further down in the code, if the number of output ports is 2, we set to true
	 * We send data to error output port only in case where hasErrorPort is set
	 * to true which implies that the operator instance has a error output port
	 * defined.
	 */
	private boolean hasErrorPort = false;

	/** 
	 * The SQL statement from statement parameter is static, and JDBC PreparedStatement
	 * interface will be used for execution.
	 * The SQL statement from statementAttr parameter is dynamic, and JDBC
	 * Statement interface will be used for execution.
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
	// This parameter specifies the value of SQL statement that is from stream attribute (no parameter markers).
	private TupleAttribute<Tuple, String> statementAttr;

	// This parameter specifies the number of statements to execute as a batch.
	// The default transaction size is 1.
	private int batchSize = 1;
	// This parameter specifies the number of executions to commit per transaction.
	// The default transaction size is 1 and transactions are automatically committed.
	private int transactionSize = 1;
	// Transaction counter
	private int transactionCount = 0;
	// Execution Batch counter
	private int batchCount = 0;

	// This parameter points to an output attribute and returns true if the statement produces result sets,
	// otherwise, returns false.
	private String hasResultSetAttr = null;
	private boolean hasResultSetValue = false;
	
	// This parameter points to an output attribute and returns the SQL status information.
	private String sqlStatusAttr = null;
	// sqlStatus attribute for data output port
	private String sqlStatusDataOutput = null;
	// sqlStatus attribute for error output port
	private String sqlStatusErrorOutput = null;
	
	// SQL ErrorCode & SQLState
	private int errorCode = IJDBCConstants.SQL_ERRORCODE_SUCCESS;
	private String sqlState = IJDBCConstants.SQL_STATE_SUCCESS;

	//Parameter statement
	@Parameter(optional = true, description="This parameter specifies the value of any valid SQL or stored procedure statement. The statement can contain parameter markers")
    public void setStatement(String statement){
    	this.statement = statement;
    }

	//Parameter statementParameters
	@Parameter(optional = true, description="This optional parameter specifies the value of statement parameters. The statementParameter value and SQL statement parameter markers are associated in lexicographic order. For example, the first parameter marker in the SQL statement is associated with the first statementParameter value.")
    public void setStatementParamAttrs(String statementParamAttrs){
    	this.statementParamAttrs = statementParamAttrs;

    	String statementParamNames[] = statementParamAttrs.split(",");
		statementParamArrays = new StatementParameter[statementParamNames.length];
		for (int i = 0; i< statementParamNames.length; i++){
			statementParamArrays[i] = new StatementParameter();
			statementParamArrays[i].setSplAttributeName(statementParamNames[i]);
		}
    }
	
	//Parameter statementAttr
	@Parameter(optional = true, description="This parameter specifies the value of complete SQL or stored procedure statement that is from stream attribute (no parameter markers).")
    public void setStatementAttr(TupleAttribute<Tuple, String> statementAttr){
    	this.statementAttr = statementAttr;
    }

	//Parameter transactionSize
	@Parameter(optional = true, description="This optional parameter specifies the number of executions to commit per transaction. The default transaction size is 1 and transactions are automatically committed.")
    public void setTransactionSize(int transactionSize){
    	this.transactionSize = transactionSize;
    }

	//Parameter batchSize
	@Parameter(optional = true, description="This optional parameter specifies the number of statement to execute as a batch. The default batch size is 1.")
    public void setBatchSize(int batchSize){
    	this.batchSize = batchSize;
    }
	
	//Parameter hasResultSetAttr
	@Parameter(optional = true, description="This parameter points to an output attribute and returns true if the statement produces result sets, otherwise, returns false")
    public void setHasResultSetAttr(String hasResultSetAttr){
    	this.hasResultSetAttr = hasResultSetAttr;
    }

	//Parameter sqlStatusAttr
	@Parameter(optional = true, description="This parameter points to an output attribute and returns the SQL status information, including SQL error code (the error number associated with the SQLException) and SQL state (the five-digit XOPEN SQLState code for a database error)")
    public void setSqlStatusAttr(String sqlStatusAttr){
    	this.sqlStatusAttr = sqlStatusAttr;
        if (sqlStatusAttr != null){
    		String sqlStatus[] = sqlStatusAttr.split(",");
    		if (sqlStatus.length > 0 && !sqlStatus[0].trim().isEmpty()){
    			sqlStatusDataOutput = sqlStatus[0].trim();
    		}
    		if (sqlStatus.length > 1 && !sqlStatus[1].trim().isEmpty()){
    			sqlStatusErrorOutput = sqlStatus[1].trim();
    		}
        }

    }

	/*
	 * The method checkErrorOutputPort validates that the stream on error output
	 * port contains the optional attribute of type which is the incoming tuple,
	 * and a sqlStatusType which will contain the error message in order.
	 */
	@ContextCheck
	public static void checkErrorOutputPort(OperatorContextChecker checker) {
		OperatorContext context = checker.getOperatorContext();
		// Check if the operator has an error port defined
		if (context.getNumberOfStreamingOutputs() == 2) {
			StreamingOutput<OutputTuple> errorOutputPort = context.getStreamingOutputs().get(1);
			// The optional error output port can have no more than two attributes.
			if (errorOutputPort.getStreamSchema().getAttributeCount() > 2) {
				LOGGER.log(TraceLevel.ERROR, "ATMOST_TWO_ATTR");

			}
			// The optional error output port must have at least one attribute.
			if (errorOutputPort.getStreamSchema().getAttributeCount() < 1) {
				LOGGER.log(TraceLevel.ERROR, "ATLEAST_ONE_ATTR");

			}
			// If two attributes are specified, the first attribute in the
			// optional error output port must be a tuple.
			if (errorOutputPort.getStreamSchema().getAttributeCount() == 2) {
				if (errorOutputPort.getStreamSchema().getAttribute(0).getType().getMetaType() != Type.MetaType.TUPLE) {
					LOGGER.log(TraceLevel.ERROR, "ERROR_PORT_FIRST_ATTR_TUPLE");

				}
			}
		}
	
	} 	

	@ContextCheck(compile = true, runtime = false)
	public static void checkCompileTimeConsistentRegion(OperatorContextChecker checker) {
		ConsistentRegionContext consistentRegionContext = checker.getOperatorContext().getOptionalContext(ConsistentRegionContext.class);
		
		if(consistentRegionContext != null && consistentRegionContext.isStartOfRegion()) {
			checker.setInvalidContext("The following operator cannot be the start of a consistent region: JDBCRun", new String[] {});
		}
	}

	@ContextCheck(compile = false, runtime = true)
	public static void checkParameterAttributes(OperatorContextChecker checker) {
		
		OperatorContext context = checker.getOperatorContext();	
		
		if (checker.getOperatorContext().getNumberOfStreamingOutputs() > 0){
			StreamingOutput<OutputTuple> dataPort = context.getStreamingOutputs().get(0);
			StreamSchema schema = dataPort.getStreamSchema();
			// Check hasResultSetAttr parameters at runtime
			if ((context.getParameterNames().contains("hasResultSetAttr"))) {
				if (schema.getAttribute(context.getParameterValues("hasResultSetAttr").get(0)) == null){
	                LOGGER.log(TraceLevel.ERROR, "HASRSATTR_NOT_EXIST", context.getParameterValues("hasResultSetAttr").get(0));
					checker.setInvalidContext("The attribute specified in hasResultSetAttr parameter does not exist: " + context.getParameterValues("hasResultSetAttr").get(0), null);
				}
			}
			// Check sqlStatusAttr parameters at runtime
			if ((context.getParameterNames().contains("sqlStatusAttr"))) {
				String sqlStatusAttr = context.getParameterValues("sqlStatusAttr").get(0);
	    		String sqlStatus[] = sqlStatusAttr.split(",");
	    		if (sqlStatus.length > 0 && !sqlStatus[0].trim().isEmpty()){
					if (schema.getAttribute(sqlStatus[0].trim()) == null){
		                LOGGER.log(TraceLevel.ERROR, "SQLSTATUSATTR_NOT_EXIST", sqlStatus[0]);
						checker.setInvalidContext("The attribute specified in sqlStatusAttr parameter does not exist: " + sqlStatus[0], null);
					}
	    		}
			}
		}
		if (checker.getOperatorContext().getNumberOfStreamingOutputs() > 1){
			StreamingOutput<OutputTuple> errorPort = context.getStreamingOutputs().get(1);
			StreamSchema schema = errorPort.getStreamSchema();
			// Check sqlStatusAttr parameters at runtime
			if ((context.getParameterNames().contains("sqlStatusAttr"))) {
				String sqlStatusAttr = context.getParameterValues("sqlStatusAttr").get(0);
	    		String sqlStatus[] = sqlStatusAttr.split(",");
	    		if (sqlStatus.length > 1 && !sqlStatus[1].trim().isEmpty()){
					if (schema.getAttribute(sqlStatus[1].trim()) == null){
		                LOGGER.log(TraceLevel.ERROR, "SQLSTATUSATTR_NOT_EXIST", sqlStatus[1]);
						checker.setInvalidContext("The attribute specified in sqlStatusAttr parameter does not exist: " + sqlStatus[1], null);
					}
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
    	// Must call super.initialize(context) to correctly setup an operator.
		super.initialize(context);
		
		/*
		 * Set appropriate variables if the optional error output port is
		 * specified. Also set errorOutputPort to the output port at index 0
		 */
		if (context.getNumberOfStreamingOutputs() == 2) {
			hasErrorPort = true;
			errorOutputPort = getOutput(1);
		}

		// set the data output port
		dataOutputPort = getOutput(0);
		
		// Initiate PreparedStatement
		if (statement != null) {
			isStaticStatement = true;
			TRACE.log(TraceLevel.DEBUG, "Initializing PreparedStatement: " + statement);
			jdbcConnectionHelper.initPreparedStatement(statement);
			TRACE.log(TraceLevel.DEBUG, "Initializing PreparedStatement - Completed");
		}
		
	}

	// JDBC connection need to be auto-committed or not
	@Override
	protected boolean isAutoCommit(){
        if ((consistentRegionContext != null) || (transactionSize > 1)){
        	// Set automatic commit to false when transaction size is more than 1 or it is a consistent region.
        	return false;
        }
		return true;
	}

	// Process input tuple
    protected void processTuple(StreamingInput<Tuple> stream, Tuple tuple) throws Exception{
    	// Set errorCode and sqlState to default value.
    	errorCode = IJDBCConstants.SQL_ERRORCODE_SUCCESS;
    	sqlState = IJDBCConstants.SQL_STATE_SUCCESS;

        // Execute the statement
        ResultSet rs = null;
        try{
            if (isStaticStatement){
            	if (batchSize > 1){
            		batchCount ++;
            		jdbcConnectionHelper.addPreparedStatementBatch(getStatementParameterArrays(statementParamArrays, tuple));
            		if (batchCount >= batchSize){
            			batchCount = 0;
            			transactionCount ++;
            			jdbcConnectionHelper.executePreparedStatementBatch();
            		}
            	}else{
            		transactionCount ++;
            		rs = jdbcConnectionHelper.executePreparedStatement(getStatementParameterArrays(statementParamArrays, tuple));
            	}
	        }else{
	        	String statementFromAttribute = statementAttr.getValue(tuple);
	        	if (statementFromAttribute != null && ! statementFromAttribute.isEmpty()){
        			TRACE.log(TraceLevel.DEBUG, "Statement: " + statementFromAttribute);
	        		if (batchSize > 1){
	            		batchCount ++;
	            		jdbcConnectionHelper.addStatementBatch(statementFromAttribute);
	            		if (batchCount >= batchSize){
	            			batchCount = 0;
	            			transactionCount ++;
	            			jdbcConnectionHelper.executeStatementBatch();
	            		}
	        		}else{
	        			transactionCount ++;
	        			rs = jdbcConnectionHelper.executeStatement(statementFromAttribute);
	        			TRACE.log(TraceLevel.DEBUG, "Transaction Count: " + transactionCount);
	        		}
	        	}else{
	                LOGGER.log(TraceLevel.ERROR, "SQL_STATEMENT_NULL");
	        	}
	        }
        }catch (SQLException e){
			errorCode = e.getErrorCode();
        	sqlState = e.getSQLState();
        	TRACE.log(TraceLevel.DEBUG, "SQL Exception Error Code: " + errorCode);
        	TRACE.log(TraceLevel.DEBUG, "SQL EXCEPTION SQL State: " + sqlState);
        	if (hasErrorPort){
        		// submit error message
        		submitErrorTuple(errorOutputPort, tuple);
        	}
        	
        	if (sqlFailureAction.equalsIgnoreCase(IJDBCConstants.SQLFAILURE_ACTION_LOG)){
    			TRACE.log(TraceLevel.DEBUG, "SQL Failure - Log...");
        		// The error is logged, and the error condition is cleared
            	LOGGER.log(TraceLevel.WARNING, "SQL_EXCEPTION_WARNING", new Object[] { e.toString() });
        		
        	}else if (sqlFailureAction.equalsIgnoreCase(IJDBCConstants.SQLFAILURE_ACTION_ROLLBACK)){
        		
    			TRACE.log(TraceLevel.DEBUG, "SQL Failure - Roll back...");
            	LOGGER.log(TraceLevel.ERROR, "SQL_EXCEPTION_ERROR", new Object[] { e.toString() });

            	if (consistentRegionContext != null){
    				// The error is logged, and request a reset of the consistent region.
    				consistentRegionContext.reset();
    			}else{
    				if (batchSize > 1){
    					// Clear statement batch & roll back the transaction
    					jdbcConnectionHelper.rollbackWithClearBatch();
    					// Reset the batch counter
    					batchCount = 0;
    				}else{
    					// Roll back the transaction
    					jdbcConnectionHelper.rollback();
    				}
    				// Reset the transaction counter
    				transactionCount = 0;
    			}
            	return;
        	}else if (sqlFailureAction.equalsIgnoreCase(IJDBCConstants.SQLFAILURE_ACTION_TERMINATE)){
    			TRACE.log(TraceLevel.DEBUG, "SQL Failure - Shut down...");
        		// The error is logged and the operator terminates.
            	LOGGER.log(TraceLevel.ERROR, "SQL_EXCEPTION_ERROR", new Object[] { e.toString() });
            	if (batchSize > 1){
					// Clear statement batch & Roll back the transaction
            		jdbcConnectionHelper.rollbackWithClearBatch();
            		// Reset the batch counter
            		batchCount = 0;
            	}else{
					// Roll back the transaction
            		jdbcConnectionHelper.rollback();
            	}
        		// Reset transaction counter
        		transactionCount = 0;
        		shutdown();
        		return;
        	}
        }
        // Commit the transactions according to transactionSize
		if ((consistentRegionContext == null) && (transactionSize > 1) && (transactionCount >= transactionSize)){
			TRACE.log(TraceLevel.DEBUG, "Transaction Commit...");
			transactionCount = 0;
			jdbcConnectionHelper.commit();
		}
        
		if (rs != null){
        	// Set hasReultSetValue
			if (rs.next()){
				hasResultSetValue = true;
				TRACE.log(TraceLevel.DEBUG, "Has Result Set: " + hasResultSetValue);
        		// Submit result set as output tuple
        		submitOutputTuple(dataOutputPort, tuple, rs);
			}
			
        	while (rs.next()){
        		// Submit result set as output tuple
        		submitOutputTuple(dataOutputPort, tuple, rs);
        	}
        	// Generate a window punctuation after all of the tuples are submitted 
        	dataOutputPort.punctuate(Punctuation.WINDOW_MARKER);
        }else{
        	// Set reultSetCountAttr to 0 if the statement does not produce result sets
        	hasResultSetValue = false;
            // Submit output tuple without result set
            submitOutputTuple(dataOutputPort, tuple, null);
            
        }
	}
    
	// Return SPL value according to SPL type
	protected Object getSplValue(Attribute attribute, Tuple tuple){
		
		Type splType = attribute.getType();
		int index = attribute.getIndex();
		
		if (splType.getMetaType() == MetaType.INT8)			return tuple.getByte(index);
		if (splType.getMetaType() == MetaType.INT16)		return tuple.getShort(index);
		if (splType.getMetaType() == MetaType.INT32)		return tuple.getInt(index);
		if (splType.getMetaType() == MetaType.INT64)		return tuple.getLong(index);

		if (splType.getMetaType() == MetaType.UINT8)		return tuple.getByte(index);
		if (splType.getMetaType() == MetaType.UINT16)		return tuple.getShort(index);
		if (splType.getMetaType() == MetaType.UINT32)		return tuple.getInt(index);
		if (splType.getMetaType() == MetaType.UINT64)		return tuple.getLong(index);

		if (splType.getMetaType() == MetaType.BLOB)			return tuple.getBlob(index);

		if (splType.getMetaType() == MetaType.BOOLEAN)		return tuple.getBoolean(index);
		
		if (splType.getMetaType() == MetaType.DECIMAL32)	return tuple.getBigDecimal(index);
		if (splType.getMetaType() == MetaType.DECIMAL64)	return tuple.getBigDecimal(index);
		if (splType.getMetaType() == MetaType.DECIMAL128)	return tuple.getBigDecimal(index);
		
		if (splType.getMetaType() == MetaType.FLOAT32)		return tuple.getFloat(index);
		if (splType.getMetaType() == MetaType.FLOAT64)		return tuple.getDouble(index);
		
		if (splType.getMetaType() == MetaType.RSTRING)		return tuple.getString(index);
		if (splType.getMetaType() == MetaType.USTRING)		return tuple.getString(index);

		if (splType.getMetaType() == MetaType.TIMESTAMP)	return tuple.getTimestamp(index).getSQLTimestamp();

		if (splType.getMetaType() == MetaType.XML)			return tuple.getXML(index);
		
		LOGGER.log(TraceLevel.ERROR, "SPLTYPE_NOT_SUPPORT", splType.getMetaType());
		return null;
		
	}
	
	// Create StatementParameter object arrays
	protected StatementParameter[] getStatementParameterArrays(StatementParameter[] stmtParameterArrays, Tuple tuple){
    	if (stmtParameterArrays != null){
    		for (int i = 0; i< stmtParameterArrays.length; i++){
    			TRACE.log(TraceLevel.DEBUG, "Parameter statementParameter Name: " + stmtParameterArrays[i].getSplAttributeName());
    			Attribute attribute = tuple.getStreamSchema().getAttribute(stmtParameterArrays[i].getSplAttributeName().trim());
    			if (attribute == null){
        			LOGGER.log(TraceLevel.ERROR, "STATEMENT_PARAMETER_NOT_EXIST", stmtParameterArrays[i].getSplAttributeName());
    			}else{
    				stmtParameterArrays[i].setSplAttribute(attribute);
    				stmtParameterArrays[i].setSplValue(getSplValue(stmtParameterArrays[i].getSplAttribute(), tuple));
    				TRACE.log(TraceLevel.DEBUG, "Parameter statementParameters Value: " + stmtParameterArrays[i].getSplValue());
    			}
    		}
    	}
    	return stmtParameterArrays;
	}

	// Submit output tuple according to result set
	protected void submitOutputTuple(StreamingOutput<OutputTuple> outputPort, Tuple inputTuple, ResultSet rs) throws Exception {

		OutputTuple outputTuple = outputPort.newTuple();
		
		//Pass all incoming attributes as is to the output tuple
		outputTuple.assign(inputTuple);

		// Get the schema for the output tuple type
		StreamSchema schema = outputTuple.getStreamSchema();
		
		// Assign hasResultSet value according to hasResultSetAttr parameter
		if (hasResultSetAttr != null){
			TRACE.log(TraceLevel.DEBUG, "hasResultSet: " + hasResultSetValue);
			outputTuple.setBoolean(hasResultSetAttr, hasResultSetValue);
		}

		// Assign sqlStatus value according to sqlStatusAtr parameter
        // Check if sqlStatus has been specified
        if (sqlStatusDataOutput != null){
			// Assign SQL status according to sqlStatusAttr parameter
			TRACE.log(TraceLevel.DEBUG, "sqlStatusDataOutput: " + sqlStatusDataOutput);
			Attribute attr = schema.getAttribute(sqlStatusDataOutput);
			TupleType dTupleType = (TupleType) attr.getType();
			StreamSchema dSchema = dTupleType.getTupleSchema();
			// Create a tuple with desired value
			Map<String, Object> attrmap = new HashMap<String, Object>();
			attrmap.put("errorCode", errorCode);
			attrmap.put("sqlState", new RString(sqlState));
			Tuple sqlStatusT = dSchema.getTuple(attrmap);
			// Assign the values to the output tuple
			outputTuple.setObject(sqlStatusDataOutput, sqlStatusT);
        }
		
        // Assign values from result set
        if (rs != null){
			ResultSetMetaData rsmd = rs.getMetaData();
			for (int i=1; i<=rsmd.getColumnCount(); i++){
				String columnName = rsmd.getColumnName(i);
				Attribute attr = schema.getAttribute(columnName);
				if (attr != null){
					String splAttrName = attr.getName();
					MetaType splType = attr.getType().getMetaType();
	
					// Assign value from result set
					if (splType == MetaType.RSTRING) outputTuple.setString(splAttrName, rs.getString(i));
					else if (splType == MetaType.USTRING) outputTuple.setString(splAttrName, rs.getString(i));
					else if (splType == MetaType.INT8) outputTuple.setByte(splAttrName, rs.getByte(i));
					else if (splType == MetaType.INT16) outputTuple.setShort(splAttrName, rs.getShort(i));
					else if (splType == MetaType.INT32) outputTuple.setInt(splAttrName, rs.getInt(i));
					else if (splType == MetaType.INT64) outputTuple.setLong(splAttrName, rs.getLong(i));
					else if (splType == MetaType.UINT8) outputTuple.setByte(splAttrName, rs.getByte(i));
					else if (splType == MetaType.UINT16) outputTuple.setShort(splAttrName, rs.getShort(i));
					else if (splType == MetaType.UINT32) outputTuple.setInt(splAttrName, rs.getInt(i));
					else if (splType == MetaType.UINT64) outputTuple.setLong(splAttrName, rs.getLong(i));
					else if (splType == MetaType.FLOAT32) outputTuple.setFloat(splAttrName, rs.getFloat(i));
					else if (splType == MetaType.FLOAT64) outputTuple.setDouble(splAttrName, rs.getDouble(i));
					else if (splType == MetaType.DECIMAL32) outputTuple.setBigDecimal(splAttrName, rs.getBigDecimal(i));
					else if (splType == MetaType.DECIMAL64) outputTuple.setBigDecimal(splAttrName, rs.getBigDecimal(i));
					else if (splType == MetaType.DECIMAL128) outputTuple.setBigDecimal(splAttrName, rs.getBigDecimal(i));
					else if (splType == MetaType.BLOB) outputTuple.setBlob(splAttrName, (Blob)rs.getBlob(i));
					else if (splType == MetaType.TIMESTAMP) outputTuple.setTimestamp(splAttrName, Timestamp.getTimestamp(rs.getTimestamp(i)));
					else if (splType == MetaType.XML) outputTuple.setXML(splAttrName, (XML)rs.getSQLXML(i));
					else if (splType == MetaType.BOOLEAN) outputTuple.setBoolean(splAttrName, rs.getBoolean(i));
					else LOGGER.log(TraceLevel.ERROR, "SPLTYPE_NOT_SUPPORT", splType);
					
				}
			}
        }
		// Submit result set as output tuple
		outputPort.submit(outputTuple);
	}

    // Submit error tuple
	protected void submitErrorTuple(StreamingOutput<OutputTuple> errorOutputPort, Tuple inputTuple) throws Exception{
		OutputTuple errorTuple = errorOutputPort.newTuple();
    	
		// Assign SQL status according to sqlStatusAttr parameter
        if (sqlStatusErrorOutput != null){
        	TRACE.log(TraceLevel.DEBUG, "sqlStatusErrorOutput: " + sqlStatusErrorOutput);
        	StreamSchema schema = errorTuple.getStreamSchema();
			Attribute attr = schema.getAttribute(sqlStatusErrorOutput);
			TupleType dTupleType = (TupleType) attr.getType();
			StreamSchema dSchema = dTupleType.getTupleSchema();
			// Create a tuple with desired value
			Map<String, Object> attrmap = new HashMap<String, Object>();
			attrmap.put("errorCode", errorCode);
			attrmap.put("sqlState", new RString(sqlState));
			Tuple sqlStatusT = dSchema.getTuple(attrmap);
			// Assign the values to the output tuple
			errorTuple.setObject(sqlStatusErrorOutput, sqlStatusT);
        }

        // Copy across all matching attributes.
        Tuple embeddedInputTuple = errorTuple.getTuple(0);
        if (embeddedInputTuple != null){
        	StreamSchema embeddedSchema = embeddedInputTuple.getStreamSchema();
        	Tuple embeddedTuple = embeddedSchema.getTuple(inputTuple);
        	errorTuple.setTuple(0, embeddedTuple);
        }
        
        // Submit error tuple to error output port
        errorOutputPort.submit(errorTuple);
	}

    
    @Override
    public synchronized void shutdown() throws Exception {
    	
    	if (batchSize > 1){
    		if (isStaticStatement){
    			jdbcConnectionHelper.clearPreparedStatementBatch();;
    		}else{
    			jdbcConnectionHelper.clearStatementBatch();
    		}
    	}
    	
    	// Roll back transaction & close connection
        super.shutdown();
        
    }

    
	@Override
	public void checkpoint(Checkpoint checkpoint) throws Exception {
		LOGGER.log(TraceLevel.INFO, "CR_CHECKPOINT", checkpoint.getSequenceId());
		
		// Commit the transaction
		jdbcConnectionHelper.commit();

		// Save current batch information
		if (batchSize > 1){
			TRACE.log(TraceLevel.DEBUG, "Checkpoint batchCount: " + batchCount);
			checkpoint.getOutputStream().writeInt(batchCount);
			if (isStaticStatement){
				TRACE.log(TraceLevel.DEBUG, "Checkpoint preparedStatement");
				checkpoint.getOutputStream().writeObject(jdbcConnectionHelper.getPreparedStatement());
			}else{
				TRACE.log(TraceLevel.DEBUG, "Checkpoint statement");
				checkpoint.getOutputStream().writeObject(jdbcConnectionHelper.getStatement());
			}
		}
	}
	
	@Override
	public void reset(Checkpoint checkpoint) throws Exception {
		LOGGER.log(TraceLevel.INFO, "CR_RESET", checkpoint.getSequenceId());
		
		// Roll back the transaction
		jdbcConnectionHelper.rollback();
		
		// Reset the batch information
		if (batchSize > 1){
			batchCount = checkpoint.getInputStream().readInt();
			TRACE.log(TraceLevel.DEBUG, "Reset batchCount: " + batchCount);
			if (isStaticStatement){
				jdbcConnectionHelper.setPreparedStatement((PreparedStatement)checkpoint.getInputStream().readObject());
				TRACE.log(TraceLevel.DEBUG, "Reset preparedStatement");
			}else{
				jdbcConnectionHelper.setStatement((Statement)checkpoint.getInputStream().readObject());
				TRACE.log(TraceLevel.DEBUG, "Reset statement");
			}
		}
	}
	
	@Override
	public void resetToInitialState() throws Exception {
		LOGGER.log(TraceLevel.INFO, "RESET_TO_INITIAL");
		if (batchSize > 1){
			jdbcConnectionHelper.rollbackWithClearBatch();
			batchCount = 0;
		}else{
			jdbcConnectionHelper.rollback();
		}
	}

}