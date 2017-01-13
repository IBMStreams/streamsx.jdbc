package com.ibm.streamsx.jdbc;

public enum CommitPolicy {

	OnCheckpoint,
	OnTransaction,
	OnTransactionAndCheckpoint;
	
}
