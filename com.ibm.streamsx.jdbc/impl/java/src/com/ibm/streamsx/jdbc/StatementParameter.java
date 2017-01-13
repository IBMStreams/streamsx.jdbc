/*******************************************************************************
 * Copyright (C) 2015 International Business Machines Corporation
 * All Rights Reserved
 *******************************************************************************/
package com.ibm.streamsx.jdbc;

import com.ibm.streams.operator.Attribute;

/* The class for statement parameter values 
 */
public class StatementParameter {

	// SPL attribute name
	private String splAttributeName;
	// SPL Attribute
	private Attribute splAttribute;

	// SPL attribute value
	private Object splValue;


	public String getSplAttributeName() {
		return splAttributeName;
	}

	public void setSplAttributeName(String splAttributeName) {
		this.splAttributeName = splAttributeName;
	}

	public Object getSplValue() {
		return splValue;
	}

	public void setSplValue(Object splValue) {
		this.splValue = splValue;
	}

	public Attribute getSplAttribute() {
		return splAttribute;
	}

	public void setSplAttribute(Attribute splAttribute) {
		this.splAttribute = splAttribute;
	}

}
