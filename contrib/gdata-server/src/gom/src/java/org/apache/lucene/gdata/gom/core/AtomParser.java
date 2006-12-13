/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.lucene.gdata.gom.core;

import javax.xml.namespace.QName;

/**
 * @author Simon Willnauer
 * 
 */
public interface AtomParser {

	/**
	 * Error message for an unexpected element
	 * @see String#format(java.lang.String, java.lang.Object[])
	 */
	public static final String UNEXPECTED_ELEMENT = "Expected Element '%s' but was '%s' ";


	/**
	 * Error message for an unexpected element child
	 * @see String#format(java.lang.String, java.lang.Object[])
	 */
	public static final String UNEXPECTED_ELEMENT_CHILD = "Element '%s' can not contain child elements ";

	/**
	 * Error message for an urecognized element child
	 * @see String#format(java.lang.String, java.lang.Object[])
	 */
	public static final String URECOGNIZED_ELEMENT_CHILD = "Element '%s' can not contain child elements of the type %s";

	/**
	 * Error message for an unexpected attribute
	 * @see String#format(java.lang.String, java.lang.Object[])
	 */
	public static final String UNEXPECTED_ATTRIBUTE = "Element '%s' can not contain attributes ";

	/**
	 * Error message for an unexpected element value
	 * @see String#format(java.lang.String, java.lang.Object[])
	 */
	public static final String UNEXPECTED_ELEMENT_VALUE = "Element '%s' can not contain any element value";

	/**
	 * Error message for a missing element attribute
	 * @see String#format(java.lang.String, java.lang.Object[])
	 */
	public static final String MISSING_ELEMENT_ATTRIBUTE = "Element '%s' requires an '%s' attribute";

	/**
	 * Error message for a missing element child
	 * @see String#format(java.lang.String, java.lang.Object[])
	 */
	public static final String MISSING_ELEMENT_CHILD = "Element '%s' requires a child of the type '%s'";

	/**
	 * Error message for a missing element value
	 * @see String#format(java.lang.String, java.lang.Object[])
	 */
	public static final String MISSING_ELEMENT_VALUE = "Element '%s' requires a element value of the type '%s'";

	/**
	 * Error message for a missing element value 
	 * @see String#format(java.lang.String, java.lang.Object[])
	 */
	public static final String MISSING_ELEMENT_VALUE_PLAIN = "Element '%s' requires a element value'";

	/**
	 * Error message for a duplicated element
	 * @see String#format(java.lang.String, java.lang.Object[])
	 */
	public static final String DUPLICATE_ELEMENT = "Duplicated Element '%s'";

	/**
	 * Error message for a duplicated element value
	 * @see String#format(java.lang.String, java.lang.Object[])
	 */
	public static final String DUPLICATE_ELEMENT_VALUE = "Duplicated Element value for element '%s'";

	/**
	 * Error message for a duplicated attribute
	 * @see String#format(java.lang.String, java.lang.Object[])
	 */
	public static final String DUPLICATE_ATTRIBUTE = "Duplicated Attribute '%s'";

	/**
	 * Error message for an invalid attribute
	 * @see String#format(java.lang.String, java.lang.Object[])
	 */
	public static final String INVALID_ATTRIBUTE = "The attribute '%s' must be an %s";

	/**
	 * Error message for an invalid element value
	 * @see String#format(java.lang.String, java.lang.Object[])
	 */
	public static final String INVALID_ELEMENT_VALUE = "The element value '%s' must be an %s";

	/**
	 * @param aValue
	 */
	public abstract void processElementValue(String aValue);

	/**
	 * @param aQName
	 * @param aValue
	 */
	public abstract void processAttribute(QName aQName, String aValue);

	/**
	 * 
	 */
	public abstract void processEndElement();

	/**
	 * @param name
	 * @return
	 */
	public abstract AtomParser getChildParser(QName name);

}
