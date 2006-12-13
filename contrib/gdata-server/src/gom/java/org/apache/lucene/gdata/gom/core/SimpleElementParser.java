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
abstract class SimpleElementParser implements AtomParser {
	protected String aString = null;

	protected String localname = null;

	/**
	 * @see org.apache.lucene.gdata.gom.core.AtomParser#processElementValue(java.lang.String)
	 */
	public void processElementValue(String aValue) {
		this.aString = aValue;

	}

	/**
	 * @see org.apache.lucene.gdata.gom.core.AtomParser#processAttribute(javax.xml.namespace.QName,
	 *      java.lang.String)
	 */
	public void processAttribute(QName aQName, String aValue) {
		//

	}

	/**
	 * @see org.apache.lucene.gdata.gom.core.AtomParser#processEndElement()
	 */
	public abstract void processEndElement();

	/**
	 * @see org.apache.lucene.gdata.gom.core.AtomParser#getChildParser(javax.xml.namespace.QName)
	 */
	public AtomParser getChildParser(QName aName) {
		if (aName == null)
			throw new GDataParseException("QName must not be null");
		throw new GDataParseException(String.format(
				AtomParser.UNEXPECTED_ELEMENT_CHILD, this.localname));
	}

}
