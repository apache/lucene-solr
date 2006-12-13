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

import org.apache.lucene.gdata.gom.GOMAttribute;

/**
 * @author Simon Willnauer
 * 
 */
public class GOMAttributeImpl implements GOMAttribute {
	private boolean hasDefaultNamespace;

	private QName qName;

	private String localName;

	private String uri;

	private String prefix;

	private String value;

	/**
	 * 
	 */
	public GOMAttributeImpl() {
		super();
	}

	/**
	 * @param localName
	 * @param value
	 */
	public GOMAttributeImpl(String localName, String value) {
		this.hasDefaultNamespace = true;
		this.value = value;
		this.localName = localName;
	}

	/**
	 * @param namespaceUri
	 * @param namespacePrefix
	 * @param localName
	 * @param value
	 */
	public GOMAttributeImpl(String namespaceUri, String namespacePrefix,
			String localName, String value) {
		this.localName = localName;
		this.uri = namespaceUri;
		this.prefix = namespacePrefix;
		this.value = value;
	}

	/**
	 * @see org.apache.lucene.gdata.gom.GOMXmlEntity#getQname()
	 */
	public QName getQname() {
		if (this.qName == null)
			this.qName = new QName(this.uri, (this.localName == null ? ""
					: this.localName), (this.prefix == null ? "" : this.prefix));
		return this.qName;

	}

	/**
	 * @see org.apache.lucene.gdata.gom.GOMXmlEntity#setLocalName(java.lang.String)
	 */
	public void setLocalName(String aLocalName) {
		if (aLocalName == null)
			return;
		this.qName = null;
		this.localName = aLocalName;

	}

	/**
	 * @see org.apache.lucene.gdata.gom.GOMXmlEntity#getLocalName()
	 */
	public String getLocalName() {
		return this.localName;
	}

	/**
	 * @see org.apache.lucene.gdata.gom.GOMXmlEntity#getTextValue()
	 */
	public String getTextValue() {
		return this.value == null ? "" : this.value;
	}

	/**
	 * @see org.apache.lucene.gdata.gom.GOMXmlEntity#setTextValue(java.lang.String)
	 */
	public void setTextValue(String aTextValue) {
		this.value = aTextValue;

	}

	/**
	 * @see org.apache.lucene.gdata.gom.GOMAttribute#hasDefaultNamespace()
	 */
	public boolean hasDefaultNamespace() {

		return this.hasDefaultNamespace;
	}

	void setHasDefaultNamespace(boolean aBoolean) {
		this.hasDefaultNamespace = aBoolean;
	}

	/**
	 * @see org.apache.lucene.gdata.gom.GOMXmlEntity#setNamespaceUri(java.lang.String)
	 */
	public void setNamespaceUri(String aString) {
		if (aString == null)
			return;
		this.qName = null;
		this.hasDefaultNamespace = false;
		this.uri = aString;

	}

	/**
	 * @see org.apache.lucene.gdata.gom.GOMXmlEntity#setNamespacePrefix(java.lang.String)
	 */
	public void setNamespacePrefix(String aString) {
		if (aString == null)
			return;
		this.qName = null;
		this.hasDefaultNamespace = false;
		this.prefix = aString;

	}

}
