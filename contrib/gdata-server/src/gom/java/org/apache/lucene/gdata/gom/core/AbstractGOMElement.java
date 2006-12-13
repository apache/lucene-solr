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

import java.util.LinkedList;
import java.util.List;

import javax.xml.namespace.QName;
import javax.xml.stream.XMLStreamException;

import org.apache.lucene.gdata.gom.GOMAttribute;
import org.apache.lucene.gdata.gom.GOMElement;
import org.apache.lucene.gdata.gom.GOMNamespace;
import org.apache.lucene.gdata.gom.writer.GOMOutputWriter;

/**
 * @author Simon Willnauer
 */
public abstract class AbstractGOMElement implements GOMElement {
	/**
	 * atomCommonAttribute <br/> attribute xml:lang { atomLanguageTag }?
	 */
	protected String xmlLang;

	/**
	 * atomCommonAttribute <br/> attribute xml:base { atomUri }?
	 */
	protected String xmlBase;

	protected QName qname;

	protected String textValue;

	protected String localName;

	protected String nsUri;

	protected String nsPrefix;

	/**
	 * atomCommonAttributes <br/> undefinedAttribute*
	 */
	protected List<GOMAttribute> extensionAttributes = new LinkedList<GOMAttribute>();

	/**
	 * {@inheritDoc}
	 * 
	 * @see org.apache.lucene.gdata.gom.GOMXmlEntity#getQname()
	 */
	public QName getQname() {
		return this.qname;
	}

	/**
	 * {@inheritDoc}
	 * 
	 * @see org.apache.lucene.gdata.gom.GOMXmlEntity#getTextValue()
	 */
	public String getTextValue() {
		return this.textValue;
	}

	/**
	 * {@inheritDoc}
	 * 
	 * @see org.apache.lucene.gdata.gom.GOMXmlEntity#setTextValue(java.lang.String)
	 */
	public void setTextValue(String aTextValue) {
		this.textValue = aTextValue;

	}

	/**
	 * {@inheritDoc}
	 * 
	 * @see org.apache.lucene.gdata.gom.GOMXmlEntity#getLocalName()
	 */
	public String getLocalName() {
		return this.localName;
	}

	/**
	 * {@inheritDoc}
	 * 
	 * @see org.apache.lucene.gdata.gom.GOMXmlEntity#setLocalName(java.lang.String)
	 */
	public void setLocalName(String aLocalName) {
		// must override
	}

	protected void addAttribute(GOMAttribute aAttribute) {
		if (aAttribute != null)
			this.extensionAttributes.add(aAttribute);

	}

	/**
	 * {@inheritDoc}
	 * 
	 * @see org.apache.lucene.gdata.gom.core.AtomParser#getChildParser(javax.xml.namespace.QName)
	 */
	public AtomParser getChildParser(QName aName) {
		throw new GDataParseException(String.format(UNEXPECTED_ELEMENT_CHILD,
				this.qname));
	}

	/**
	 * {@inheritDoc}
	 * 
	 * @see org.apache.lucene.gdata.gom.core.AtomParser#processAttribute(javax.xml.namespace.QName,
	 *      java.lang.String)
	 */
	public void processAttribute(QName aQName, String aValue) {
		if (aQName == null)
			throw new GDataParseException("QName must not be null");
		if (aQName.getNamespaceURI().equals(GOMNamespace.XML_NS_URI)) {
			if (aQName.getLocalPart().equals(XML_BASE))
				this.xmlBase = aValue;
			else if (aQName.getLocalPart().equals(XML_LANG))
				this.xmlLang = aValue;

		} else {
			GOMAttributeImpl impl = new GOMAttributeImpl(aQName
					.getNamespaceURI(), aQName.getPrefix(), aQName
					.getLocalPart(), aValue);
			this.addAttribute(impl);
		}

	}

	/**
	 * {@inheritDoc}
	 * 
	 * @see org.apache.lucene.gdata.gom.core.AtomParser#processElementValue(java.lang.String)
	 */
	public void processElementValue(String aValue) {
		throw new GDataParseException(String.format(UNEXPECTED_ELEMENT_VALUE,
				this.qname));
	}

	/**
	 * {@inheritDoc}
	 * 
	 * @see org.apache.lucene.gdata.gom.core.AtomParser#processEndElement()
	 */
	public void processEndElement() {
		// no post processing

	}

	/**
	 * {@inheritDoc}
	 * 
	 * @see org.apache.lucene.gdata.gom.GOMXmlEntity#setNamespaceUri(java.lang.String)
	 */
	public void setNamespaceUri(String aString) {
		this.nsUri = aString;

	}

	/**
	 * 
	 * {@inheritDoc}
	 * 
	 * @see org.apache.lucene.gdata.gom.GOMXmlEntity#setNamespacePrefix(java.lang.String)
	 */
	public void setNamespacePrefix(String aString) {
		this.nsPrefix = aString;
	}

	protected List<GOMAttribute> getXmlNamespaceAttributes() {
		List<GOMAttribute> retVal = new LinkedList<GOMAttribute>();
		if (this.xmlBase != null)
			retVal.add(new GOMAttributeImpl(GOMNamespace.XML_NS_URI,
					GOMNamespace.XML_NS_PREFIX, "base", this.xmlBase));
		if (this.xmlLang != null)
			retVal.add(new GOMAttributeImpl(GOMNamespace.XML_NS_URI,
					GOMNamespace.XML_NS_PREFIX, "lang", this.xmlLang));
		return retVal;

	}

	/**
	 * {@inheritDoc}
	 * 
	 * @see org.apache.lucene.gdata.gom.GOMElement#writeRssOutput(org.apache.lucene.gdata.gom.writer.GOMStaxWriter,
	 *      java.lang.String)
	 */
	public void writeRssOutput(GOMOutputWriter aStreamWriter, String aRssName)
			throws XMLStreamException {

	}




	/**
	 * 
	 * {@inheritDoc}
	 * 
	 * @see org.apache.lucene.gdata.gom.GOMElement#getXmlBase()
	 */
	public String getXmlBase() {
		return this.xmlBase;
	}

	/**
	 * 
	 * {@inheritDoc}
	 * 
	 * @see org.apache.lucene.gdata.gom.GOMElement#getXmlLang()
	 */
	public String getXmlLang() {

		return this.xmlLang;
	}

}
