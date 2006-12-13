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

import java.net.URISyntaxException;
import java.util.List;

import javax.xml.namespace.QName;
import javax.xml.stream.XMLStreamException;

import org.apache.lucene.gdata.gom.GOMAttribute;
import org.apache.lucene.gdata.gom.GOMGenerator;
import org.apache.lucene.gdata.gom.GOMNamespace;
import org.apache.lucene.gdata.gom.core.utils.AtomParserUtils;
import org.apache.lucene.gdata.gom.writer.GOMOutputWriter;

/**
 * @author Simon Willnauer
 * 
 */
public class GOMGeneratorImpl extends AbstractGOMElement implements
		GOMGenerator {

	private String generatorVersion;

	private String uri;

	/**
	 * 
	 */
	public GOMGeneratorImpl() {
		super();
		this.localName = GOMGenerator.LOCALNAME;
		this.qname = new QName(GOMNamespace.ATOM_NS_URI, this.localName);
	}

	/**
	 * @see org.apache.lucene.gdata.gom.GOMGenerator#setUri(java.lang.String)
	 */
	public void setUri(String aUri) {
		this.uri = aUri;

	}

	/**
	 * @see org.apache.lucene.gdata.gom.GOMGenerator#setGeneratorVersion(java.lang.String)
	 */
	public void setGeneratorVersion(String aVersion) {
		this.generatorVersion = aVersion;

	}

	/**
	 * @see org.apache.lucene.gdata.gom.GOMGenerator#getGeneratorVersion()
	 */
	public String getGeneratorVersion() {
		return this.generatorVersion;
	}

	/**
	 * @see org.apache.lucene.gdata.gom.GOMGenerator#getUri()
	 */
	public String getUri() {
		return this.uri;
	}

	/**
	 * @see org.apache.lucene.gdata.gom.core.AbstractGOMElement#processAttribute(javax.xml.namespace.QName,
	 *      java.lang.String)
	 */
	@Override
	public void processAttribute(QName aQName, String aValue) {
		if (aQName == null)
			throw new IllegalArgumentException("Qname must not be null");
		if (aValue == null)
			throw new IllegalArgumentException("Value must not be null");
		if (aQName.getNamespaceURI().equals(GOMNamespace.ATOM_NS_URI)) {
			if (aQName.getLocalPart().equals("uri")) {
				if (this.uri != null)
					throw new GDataParseException(String.format(
							AtomParser.DUPLICATE_ATTRIBUTE, "uri"));
				this.uri = aValue;
			} else if (aQName.getLocalPart().equals("version")) {
				if (this.generatorVersion != null)
					throw new GDataParseException(String.format(
							AtomParser.DUPLICATE_ATTRIBUTE, "version"));
				this.generatorVersion = aValue;
			}
		}
		super.processAttribute(aQName, aValue);
	}

	/**
	 * @see org.apache.lucene.gdata.gom.core.AbstractGOMElement#processElementValue(java.lang.String)
	 */
	@Override
	public void processElementValue(String aValue) {
		if (this.textValue != null)
			throw new GDataParseException(String.format(
					AtomParser.DUPLICATE_ELEMENT_VALUE, this.localName));
		this.textValue = aValue;

	}

	/**
	 * @see org.apache.lucene.gdata.gom.core.AbstractGOMElement#processEndElement()
	 */
	@Override
	public void processEndElement() {
		if (this.uri != null)
			try {
				AtomParserUtils.getAbsolutAtomURI(this.xmlBase, this.uri);
			} catch (URISyntaxException e) {
				throw new GDataParseException(String.format(
						AtomParser.INVALID_ELEMENT_VALUE, this.localName,
						"absolute uri"));
			}

	}

	/**
	 * @see org.apache.lucene.gdata.gom.GOMElement#writeAtomOutput(org.apache.lucene.gdata.gom.writer.GOMStaxWriter)
	 */
	public void writeAtomOutput(GOMOutputWriter aStreamWriter)
			throws XMLStreamException {
		List<GOMAttribute> list = getXmlNamespaceAttributes();
		if (this.uri != null)
			list.add(new GOMAttributeImpl("uri", this.uri));
		if (this.generatorVersion != null)
			list.add(new GOMAttributeImpl("version", this.generatorVersion));

		aStreamWriter.writeSimpleXMLElement(this.qname, list, this.textValue);
	}

	/**
	 * @see org.apache.lucene.gdata.gom.GOMElement#writeRssOutput(org.apache.lucene.gdata.gom.writer.GOMStaxWriter)
	 */
	public void writeRssOutput(GOMOutputWriter aStreamWriter)
			throws XMLStreamException {
		aStreamWriter.writeSimpleXMLElement(this.localName,
				getXmlNamespaceAttributes(), this.textValue);
	}

}
