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

import java.util.ArrayList;
import java.util.List;

import javax.xml.namespace.QName;
import javax.xml.stream.XMLStreamException;

import org.apache.lucene.gdata.gom.GOMAttribute;
import org.apache.lucene.gdata.gom.GOMNamespace;
import org.apache.lucene.gdata.gom.core.utils.GOMUtils;
import org.apache.lucene.gdata.gom.writer.GOMOutputWriter;

/**
 * @author Simon Willnauer
 * 
 */
public class GOMPersonImpl extends AbstractGOMElement implements
		org.apache.lucene.gdata.gom.GOMPerson {

	private final static String NAME_LOCAL_NAME = "name";

	private final static String EMAIL_LOCAL_NAME = "email";

	private final static String URI_LOCAL_NAME = "uri";

	protected String uri;

	protected String email;

	protected String name;

	/**
	 * 
	 */
	public GOMPersonImpl() {
		super();
		this.localName = LOCALNAME;
		this.qname = new QName(GOMNamespace.ATOM_NS_URI, this.localName);

	}

	/**
	 * @see org.apache.lucene.gdata.gom.GOMPerson#setName(java.lang.String)
	 */
	public void setName(String aName) {
		this.name = aName;

	}

	/**
	 * @see org.apache.lucene.gdata.gom.GOMPerson#setEmail(java.lang.String)
	 */
	public void setEmail(String aEmail) {
		this.email = aEmail;
	}

	/**
	 * @see org.apache.lucene.gdata.gom.GOMPerson#setUri(java.lang.String)
	 */
	public void setUri(String aUri) {
		this.uri = aUri;
	}

	/**
	 * @see org.apache.lucene.gdata.gom.GOMPerson#getName()
	 */
	public String getName() {

		return this.name;
	}

	/**
	 * @see org.apache.lucene.gdata.gom.GOMPerson#getEmail()
	 */
	public String getEmail() {
		return this.email;
	}

	/**
	 * @see org.apache.lucene.gdata.gom.GOMPerson#getUri()
	 */
	public String getUri() {

		return this.uri;
	}

	/**
	 * @see org.apache.lucene.gdata.gom.GOMElement#writeAtomOutput(org.apache.lucene.gdata.gom.writer.GOMStaxWriter)
	 */
	public void writeAtomOutput(GOMOutputWriter aStreamWriter)
			throws XMLStreamException {
		if (aStreamWriter == null)
			throw new GDataParseException("GOMWriter must not be null");
		List<GOMAttribute> list = getXmlNamespaceAttributes();
		if (this.xmlLang != null) {
			list = new ArrayList<GOMAttribute>(1);
			list.add(GOMUtils
					.buildXMLNamespaceAttribute(this.xmlLang, XML_LANG));
		}
		aStreamWriter.writeStartElement(this.qname, list);
		aStreamWriter.writeSimpleXMLElement(NAME_LOCAL_NAME, this.name, null);
		if (this.email != null)
			aStreamWriter.writeSimpleXMLElement(EMAIL_LOCAL_NAME, this.email,
					null);
		if (this.uri != null)
			aStreamWriter.writeSimpleXMLElement(URI_LOCAL_NAME, this.uri, null);

		aStreamWriter.writeEndElement();

	}

	/**
	 * @see org.apache.lucene.gdata.gom.GOMElement#writeRssOutput(org.apache.lucene.gdata.gom.writer.GOMStaxWriter)
	 */
	public void writeRssOutput(GOMOutputWriter aStreamWriter)
			throws XMLStreamException {
		//
	}

	/**
	 * @see org.apache.lucene.gdata.gom.core.AbstractGOMElement#getChildParser(javax.xml.namespace.QName)
	 */
	@Override
	public AtomParser getChildParser(QName aName) {
		if (aName == null)
			throw new GDataParseException("QName must not be null");
		if (aName.getNamespaceURI().equals(GOMNamespace.ATOM_NS_URI)) {
			if (aName.getLocalPart().equals(NAME_LOCAL_NAME))
				return this.new NameParser();
			if (aName.getLocalPart().equals(URI_LOCAL_NAME))
				return this.new UriParser();
			if (aName.getLocalPart().equals(EMAIL_LOCAL_NAME))
				return this.new EmailParser();
		}
		return super.getChildParser(aName);
	}

	/**
	 * @see org.apache.lucene.gdata.gom.core.AbstractGOMElement#processEndElement()
	 */
	@Override
	public void processEndElement() {
		if (this.name == null)
			throw new GDataParseException(String.format(
					AtomParser.MISSING_ELEMENT_CHILD, this.localName,
					NAME_LOCAL_NAME));
	}

	class NameParser extends SimpleElementParser {

		NameParser() {
			this.localname = NAME_LOCAL_NAME;
		}

		/**
		 * @see org.apache.lucene.gdata.gom.core.AtomParser#processEndElement()
		 */
		public void processEndElement() {
			if (name != null)
				throw new GDataParseException(String.format(
						AtomParser.DUPLICATE_ELEMENT, this.localname));
			if (this.aString != null)
				name = this.aString;

		}

	}

	class UriParser extends SimpleElementParser {

		UriParser() {
			this.localname = URI_LOCAL_NAME;
		}

		/**
		 * @see org.apache.lucene.gdata.gom.core.AtomParser#processEndElement()
		 */
		public void processEndElement() {
			if (uri != null)
				throw new GDataParseException(String.format(
						AtomParser.DUPLICATE_ELEMENT, this.localname));
			if (this.aString != null)
				uri = this.aString;

		}

	}

	class EmailParser extends SimpleElementParser {

		EmailParser() {
			this.localname = EMAIL_LOCAL_NAME;
		}

		/**
		 * @see org.apache.lucene.gdata.gom.core.AtomParser#processEndElement()
		 */
		public void processEndElement() {
			if (email != null)
				throw new GDataParseException(String.format(
						AtomParser.DUPLICATE_ELEMENT, this.localname));
			if (this.aString != null)
				email = this.aString;

		}

	}

	/**
	 * @see org.apache.lucene.gdata.gom.core.AbstractGOMElement#writeRssOutput(org.apache.lucene.gdata.gom.writer.GOMOutputWriter,
	 *      java.lang.String)
	 */
	@Override
	public void writeRssOutput(GOMOutputWriter aStreamWriter, String aRssName)
			throws XMLStreamException {
		if (aStreamWriter == null)
			throw new GDataParseException("GOMWriter must not be null");
		StringBuilder builder = new StringBuilder("");
		if (this.email != null)
			builder.append(this.email);
		if (this.name != null)
			builder.append("(").append(this.name).append(")");
		aStreamWriter.writeSimpleXMLElement(aRssName,
				getXmlNamespaceAttributes(), builder.toString());

	}

}
