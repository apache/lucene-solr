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

import java.io.StringWriter;
import java.util.List;

import javax.xml.namespace.QName;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

import org.apache.lucene.gdata.gom.ContentType;
import org.apache.lucene.gdata.gom.GOMAttribute;
import org.apache.lucene.gdata.gom.GOMTextConstruct;
import org.apache.lucene.gdata.gom.core.utils.GOMUtils;
import org.apache.lucene.gdata.gom.writer.GOMOutputWriter;

/**
 * @author Simon Willnauer
 * 
 */
public abstract class GOMTextContructImpl extends AbstractGOMElement implements
		GOMTextConstruct {

	protected ContentType contentType;

	protected String rssLocalName;

	/*
	 * parses the xhtml content
	 */
	protected transient XMLBlobContentParser blobParser = null;

	/*
	 * this string builder contains the html while parsing the incoming text
	 * contruct. process element value will be called multiple times
	 */
	protected transient StringBuilder htmlBuilder = null;

	/**
	 * @return the contentType
	 * 
	 */
	public ContentType getContentType() {
		return this.contentType;
	}

	/**
	 * @see org.apache.lucene.gdata.gom.core.AtomParser#processElementValue(java.lang.String)
	 */
	public void processElementValue(String aValue) {
		if (this.htmlBuilder != null)
			this.htmlBuilder.append(aValue);
		else {
			this.textValue = aValue;
		}
	}

	/**
	 * @see org.apache.lucene.gdata.gom.core.AtomParser#processAttribute(javax.xml.namespace.QName,
	 *      java.lang.String)
	 */
	public void processAttribute(QName aQName, String aValue) {
		if (aQName == null)
			throw new GDataParseException("QName must not be null");
		if ("type".equals(aQName.getLocalPart()) && aValue != null) {
			if (this.contentType != null)
				throw new GDataParseException(String.format(
						DUPLICATE_ATTRIBUTE, "type"));
			this.contentType = ContentType.valueOf(aValue.toUpperCase());
			if (this.contentType == ContentType.HTML)
				this.htmlBuilder = new StringBuilder();
		}
		super.processAttribute(aQName, aValue);

	}

	/**
	 * @see org.apache.lucene.gdata.gom.core.AtomParser#processEndElement()
	 */
	public void processEndElement() {
		if (this.contentType == null)
			throw new GDataParseException(String.format(
					MISSING_ELEMENT_ATTRIBUTE, this.qname, "type"));
		switch (this.contentType) {
		case XHTML:
			if (this.blobParser != null) {
				this.textValue = this.blobParser.toString();
				this.blobParser.close();
				this.blobParser = null;
			}

			break;
		case HTML:
			if (this.htmlBuilder != null) {
				this.textValue = this.htmlBuilder.toString();
				this.htmlBuilder = null;
			}

		default:
			break;
		}

	}

	/**
	 * @see org.apache.lucene.gdata.gom.GOMElement#writeAtomOutput(org.apache.lucene.gdata.gom.writer.GOMStaxWriter)
	 */
	public void writeAtomOutput(GOMOutputWriter aStreamWriter)
			throws XMLStreamException {
		List<GOMAttribute> xmlNamespaceAttributes = getXmlNamespaceAttributes();
		xmlNamespaceAttributes.add(GOMUtils
				.getAttributeByContentTypeDefaultNs(this.contentType));
		if (this.contentType == ContentType.XHTML) {
			/*
			 * if the content is xhtml write it unescaped
			 */
			aStreamWriter.writeStartElement(this.localName,
					xmlNamespaceAttributes);
			aStreamWriter.writeContentUnescaped(this.textValue);
			aStreamWriter.writeEndElement();

		} else {
			// html and text will be escaped by stax writer
			aStreamWriter.writeSimpleXMLElement(this.localName,
					xmlNamespaceAttributes, this.textValue);
		}

	}

	/**
	 * @see org.apache.lucene.gdata.gom.GOMElement#writeRssOutput(org.apache.lucene.gdata.gom.writer.GOMStaxWriter)
	 */
	public void writeRssOutput(GOMOutputWriter aStreamWriter)
			throws XMLStreamException {
		/*
		 * RSS does not support markup as child elements StaX Writer will encode
		 * all containing markup into valid xml entities
		 */
		aStreamWriter.writeSimpleXMLElement(this.rssLocalName,
				getXmlNamespaceAttributes(), this.textValue);
	}

	/**
	 * @see org.apache.lucene.gdata.gom.core.AtomParser#getChildParser(javax.xml.namespace.QName)
	 */
	@Override
	public AtomParser getChildParser(QName aName) {
		if (aName == null)
			throw new GDataParseException("QName must not be null");
		if (this.contentType == ContentType.XHTML
				&& aName.getLocalPart().equals("div")) {
			if (this.blobParser != null)
				throw new GDataParseException(String.format(
						DUPLICATE_ELEMENT, "div"));
			this.blobParser = new XMLBlobContentParser();
			return this.blobParser.getChildParser(aName);
		}

		return super.getChildParser(aName);

	}

	class XMLBlobContentParser implements AtomParser {
		private StringWriter writer;

		private XMLStreamWriter xmlWriter;

		/**
		 * 
		 */
		public XMLBlobContentParser() {
			super();
			this.writer = new StringWriter();
			try {
				this.xmlWriter = XMLOutputFactory.newInstance()
						.createXMLStreamWriter(this.writer);
			} catch (Exception e) {
				throw new GDataParseException(e);
			}
		}

		/**
		 * @see org.apache.lucene.gdata.gom.core.AtomParser#processElementValue(java.lang.String)
		 */
		public void processElementValue(String aValue) {
			try {
				this.xmlWriter.writeCharacters(aValue);
			} catch (XMLStreamException e) {
				throw new GDataParseException(e);
			}

		}

		/**
		 * @see org.apache.lucene.gdata.gom.core.AtomParser#processAttribute(javax.xml.namespace.QName,
		 *      java.lang.String)
		 */
		public void processAttribute(QName aQName, String aValue) {
			try {
				this.xmlWriter.writeAttribute(aQName.getNamespaceURI(), aQName
						.getLocalPart(), aQName.getPrefix(), aValue);
			} catch (XMLStreamException e) {
				throw new GDataParseException(e);
			}

		}

		/**
		 * @see org.apache.lucene.gdata.gom.core.AtomParser#processEndElement()
		 */
		public void processEndElement() {
			try {
				this.xmlWriter.writeEndElement();
			} catch (XMLStreamException e) {
				throw new GDataParseException(e);
			}

		}

		/**
		 * @see org.apache.lucene.gdata.gom.core.AtomParser#getChildParser(javax.xml.namespace.QName)
		 */
		public AtomParser getChildParser(QName aName) {
			try {
				this.xmlWriter.writeStartElement(aName.getNamespaceURI(), aName
						.getLocalPart(), aName.getPrefix());
			} catch (XMLStreamException e) {
				throw new GDataParseException(e);
			}
			return this;
		}

		/**
		 * @see java.lang.Object#toString()
		 */
		public String toString() {
			return this.writer.toString();
		}

		/**
		 * 
		 */
		public void close() {
			try {
				this.xmlWriter.close();
				this.writer.close();
			} catch (Exception e) {
				throw new GDataParseException(e);
			}
		}

	}

}
