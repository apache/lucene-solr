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

import org.apache.lucene.gdata.gom.AtomMediaType;
import org.apache.lucene.gdata.gom.GOMAttribute;
import org.apache.lucene.gdata.gom.GOMContent;
import org.apache.lucene.gdata.gom.GOMNamespace;
import org.apache.lucene.gdata.gom.core.utils.AtomParserUtils;
import org.apache.lucene.gdata.gom.core.utils.GOMUtils;
import org.apache.lucene.gdata.gom.writer.GOMOutputWriter;

/**
 * @author Simon Willnauer
 * 
 */
public class GOMContentImpl extends GOMTextContructImpl implements GOMContent {
	private String src;

	private String type;

	private AtomMediaType mediaType;

	/**
	 * 
	 */
	public GOMContentImpl() {
		this.localName = GOMContent.LOCALNAME;
		this.qname = new QName(GOMNamespace.ATOM_NS_URI, this.localName);
		this.rssLocalName = GOMContent.LOCAL_NAME_RSS;
	}

	/**
	 * @see org.apache.lucene.gdata.gom.GOMContent#getSrc()
	 */
	public String getSrc() {
		return this.src;
	}

	/**
	 * @see org.apache.lucene.gdata.gom.GOMContent#setSrc(java.lang.String)
	 */
	public void setSrc(String aSrc) {
		this.src = aSrc;

	}

	/**
	 * @see org.apache.lucene.gdata.gom.core.GOMTextContructImpl#processAttribute(javax.xml.namespace.QName,
	 *      java.lang.String)
	 */
	@Override
	public void processAttribute(QName aQName, String aValue) {
		if (aQName == null)
			throw new GDataParseException("QName must not be null");
		if (aValue == null)
			throw new GDataParseException("Value must not be null");
		if ("src".equals(aQName.getLocalPart())) {
			if (this.src != null)
				throw new GDataParseException(String.format(
						DUPLICATE_ATTRIBUTE, "src"));
			this.src = aValue;
			return;
		}
		if ("type".equals(aQName.getLocalPart())) {
			if (this.contentType != null || this.mediaType != null)
				throw new GDataParseException(String.format(
						DUPLICATE_ATTRIBUTE, "type"));
			if (AtomParserUtils.isAtomMediaType(aValue)) {
				this.type = aValue;
				this.mediaType = AtomParserUtils.getAtomMediaType(aValue);
				return;
			}

		}
		super.processAttribute(aQName, aValue);

	}

	/**
	 * @see org.apache.lucene.gdata.gom.core.GOMTextContructImpl#processElementValue(java.lang.String)
	 */
	@Override
	public void processElementValue(String aValue) {
		if (this.src != null)
			throw new GDataParseException(String.format(
					AtomParser.UNEXPECTED_ELEMENT_VALUE, this.localName
							+ " with attribute src set "));
		super.processElementValue(aValue);
	}

	/**
	 * @see org.apache.lucene.gdata.gom.core.GOMTextContructImpl#processEndElement()
	 */
	@Override
	public void processEndElement() {
		if (this.src != null)
			try {
				AtomParserUtils.getAbsolutAtomURI(this.xmlBase, this.src);
			} catch (URISyntaxException e) {
				throw new GDataParseException(String.format(INVALID_ATTRIBUTE,
						"src", "absolute uri"), e);
			}

		if (this.mediaType == null)
			super.processEndElement();
		else if (this.blobParser != null) {
			this.textValue = this.blobParser.toString();
			this.blobParser.close();
			this.blobParser = null;
		}

	}

	/**
	 * @see org.apache.lucene.gdata.gom.core.GOMTextContructImpl#getChildParser(javax.xml.namespace.QName)
	 */
	@Override
	public AtomParser getChildParser(QName aName) {
		if (aName == null)
			throw new GDataParseException("QName must not be null");
		if (this.mediaType == AtomMediaType.XML) {
			if (this.blobParser != null)
				throw new GDataParseException(String.format(
						DUPLICATE_ELEMENT, aName.getLocalPart()));
			this.blobParser = new XMLBlobContentParser();
			return this.blobParser.getChildParser(aName);
		}
		return super.getChildParser(aName);
	}

	/**
	 * @see org.apache.lucene.gdata.gom.GOMElement#writeAtomOutput(org.apache.lucene.gdata.gom.writer.GOMStaxWriter)
	 */
	public void writeAtomOutput(GOMOutputWriter aStreamWriter)
			throws XMLStreamException {

		if (this.mediaType != null) {
			List<GOMAttribute> xmlNamespaceAttributes = getXmlNamespaceAttributes();
			xmlNamespaceAttributes.add(GOMUtils.buildDefaultNamespaceAttribute(
					this.type, "type"));
			aStreamWriter.writeStartElement(this.localName,
					xmlNamespaceAttributes);
			if (this.src == null)
				aStreamWriter.writeContentUnescaped(this.textValue);
			else
				aStreamWriter.writeAttribute(GOMUtils
						.buildDefaultNamespaceAttribute(this.src, "src"));
			aStreamWriter.writeEndElement();

		} else {
			super.writeAtomOutput(aStreamWriter);
		}

	}

	/**
	 * @see org.apache.lucene.gdata.gom.core.GOMTextContructImpl#writeRssOutput(org.apache.lucene.gdata.gom.writer.GOMStaxWriter)
	 */
	@Override
	public void writeRssOutput(GOMOutputWriter aStreamWriter)
			throws XMLStreamException {
		if (this.mediaType != null) {
			// if content is atomOutOfLineContent (has not textValue) ->
			// generate a <link> element.
			if (src != null) {
				aStreamWriter.writeSimpleXMLElement("link", null, this.src);
			} else if (this.mediaType == AtomMediaType.TEXT) {
				aStreamWriter.writeSimpleXMLElement("description", null,
						this.textValue);
			} else {
				// RSS doesn't support non-text content --> write atom type
				this.writeAtomOutput(aStreamWriter);
			}
		} else {
			super.writeRssOutput(aStreamWriter);
		}
	}

	/**
	 * @see org.apache.lucene.gdata.gom.GOMContent#setAtomMediaType(org.apache.lucene.gdata.gom.AtomMediaType)
	 */
	public void setAtomMediaType(AtomMediaType aMediaType) {

		this.mediaType = aMediaType;

	}

	/**
	 * @see org.apache.lucene.gdata.gom.GOMContent#getAtomMediaType()
	 */
	public AtomMediaType getAtomMediaType() {
		return this.mediaType;
	}

}
