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
import org.apache.lucene.gdata.gom.GOMLink;
import org.apache.lucene.gdata.gom.GOMNamespace;
import org.apache.lucene.gdata.gom.core.utils.AtomParserUtils;
import org.apache.lucene.gdata.gom.core.utils.GOMUtils;
import org.apache.lucene.gdata.gom.writer.GOMOutputWriter;

/**
 * element atom:link { atomCommonAttributes, attribute href { atomUri },
 * attribute rel { atomNCName | atomUri }?, attribute type { atomMediaType }?,
 * attribute hreflang { atomLanguageTag }?, attribute title { text }?, attribute
 * length { text }?, undefinedContent }
 * 
 * @author Simon Willnauer
 * 
 */
public class GOMLinkImpl extends AbstractGOMElement implements GOMLink {
	private String href;

	private String rel;

	private String type;

	private String hrefLang;

	private String title;

	private Integer length;

	/**
	 * 
	 */
	public GOMLinkImpl() {
		super();
		this.localName = LOCALNAME;
		this.qname = new QName(this.localName);
	}

	/**
	 * @see org.apache.lucene.gdata.gom.GOMLink#getHref()
	 */
	public String getHref() {
		return this.href;
	}

	/**
	 * @see org.apache.lucene.gdata.gom.GOMLink#setHref(java.lang.String)
	 */
	public void setHref(String aHref) {
		href = aHref;
	}

	/**
	 * @see org.apache.lucene.gdata.gom.GOMLink#getHrefLang()
	 */
	public String getHrefLang() {
		return this.hrefLang;
	}

	/**
	 * @see org.apache.lucene.gdata.gom.GOMLink#setHrefLang(java.lang.String)
	 */
	public void setHrefLang(String aHrefLang) {
		hrefLang = aHrefLang;
	}

	/**
	 * @see org.apache.lucene.gdata.gom.GOMLink#getLength()
	 */
	public Integer getLength() {
		return this.length;
	}

	/**
	 * @see org.apache.lucene.gdata.gom.GOMLink#setLength(java.lang.String)
	 */
	public void setLength(Integer aLength) {
		length = aLength;
	}

	/**
	 * @see org.apache.lucene.gdata.gom.GOMLink#getRel()
	 */
	public String getRel() {
		return this.rel;
	}

	/**
	 * @see org.apache.lucene.gdata.gom.GOMLink#setRel(java.lang.String)
	 */
	public void setRel(String aRel) {
		rel = aRel;
	}

	/**
	 * @see org.apache.lucene.gdata.gom.GOMLink#getTitle()
	 */
	public String getTitle() {
		return this.title;
	}

	/**
	 * @see org.apache.lucene.gdata.gom.GOMLink#setTitle(java.lang.String)
	 */
	public void setTitle(String aTitle) {
		title = aTitle;
	}

	/**
	 * @see org.apache.lucene.gdata.gom.GOMLink#getType()
	 */
	public String getType() {
		return this.type;
	}

	/**
	 * @see org.apache.lucene.gdata.gom.GOMLink#setType(java.lang.String)
	 */
	public void setType(String aType) {
		type = aType;
	}

	/**
	 * @see org.apache.lucene.gdata.gom.core.AbstractGOMElement#processAttribute(javax.xml.namespace.QName,
	 *      java.lang.String)
	 */
	@Override
	public void processAttribute(QName aQName, String aValue) {
		if (aQName == null)
			throw new IllegalArgumentException("QName must not be null");

		if (aQName.getNamespaceURI().equals(GOMNamespace.ATOM_NS_URI)
				|| aQName.getNamespaceURI().equals("")) {
			String localName = aQName.getLocalPart();

			if (localName.equals("href")) {
				if (this.href != null)
					throw new GDataParseException(String.format(
							DUPLICATE_ATTRIBUTE, "href"));
				this.href = aValue;
			} else if (localName.equals("type")) {
				if (this.type != null)
					throw new GDataParseException(String.format(
							DUPLICATE_ATTRIBUTE, "type"));
				this.type = aValue;
			} else if (localName.equals("rel")) {
				if (this.rel != null)
					throw new GDataParseException(String.format(
							DUPLICATE_ATTRIBUTE, "rel"));
				this.rel = aValue;
			} else if (localName.equals("title")) {
				if (this.title != null)
					throw new GDataParseException(String.format(
							DUPLICATE_ATTRIBUTE, "title"));
				this.title = aValue;

			} else if (localName.equals("hreflang")) {
				if (this.hrefLang != null)
					throw new GDataParseException(String.format(
							DUPLICATE_ATTRIBUTE, "hreflang"));
				this.hrefLang = aValue;
			} else if (localName.equals("length")) {
				if (this.length != null)
					throw new GDataParseException(String.format(
							DUPLICATE_ATTRIBUTE, "length"));
				try {
					this.length = new Integer(Integer.parseInt(aValue));
				} catch (NumberFormatException e) {
					throw new GDataParseException(
							"attribute lenght must be an integer");
				}
			}

		}
		super.processAttribute(aQName, aValue);
	}

	/**
	 * @see org.apache.lucene.gdata.gom.core.AbstractGOMElement#processEndElement()
	 */
	@Override
	public void processEndElement() {
		if (this.href == null)
			throw new GDataParseException(String.format(
					MISSING_ELEMENT_ATTRIBUTE, this.localName, "href"));
		try {
			AtomParserUtils.getAbsolutAtomURI(this.xmlBase, this.href);
		} catch (URISyntaxException e) {
			throw new GDataParseException(String.format(INVALID_ATTRIBUTE,
					"href", "absolute uri"), e);
		}

	}

	/**
	 * @see org.apache.lucene.gdata.gom.GOMElement#writeAtomOutput(org.apache.lucene.gdata.gom.writer.GOMStaxWriter)
	 */
	public void writeAtomOutput(GOMOutputWriter aStreamWriter)
			throws XMLStreamException {
		List<GOMAttribute> attList = getXmlNamespaceAttributes();
		attList.add(GOMUtils.buildDefaultNamespaceAttribute(
				this.href == null ? "" : this.href, "href"));
		if (this.rel != null)
			attList.add(GOMUtils
					.buildDefaultNamespaceAttribute(this.rel, "rel"));
		if (this.title != null)
			attList.add(GOMUtils.buildDefaultNamespaceAttribute(this.title,
					"title"));
		if (this.type != null)
			attList.add(GOMUtils.buildDefaultNamespaceAttribute(this.type,
					"type"));
		if (this.hrefLang != null)
			attList.add(GOMUtils.buildDefaultNamespaceAttribute(this.hrefLang,
					"hreflang"));
		if (this.length != null)
			attList.add(GOMUtils.buildDefaultNamespaceAttribute(this.length
					.toString(), "length"));

		aStreamWriter.writeSimpleXMLElement(this.qname, attList, null);

	}

	/**
	 * @see org.apache.lucene.gdata.gom.GOMElement#writeRssOutput(org.apache.lucene.gdata.gom.writer.GOMStaxWriter)
	 */
	public void writeRssOutput(GOMOutputWriter aStreamWriter)
			throws XMLStreamException {
		List<GOMAttribute> xmlNamespaceAttributes = getXmlNamespaceAttributes();
		if (this.rel != null && rel.equalsIgnoreCase("enclosure")) {
			if (type != null)
				xmlNamespaceAttributes.add(GOMUtils
						.buildDefaultNamespaceAttribute(type, "type"));
			if (href != null)
				xmlNamespaceAttributes.add(GOMUtils
						.buildDefaultNamespaceAttribute(href, "href"));

			aStreamWriter.writeSimpleXMLElement("enclosure",
					xmlNamespaceAttributes, null);
		} else if ("comments".equalsIgnoreCase(this.rel))
			aStreamWriter.writeSimpleXMLElement("comments", null, this.href);

		else if ("alternate".equalsIgnoreCase(this.rel))
			aStreamWriter.writeSimpleXMLElement("link", null, this.href);
	}

}
