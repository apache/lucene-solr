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
import java.util.LinkedList;
import java.util.List;

import javax.xml.namespace.QName;
import javax.xml.stream.XMLStreamException;

import org.apache.lucene.gdata.gom.GOMAttribute;
import org.apache.lucene.gdata.gom.GOMCategory;
import org.apache.lucene.gdata.gom.GOMNamespace;
import org.apache.lucene.gdata.gom.core.utils.AtomParserUtils;
import org.apache.lucene.gdata.gom.core.utils.GOMUtils;
import org.apache.lucene.gdata.gom.writer.GOMOutputWriter;

/**
 * @author Simon Willnauer
 * 
 */
public class GOMCategoryImpl extends AbstractGOMElement implements GOMCategory {

	private static final String DOMAIN = "domain";

	protected String term;

	protected String label;

	protected String scheme;

	/**
	 * 
	 */
	public GOMCategoryImpl() {
		super();
		this.localName = LOCALNAME;
		this.qname = new QName(GOMNamespace.ATOM_NS_URI, this.localName);
	}

	/**
	 * @see org.apache.lucene.gdata.gom.GOMCategory#setTerm(java.lang.String)
	 */
	public void setTerm(String aTerm) {

		this.term = aTerm;

	}

	/**
	 * @see org.apache.lucene.gdata.gom.GOMCategory#setLabel(java.lang.String)
	 */
	public void setLabel(String aLabel) {
		this.label = aLabel;

	}

	/**
	 * @see org.apache.lucene.gdata.gom.GOMCategory#setScheme(java.lang.String)
	 */
	public void setScheme(String aScheme) {
		this.scheme = aScheme;
	}

	/**
	 * @see org.apache.lucene.gdata.gom.GOMCategory#getTerm()
	 */
	public String getTerm() {
		return this.term;
	}

	/**
	 * @see org.apache.lucene.gdata.gom.GOMCategory#getScheme()
	 */
	public String getScheme() {
		return this.scheme;
	}

	/**
	 * @see org.apache.lucene.gdata.gom.GOMCategory#getLabel()
	 */
	public String getLabel() {
		return this.label;
	}

	/**
	 * @see org.apache.lucene.gdata.gom.core.AbstractGOMElement#processAttribute(javax.xml.namespace.QName,
	 *      java.lang.String)
	 */
	@Override
	public void processAttribute(QName aQName, String aValue) {
		if (aQName == null)
			throw new GDataParseException("QName must not be null");
		if (aQName.getNamespaceURI().equals(GOMNamespace.ATOM_NS_URI)
				|| aQName.getNamespaceURI().equals("")) {
			String localPart = aQName.getLocalPart();
			if (localPart.equals(TERM_ATTRIBUTE)) {
				if (this.term != null)
					throw new GDataParseException(String.format(
							AtomParser.DUPLICATE_ATTRIBUTE, TERM_ATTRIBUTE));
				this.term = aValue;
			} else if (localPart.equals(LABLE_ATTRIBUTE)) {
				if (this.label != null)
					throw new GDataParseException(String.format(
							AtomParser.DUPLICATE_ATTRIBUTE, LABLE_ATTRIBUTE));
				this.label = aValue;
			} else if (localPart.equals(SCHEME_ATTRIBUTE)) {
				if (this.scheme != null)
					throw new GDataParseException(String.format(
							AtomParser.DUPLICATE_ATTRIBUTE, SCHEME_ATTRIBUTE));
				this.scheme = aValue;
			} else {
				super.processAttribute(aQName, aValue);
			}

		} else {
			super.processAttribute(aQName, aValue);
		}

	}

	/**
	 * @see org.apache.lucene.gdata.gom.core.AbstractGOMElement#processEndElement()
	 */
	@Override
	public void processEndElement() {
		/*
		 * ATOM defines "undefinedContent" for this. GData defines this as no
		 * content containing element
		 */
		if (this.term == null)
			throw new GDataParseException(String.format(
					AtomParser.MISSING_ELEMENT_ATTRIBUTE, this.localName,
					TERM_ATTRIBUTE));
		if (this.scheme != null) {
			try {
				AtomParserUtils.getAbsolutAtomURI(this.xmlBase, this.scheme);
			} catch (URISyntaxException e) {
				throw new GDataParseException(String.format(
						AtomParser.INVALID_ATTRIBUTE, this.localName
								+ " attribute " + GOMCategory.SCHEME_ATTRIBUTE,
						"absolute uri"), e);
			}
		}
	}

	/**
	 * @see org.apache.lucene.gdata.gom.GOMElement#writeAtomOutput(org.apache.lucene.gdata.gom.writer.GOMStaxWriter)
	 */
	public void writeAtomOutput(GOMOutputWriter aStreamWriter)
			throws XMLStreamException {
		if (aStreamWriter == null)
			throw new NullPointerException("StreamWriter is null");
		List<GOMAttribute> list = new LinkedList<GOMAttribute>();
		/*
		 * term attribute is requiered for a category. attribute term { text },
		 */
		list.add(GOMUtils.buildDefaultNamespaceAttribute(this.term,
				TERM_ATTRIBUTE));
		if (this.scheme != null)
			list.add(GOMUtils.buildDefaultNamespaceAttribute(this.scheme,
					SCHEME_ATTRIBUTE));
		if (this.label != null)
			list.add(GOMUtils.buildDefaultNamespaceAttribute(this.label,
					LABLE_ATTRIBUTE));

		if (this.xmlLang != null)
			list.add(GOMUtils
					.buildXMLNamespaceAttribute(this.xmlLang, XML_LANG));
		aStreamWriter.writeSimpleXMLElement(this.localName, list, null);

	}

	/**
	 * @see org.apache.lucene.gdata.gom.GOMElement#writeRssOutput(org.apache.lucene.gdata.gom.writer.GOMStaxWriter)
	 */
	public void writeRssOutput(GOMOutputWriter aStreamWriter)
			throws XMLStreamException {
		if (aStreamWriter == null)
			throw new NullPointerException("StreamWriter is null");
		List<GOMAttribute> list = getXmlNamespaceAttributes();
		/*
		 * build this domain attr. even if scheme is null or empty
		 */
		list.add(GOMUtils.buildDefaultNamespaceAttribute(this.scheme, DOMAIN));

		aStreamWriter.writeSimpleXMLElement(this.localName, list, this.term);

	}

}
