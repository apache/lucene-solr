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
import org.apache.lucene.gdata.gom.writer.GOMOutputWriter;
//TODO add java doc
/**
 * 
 * @author Simon Willnauer
 * 
 */
public class ArbitraryGOMXml extends AbstractGOMElement {
	private List<GOMElement> children = new LinkedList<GOMElement>();

	private List<GOMAttribute> attributes = new LinkedList<GOMAttribute>();

	/**
	 * this method will never return <code>null</code>
	 * 
	 * @return Returns the attributes of this xml element.
	 */
	public List<GOMAttribute> getAttributes() {
		return this.attributes;
	}

	/**
	 * this method will never return <code>null</code>
	 * 
	 * @return - the child elements of this xml element
	 */
	public List<GOMElement> getChildren() {
		return this.children;
	}

	/**
	 * Class constructor
	 * 
	 * @param qname -
	 *            the elements qname
	 */
	public ArbitraryGOMXml(QName qname) {
		if (qname == null)
			throw new IllegalArgumentException("QName must not be null");

		this.qname = qname;
		this.localName = qname.getLocalPart();
	}

	/**
	 * {@inheritDoc} 
	 * 
	 * @see org.apache.lucene.gdata.gom.core.AbstractGOMElement#getChildParser(javax.xml.namespace.QName)
	 */
	@Override
	public AtomParser getChildParser(QName aName) {
		if (aName == null)
			throw new GDataParseException("QName must not be null");
		/*
		 * either a text value or a child
		 */
		if (this.textValue != null)
			throw new GDataParseException(String.format(
					AtomParser.UNEXPECTED_ELEMENT_CHILD, this.localName));
		GOMElement element = new ArbitraryGOMXml(aName);
		this.children.add(element);
		return element;
	}

	/**
	 * {@inheritDoc} 
	 * 
	 * @see org.apache.lucene.gdata.gom.core.AbstractGOMElement#processAttribute(javax.xml.namespace.QName,
	 *      java.lang.String)
	 */
	@Override
	public void processAttribute(QName aQName, String aValue) {
		if (aQName == null)
			throw new GDataParseException("QName must not be null");
		GOMAttributeImpl impl = new GOMAttributeImpl(aQName.getNamespaceURI(),
				aQName.getPrefix(), aQName.getLocalPart(), aValue);
		this.attributes.add(impl);

	}

	/**
	 * {@inheritDoc}
	 * 
	 * @see org.apache.lucene.gdata.gom.core.AbstractGOMElement#processElementValue(java.lang.String)
	 */
	@Override
	public void processElementValue(String aValue) {
		if (aValue == null)
			throw new GDataParseException("Element value must not be null");
		/*
		 * either a text value or a child
		 */
		if (this.children.size() > 0)
			throw new GDataParseException(String.format(
					AtomParser.UNEXPECTED_ELEMENT_VALUE, this.localName));
		if (this.textValue != null)
			throw new GDataParseException(String.format(
					AtomParser.UNEXPECTED_ELEMENT_VALUE, this.localName));
		this.textValue = aValue;

	}

	/**
	 * {@inheritDoc}
	 * 
	 * @see org.apache.lucene.gdata.gom.GOMElement#writeAtomOutput(org.apache.lucene.gdata.gom.writer.GOMStaxWriter)
	 */
	public void writeAtomOutput(GOMOutputWriter aStreamWriter)
			throws XMLStreamException {
		if (aStreamWriter == null)
			throw new NullPointerException("StreamWriter is null");
		aStreamWriter.writeStartElement(this.qname, this.attributes);
		if (this.textValue == null) {
			for (GOMElement element : this.children) {
				element.writeAtomOutput(aStreamWriter);
			}
		} else {
			aStreamWriter.writeContent(this.textValue);
		}
		aStreamWriter.writeEndElement();

	}

	/**
	 * {@inheritDoc}
	 * 
	 * @see org.apache.lucene.gdata.gom.GOMElement#writeRssOutput(org.apache.lucene.gdata.gom.writer.GOMStaxWriter)
	 */
	public void writeRssOutput(GOMOutputWriter aStreamWriter)
			throws XMLStreamException {
		// delegate it by default
		this.writeAtomOutput(aStreamWriter);

	}

}
