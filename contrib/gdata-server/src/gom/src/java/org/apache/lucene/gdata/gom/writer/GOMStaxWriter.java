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
package org.apache.lucene.gdata.gom.writer;

import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.xml.namespace.QName;
import javax.xml.stream.FactoryConfigurationError;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

import org.apache.lucene.gdata.gom.GOMAttribute;
import org.apache.lucene.gdata.gom.GOMNamespace;

/**
 * @author Simon Willnauer
 * 
 */
public class GOMStaxWriter implements GOMOutputWriter {
	private static final String STAX_OUTPUTFACTORY_KEY = "org.apache.lucene.gdata.gom.writer.GOMXmlOutputFactory";

	private final Set<GOMNamespace> namespaceSet = new HashSet<GOMNamespace>(16);

	private final XMLStreamWriter writer;
	static {
		/*
		 * set the system property to make sure the factory will be found
		 */
		String property = System.getProperty(STAX_OUTPUTFACTORY_KEY);
		if (property == null)
			System.setProperty(STAX_OUTPUTFACTORY_KEY, STAX_OUTPUTFACTORY_KEY);
	}

	/**
	 * @param aOutputStream
	 * @param encoding
	 * @throws UnsupportedEncodingException
	 * @throws XMLStreamException
	 * @throws FactoryConfigurationError
	 */
	public GOMStaxWriter(final OutputStream aOutputStream, String encoding)
			throws UnsupportedEncodingException, XMLStreamException,
			FactoryConfigurationError {
		this(new OutputStreamWriter(aOutputStream, encoding));
	}

	/**
	 * Class constructor
	 * 
	 * 
	 * @param aOutputStream -
	 *            a output stream to write the xml stream to.
	 * @throws XMLStreamException -
	 *             if a write exception occurs
	 * @throws FactoryConfigurationError -
	 *             if XMLOutputFactory throws an exception
	 * 
	 */
	public GOMStaxWriter(final OutputStream aOutputStream)
			throws XMLStreamException, FactoryConfigurationError {
		this(new OutputStreamWriter(aOutputStream));
	}

	/**
	 * Class constructor
	 * 
	 * @param aWriter -
	 *            a writer to write the xml stream to.
	 * @throws XMLStreamException -
	 *             if a write exception occurs
	 * @throws FactoryConfigurationError -
	 *             if XMLOutputFactory throws an exception
	 */
	public GOMStaxWriter(final Writer aWriter) throws XMLStreamException,
			FactoryConfigurationError {
		if (aWriter == null)
			throw new IllegalArgumentException("Given writer must not be null");

		this.writer = XMLOutputFactory.newInstance(STAX_OUTPUTFACTORY_KEY,
				GOMStaxWriter.class.getClassLoader()).createXMLStreamWriter(
				aWriter);

	}

	/**
	 * @see org.apache.lucene.gdata.gom.writer.GOMOutputWriter#writeSimpleXMLElement(javax.xml.namespace.QName,
	 *      java.util.List, java.lang.String)
	 */
	public void writeSimpleXMLElement(QName aName, List<GOMAttribute> aList,
			String aValue) throws XMLStreamException {
		writeStartElement(aName, aList);
		writeContent(aValue);
		this.writer.writeEndElement();
	}

	/**
	 * @see org.apache.lucene.gdata.gom.writer.GOMOutputWriter#writeSimpleXMLElement(javax.xml.namespace.QName,
	 *      java.lang.String, org.apache.lucene.gdata.gom.GOMAttribute)
	 */
	public void writeSimpleXMLElement(QName aName, String aValue,
			GOMAttribute aAttribute) throws XMLStreamException {
		List<GOMAttribute> list = null;
		if (aAttribute != null) {
			list = new ArrayList<GOMAttribute>(1);
			list.add(aAttribute);
		}
		writeSimpleXMLElement(aName, list, aValue);
	}

	/**
	 * @see org.apache.lucene.gdata.gom.writer.GOMOutputWriter#writeSimpleXMLElement(java.lang.String,
	 *      java.lang.String, org.apache.lucene.gdata.gom.GOMAttribute)
	 */
	public void writeSimpleXMLElement(String aName, String aValue,
			GOMAttribute aAttribute) throws XMLStreamException {
		List<GOMAttribute> list = null;
		if (aAttribute != null) {
			list = new ArrayList<GOMAttribute>(1);
			list.add(aAttribute);
		}
		writeSimpleXMLElement(aName, list, aValue);
	}

	/**
	 * @see org.apache.lucene.gdata.gom.writer.GOMOutputWriter#writeSimpleXMLElement(java.lang.String,
	 *      java.util.List, java.lang.String)
	 */
	public void writeSimpleXMLElement(String aName, List<GOMAttribute> aList,
			String aValue) throws XMLStreamException {
		writeStartElement(aName, aList);
		writeContent(aValue);
		this.writer.writeEndElement();
	}

	/**
	 * @see org.apache.lucene.gdata.gom.writer.GOMOutputWriter#writeAttribute(org.apache.lucene.gdata.gom.GOMAttribute)
	 */
	public void writeAttribute(GOMAttribute attribute)
			throws XMLStreamException {
		if (attribute.hasDefaultNamespace())
			this.writer.writeAttribute(attribute.getLocalName(), attribute
					.getTextValue());
		else
			this.writer.writeAttribute(attribute.getQname().getPrefix(),
					attribute.getQname().getNamespaceURI(), attribute
							.getLocalName(), attribute.getTextValue());
	}

	/**
	 * @see org.apache.lucene.gdata.gom.writer.GOMOutputWriter#writeStartElement(java.lang.String,
	 *      java.util.List)
	 */
	public void writeStartElement(String aName, List<GOMAttribute> aList)
			throws XMLStreamException {
		this.writer.writeStartElement(aName);
		if (aList != null)
			for (GOMAttribute attribute : aList) {
				writeAttribute(attribute);
			}
	}

	/**
	 * @see org.apache.lucene.gdata.gom.writer.GOMOutputWriter#writeStartElement(java.lang.String,
	 *      org.apache.lucene.gdata.gom.GOMAttribute)
	 */
	public void writeStartElement(String aName, GOMAttribute aAttribute)
			throws XMLStreamException {
		this.writer.writeStartElement(aName);
		writeAttribute(aAttribute);
	}

	/**
	 * @see org.apache.lucene.gdata.gom.writer.GOMOutputWriter#writeStartElement(java.lang.String)
	 */
	public void writeStartElement(String aName) throws XMLStreamException {
		this.writer.writeStartElement(aName);
	}

	/**
	 * @see org.apache.lucene.gdata.gom.writer.GOMOutputWriter#writeStartElement(javax.xml.namespace.QName,
	 *      java.util.List)
	 */
	public void writeStartElement(QName aName, List<GOMAttribute> aList)
			throws XMLStreamException {
		this.writer.writeStartElement(aName.getPrefix(), aName.getLocalPart(),
				aName.getNamespaceURI());
		if (aList != null)
			for (GOMAttribute attribute : aList) {
				writeAttribute(attribute);
			}
	}

	/**
	 * @see org.apache.lucene.gdata.gom.writer.GOMOutputWriter#writeEndElement()
	 */
	public void writeEndElement() throws XMLStreamException {
		this.writer.writeEndElement();
	}

	/**
	 * @see org.apache.lucene.gdata.gom.writer.GOMOutputWriter#writeContent(java.lang.String)
	 */
	public void writeContent(String aContent) throws XMLStreamException {
		if (aContent != null) {
			char[] cs = aContent.toCharArray();
			this.writer.writeCharacters(cs, 0, cs.length);
		}
	}

	/**
	 * @see org.apache.lucene.gdata.gom.writer.GOMOutputWriter#writeContentUnescaped(java.lang.String)
	 */
	public void writeContentUnescaped(String aContent)
			throws XMLStreamException {
		if (aContent != null)
			this.writer.writeCharacters(aContent);
	}

	/**
	 * @see org.apache.lucene.gdata.gom.writer.GOMOutputWriter#writeDefaultNamespace(java.lang.String)
	 */
	public void writeDefaultNamespace(String aNsUri) throws XMLStreamException {
		this.writer.writeDefaultNamespace(aNsUri);

	}

	/**
	 * @see org.apache.lucene.gdata.gom.writer.GOMOutputWriter#writeDefaultNamespace(org.apache.lucene.gdata.gom.GOMNamespace)
	 */
	public void writeDefaultNamespace(GOMNamespace aNameSpace)
			throws XMLStreamException {
		if (aNameSpace != null)
			writeDefaultNamespace(aNameSpace.getNamespaceUri());
	}

	/**
	 * @see org.apache.lucene.gdata.gom.writer.GOMOutputWriter#writeNamespace(org.apache.lucene.gdata.gom.GOMNamespace)
	 */
	public void writeNamespace(GOMNamespace aNameSpace)
			throws XMLStreamException {
		if (aNameSpace == null)
			return;
		if (this.namespaceSet.contains(aNameSpace))
			return;
		this.namespaceSet.add(aNameSpace);
		this.writer.writeNamespace(aNameSpace.getNamespacePrefix(), aNameSpace
				.getNamespaceUri());
	}

	/**
	 * @see org.apache.lucene.gdata.gom.writer.GOMOutputWriter#writeStartDocument(java.lang.String,
	 *      java.lang.String)
	 */
	public void writeStartDocument(String aString, String aString2)
			throws XMLStreamException {
		this.writer.writeStartDocument(aString, aString2);
	}

	/**
	 * @see org.apache.lucene.gdata.gom.writer.GOMOutputWriter#writeEndDocument()
	 */
	public void writeEndDocument() throws XMLStreamException {
		this.writer.writeEndDocument();
	}

	/**
	 * @see org.apache.lucene.gdata.gom.writer.GOMOutputWriter#flush()
	 */
	public void flush() throws XMLStreamException {
		this.writer.flush();
	}

	/**
	 * @see org.apache.lucene.gdata.gom.writer.GOMOutputWriter#close()
	 */
	public void close() throws XMLStreamException {
		this.writer.close();
	}

}
