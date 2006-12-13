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

import java.util.List;

import javax.xml.namespace.QName;
import javax.xml.stream.XMLStreamException;

import org.apache.lucene.gdata.gom.GOMAttribute;
import org.apache.lucene.gdata.gom.GOMNamespace;

/**
 * @author Simon Willnauer
 * 
 */
public interface GOMOutputWriter {

	/**
	 * Writes a simple element with full namespace
	 * 
	 * @param aName -
	 *            element QName
	 * @param aList -
	 *            attribute list
	 * @param aValue -
	 *            character value
	 * @throws XMLStreamException -
	 *             if a write exception occurs
	 */
	public abstract void writeSimpleXMLElement(QName aName,
			List<GOMAttribute> aList, String aValue) throws XMLStreamException;

	/**
	 * Writes a simple element with full namespace
	 * 
	 * @param aName -
	 *            element QName
	 * @param aAttribute -
	 *            attribute
	 * @param aValue -
	 *            character value
	 * @throws XMLStreamException -
	 *             if a write exception occurs
	 */
	public abstract void writeSimpleXMLElement(QName aName, String aValue,
			GOMAttribute aAttribute) throws XMLStreamException;

	/**
	 * Writes a simple element with full namespace
	 * 
	 * @param aName -
	 *            the local name of the element
	 * @param aAttribute -
	 *            attribute
	 * @param aValue -
	 *            character value
	 * @throws XMLStreamException -
	 *             if a write exception occurs
	 */
	public abstract void writeSimpleXMLElement(String aName, String aValue,
			GOMAttribute aAttribute) throws XMLStreamException;

	/**
	 * Writes a simple element with default namespace
	 * 
	 * @param aName -
	 *            elements name
	 * @param aList -
	 *            attribute list
	 * @param aValue -
	 *            character value
	 * @throws XMLStreamException -
	 *             if a write exception occurs
	 */
	public abstract void writeSimpleXMLElement(String aName,
			List<GOMAttribute> aList, String aValue) throws XMLStreamException;

	/**
	 * writes a attribute with the attribute namespace Uri
	 * 
	 * @param attribute -
	 *            the attribute
	 * @throws XMLStreamException -
	 *             if a write exception occurs
	 */
	public abstract void writeAttribute(GOMAttribute attribute)
			throws XMLStreamException;

	/**
	 * @param aName
	 * @param aList
	 * @throws XMLStreamException
	 */
	public abstract void writeStartElement(String aName,
			List<GOMAttribute> aList) throws XMLStreamException;

	/**
	 * @param aName
	 * @param aAttribute
	 * @throws XMLStreamException
	 */
	public abstract void writeStartElement(String aName, GOMAttribute aAttribute)
			throws XMLStreamException;

	/**
	 * @param aName
	 * @throws XMLStreamException
	 */
	public abstract void writeStartElement(String aName)
			throws XMLStreamException;

	/**
	 * @param aName
	 * @param aList
	 * @throws XMLStreamException
	 */
	public abstract void writeStartElement(QName aName, List<GOMAttribute> aList)
			throws XMLStreamException;

	/**
	 * @throws XMLStreamException
	 */
	public abstract void writeEndElement() throws XMLStreamException;

	/**
	 * @param aContent
	 * @throws XMLStreamException
	 */
	public abstract void writeContent(String aContent)
			throws XMLStreamException;

	/**
	 * @param aContent
	 * @throws XMLStreamException
	 */
	public abstract void writeContentUnescaped(String aContent)
			throws XMLStreamException;

	/**
	 * @param aNameSpace
	 * @throws XMLStreamException
	 */
	public abstract void writeDefaultNamespace(GOMNamespace aNameSpace)
			throws XMLStreamException;

	/**
	 * @param aNameSpace
	 * @throws XMLStreamException
	 */
	public abstract void writeNamespace(GOMNamespace aNameSpace)
			throws XMLStreamException;

	/**
	 * @param aString
	 * @param aString2
	 * @throws XMLStreamException
	 */
	public abstract void writeStartDocument(String aString, String aString2)
			throws XMLStreamException;

	/**
	 * Writes a end element tag according to the start element tag
	 * 
	 * @throws XMLStreamException -
	 *             if no start tag has been written or the element stack points
	 *             to a different element
	 */
	public abstract void writeEndDocument() throws XMLStreamException;

	/**
	 * Flush the GOMWriter
	 * 
	 * @throws XMLStreamException
	 */
	public abstract void flush() throws XMLStreamException;

	/**
	 * Closes the GOM Writer
	 * 
	 * @throws XMLStreamException
	 */
	public abstract void close() throws XMLStreamException;

}