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

import javax.xml.namespace.QName;
import javax.xml.stream.XMLStreamException;

import org.apache.lucene.gdata.gom.GOMId;
import org.apache.lucene.gdata.gom.GOMNamespace;
import org.apache.lucene.gdata.gom.writer.GOMOutputWriter;

/**
 * @author Simon Willnauer
 * 
 */
class GOMIdImpl extends AbstractGOMElement implements GOMId {

	protected static final QName ATOM_QNAME = new QName(
			GOMNamespace.ATOM_NS_URI, LOCALNAME, GOMNamespace.ATOM_NS_PREFIX);

	GOMIdImpl() {
		this.localName = LOCALNAME;
		this.qname = new QName(GOMNamespace.ATOM_NS_URI, this.localName);
	}

	/**
	 * @see org.apache.lucene.gdata.gom.core.AbstractGOMElement#getLocalName()
	 */
	@Override
	public String getLocalName() {
		return this.localName;
	}

	/**
	 * @see org.apache.lucene.gdata.gom.core.AtomParser#processElementValue(java.lang.String)
	 */
	public void processElementValue(String aValue) {
		this.textValue = aValue;

	}

	/**
	 * @see org.apache.lucene.gdata.gom.core.AtomParser#processEndElement()
	 */
	public void processEndElement() {
		if (this.textValue == null)
			throw new GDataParseException(
					"Element id must have a unique id value -- is null");

	}

	/**
	 * @see org.apache.lucene.gdata.gom.GOMElement#writeAtomOutput(org.apache.lucene.gdata.gom.writer.GOMStaxWriter)
	 */
	public void writeAtomOutput(GOMOutputWriter aStreamWriter)
			throws XMLStreamException {
		if (aStreamWriter == null)
			throw new GDataParseException("GOMWriter must not be null");
		aStreamWriter.writeSimpleXMLElement(LOCALNAME,
				getXmlNamespaceAttributes(), this.textValue);

	}

	/**
	 * @see org.apache.lucene.gdata.gom.GOMElement#writeRssOutput(org.apache.lucene.gdata.gom.writer.GOMStaxWriter)
	 */
	public void writeRssOutput(GOMOutputWriter aStreamWriter)
			throws XMLStreamException {
		if (aStreamWriter == null)
			throw new GDataParseException("GOMWriter must not be null");
		aStreamWriter.writeSimpleXMLElement(ATOM_QNAME,
				getXmlNamespaceAttributes(), this.textValue);

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
		aStreamWriter.writeSimpleXMLElement(aRssName,
				getXmlNamespaceAttributes(), this.textValue);
	}

}
