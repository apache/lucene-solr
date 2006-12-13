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

import org.apache.lucene.gdata.gom.GOMNamespace;
import org.apache.lucene.gdata.gom.writer.GOMOutputWriter;

/**
 * @author Simon Willnauer
 * 
 */
public class SimpleGOMElementImpl extends AbstractGOMElement {
	public static final String ELEMENT_OS_ITEMS_PER_PAGE = "itemsPerPage";

	public static final String ELEMENT_OS_START_INDEX = "startIndex";

	private SimpleValidator validator;

	/**
	 * 
	 */
	public SimpleGOMElementImpl(String aLocalName, GOMNamespace aNamespace) {
		super();
		if (aLocalName == null)
			throw new IllegalArgumentException("localname must not be null");
		if (aNamespace == null)
			throw new IllegalArgumentException("Namespace must not be null");
		this.localName = aLocalName;
		this.qname = new QName(aNamespace.getNamespaceUri(), this.localName,
				aNamespace.getNamespacePrefix());
	}

	SimpleGOMElementImpl() {
		// for subclasses
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
		if (this.validator != null)
			this.validator.validate(this.textValue);

	}

	/**
	 * @see org.apache.lucene.gdata.gom.GOMElement#writeAtomOutput(org.apache.lucene.gdata.gom.writer.GOMStaxWriter)
	 */
	public void writeAtomOutput(GOMOutputWriter aStreamWriter)
			throws XMLStreamException {
		aStreamWriter.writeSimpleXMLElement(this.qname,
				getXmlNamespaceAttributes(), this.textValue);

	}

	/**
	 * @see org.apache.lucene.gdata.gom.GOMElement#writeRssOutput(org.apache.lucene.gdata.gom.writer.GOMStaxWriter)
	 */
	public void writeRssOutput(GOMOutputWriter aStreamWriter)
			throws XMLStreamException {
		writeAtomOutput(aStreamWriter);

	}

	protected abstract static class SimpleValidator {
		String localName;

		protected SimpleValidator(String aLocalName) {
			this.localName = aLocalName;
		}

		/**
		 * @param aTextValue
		 */
		protected void validate(String aTextValue) {
			if (aTextValue == null)
				throw new GDataParseException(String.format(
						AtomParser.MISSING_ELEMENT_VALUE_PLAIN,
						this.localName));
		}

	}

	/**
	 * @param aValidator
	 *            The validator to set.
	 */
	public void setValidator(SimpleValidator aValidator) {
		validator = aValidator;
	}

}
