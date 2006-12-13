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

import java.util.Date;

import javax.xml.stream.XMLStreamException;

import org.apache.lucene.gdata.gom.GOMDateConstruct;
import org.apache.lucene.gdata.gom.core.utils.GOMUtils;
import org.apache.lucene.gdata.gom.writer.GOMOutputWriter;

/**
 * A Date construct is an element whose content MUST conform to the "date-time"
 * production in [RFC3339]. In addition, an uppercase "T" character MUST be used
 * to separate date and time, and an uppercase "Z" character MUST be present in
 * the absence of a numeric time zone offset.
 * 
 * @author Simon Willnauer
 */
public abstract class GOMDateConstructImpl extends AbstractGOMElement implements
		GOMDateConstruct {
	protected long date;

	/*
	 * save the rfcString to skip the building while rendering the element
	 */
	protected String rfc3339String;

	/**
	 * @see org.apache.lucene.gdata.gom.GOMDateConstruct#setDate(java.util.Date)
	 */
	public void setDate(Date aDate) {
		if (aDate == null)
			return;
		this.date = aDate.getTime();
		this.rfc3339String = GOMUtils.buildRfc3339DateFormat(this.date);
	}

	/**
	 * @see org.apache.lucene.gdata.gom.GOMDateConstruct#getDate()
	 */
	public Date getDate() {
		return new Date(this.date);

	}

	/**
	 * @see org.apache.lucene.gdata.gom.core.AbstractGOMElement#processElementValue(java.lang.String)
	 */
	@Override
	public void processElementValue(String aValue) {
		if (aValue == null)
			throw new IllegalArgumentException("element value must not be null");
		this.date = GOMUtils.parseRfc3339DateFormat(aValue);
		this.rfc3339String = aValue;

	}

	/**
	 * @see org.apache.lucene.gdata.gom.core.AbstractGOMElement#processEndElement()
	 */
	@Override
	public void processEndElement() {
		if (this.rfc3339String == null)
			throw new GDataParseException(String.format(
					MISSING_ELEMENT_VALUE, this.localName,
					"RFC3339 Date Time"));

	}

	/**
	 * @see org.apache.lucene.gdata.gom.GOMElement#writeAtomOutput(org.apache.lucene.gdata.gom.writer.GOMStaxWriter)
	 */
	public void writeAtomOutput(GOMOutputWriter aStreamWriter)
			throws XMLStreamException {
		if (this.rfc3339String == null)
			this.rfc3339String = GOMUtils
					.buildRfc3339DateFormat(this.date == 0 ? System
							.currentTimeMillis() : this.date);
		aStreamWriter.writeSimpleXMLElement(this.qname,
				getXmlNamespaceAttributes(), this.rfc3339String);

	}

}
