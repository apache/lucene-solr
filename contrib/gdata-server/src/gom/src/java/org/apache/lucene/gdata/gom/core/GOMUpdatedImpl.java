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
import org.apache.lucene.gdata.gom.GOMUpdated;
import org.apache.lucene.gdata.gom.core.utils.GOMUtils;
import org.apache.lucene.gdata.gom.writer.GOMOutputWriter;

/**
 * @author Simon Willnauer
 * 
 */
public class GOMUpdatedImpl extends GOMDateConstructImpl implements GOMUpdated {
	protected static final QName ATOM_QNAME = new QName(
			GOMNamespace.ATOM_NS_URI, LOCALNAME, GOMNamespace.ATOM_NS_PREFIX);

	/**
	 * 
	 */
	public GOMUpdatedImpl() {
		super();
		this.localName = LOCALNAME;
		this.qname = new QName(GOMNamespace.ATOM_NS_URI, this.localName);
	}

	/**
	 * @see org.apache.lucene.gdata.gom.GOMElement#writeRssOutput(org.apache.lucene.gdata.gom.writer.GOMStaxWriter)
	 */
	public void writeRssOutput(GOMOutputWriter aStreamWriter)
			throws XMLStreamException {
		if (this.rfc3339String == null)
			this.rfc3339String = GOMUtils
					.buildRfc3339DateFormat(this.date == 0 ? System
							.currentTimeMillis() : this.date);
		aStreamWriter.writeSimpleXMLElement(ATOM_QNAME,
				getXmlNamespaceAttributes(), this.rfc3339String);

	}

}
