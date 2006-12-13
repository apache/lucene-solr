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

import javax.xml.stream.XMLStreamException;

import org.apache.lucene.gdata.gom.GOMDocument;
import org.apache.lucene.gdata.gom.GOMElement;
import org.apache.lucene.gdata.gom.writer.GOMOutputWriter;

/**
 * @author Simon Willnauer
 * @param <T>
 */
public class GOMDocumentImpl<T extends GOMElement> implements GOMDocument<T> {

	private static final String DEFAULT_ENCODING = "UTF-8";

	private static final String DEFAULT_VERSION = "1.0";

	private T root;

	private String version;

	private String charEncoding;

	/**
	 * 
	 */
	public GOMDocumentImpl() {
		super();
	}

	/**
	 * @see org.apache.lucene.gdata.gom.GOMDocument#setRootElement(org.apache.lucene.gdata.gom.GOMElement)
	 */
	public void setRootElement(T aRootElement) {
		this.root = aRootElement;

	}

	/**
	 * @see org.apache.lucene.gdata.gom.GOMDocument#getRootElement()
	 */
	public T getRootElement() {
		return this.root;
	}

	/**
	 * @see org.apache.lucene.gdata.gom.GOMDocument#setVersion(java.lang.String)
	 */
	public void setVersion(String aVersion) {
		this.version = aVersion;

	}

	/**
	 * @return the version
	 * @uml.property name="version"
	 */
	public String getVersion() {
		return this.version;
	}

	/**
	 * @see org.apache.lucene.gdata.gom.GOMDocument#getCharacterEncoding()
	 */
	public String getCharacterEncoding() {
		return this.charEncoding;
	}

	/**
	 * @see org.apache.lucene.gdata.gom.GOMDocument#setCharacterEncoding(java.lang.String)
	 */
	public void setCharacterEncoding(String aEncoding) {
		this.charEncoding = aEncoding;

	}

	/**
	 * @see org.apache.lucene.gdata.gom.GOMDocument#writeAtomOutput(org.apache.lucene.gdata.gom.writer.GOMStaxWriter)
	 */
	public void writeAtomOutput(GOMOutputWriter aStreamWriter)
			throws XMLStreamException {
		try {
			aStreamWriter.writeStartDocument(
					this.charEncoding == null ? DEFAULT_ENCODING
							: this.charEncoding,
					this.version == null ? DEFAULT_VERSION : this.version);
			if (this.root != null)
				this.root.writeAtomOutput(aStreamWriter);
			aStreamWriter.writeEndDocument();
			aStreamWriter.flush();
		} finally {
			aStreamWriter.close();
		}

	}

	/**
	 * @see org.apache.lucene.gdata.gom.GOMDocument#writeRssOutput(org.apache.lucene.gdata.gom.writer.GOMStaxWriter)
	 */
	public void writeRssOutput(GOMOutputWriter aStreamWriter)
			throws XMLStreamException {
		try {
			aStreamWriter.writeStartDocument(
					this.charEncoding == null ? DEFAULT_ENCODING
							: this.charEncoding,
					this.version == null ? DEFAULT_VERSION : this.version);
			if (this.root != null) {
				this.root.writeRssOutput(aStreamWriter);
			}
			aStreamWriter.writeEndDocument();
			aStreamWriter.flush();
		} finally {
			aStreamWriter.close();
		}

	}

}
