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
package org.apache.lucene.gdata.gom;

import javax.xml.stream.XMLStreamException;

import org.apache.lucene.gdata.gom.core.AtomParser;
import org.apache.lucene.gdata.gom.writer.GOMOutputWriter;

/**
 * Abstract interface which should be assignable from all classes representing
 * xml elements within the GData Object Model.
 * 
 * @author Simon Willnauer
 * 
 */
public abstract interface GOMElement extends GOMXmlEntity, AtomParser {
	/**
	 * <code>xml:lang</code> attribute localpart
	 */
	public static final String XML_LANG = "lang";

	/**
	 * <code>xml:base</code> attribute localpart
	 */
	public static final String XML_BASE = "base";

	/**
	 * 
	 * @return the xml:base attribute value
	 */
	public abstract String getXmlBase();

	/**
	 * 
	 * @return the xml:lang attribute value
	 */
	public abstract String getXmlLang();

	/**
	 * Generates the xml element represented by this class in the ATOM 1.0
	 * formate.
	 * 
	 * @param aStreamWriter -
	 *            the {@link GOMOutputWriter} implementation to write the output
	 * @throws XMLStreamException -
	 *             if the {@link GOMOutputWriter} throws an exception
	 */
	public abstract void writeAtomOutput(final GOMOutputWriter aStreamWriter)
			throws XMLStreamException;

	/**
	 * Generates the xml element represented by this class in the RSS 2.0
	 * formate.
	 * 
	 * @param aStreamWriter -
	 *            the {@link GOMOutputWriter} implementation to write the output
	 * @throws XMLStreamException -
	 *             if the {@link GOMOutputWriter} throws an exception
	 */
	public abstract void writeRssOutput(final GOMOutputWriter aStreamWriter)
			throws XMLStreamException;

	/**
	 * Generates the xml element represented by this class in the RSS 2.0
	 * formate using the parameter rssName as the element local name
	 * 
	 * @param rssName -
	 *            the local name to render the element
	 * @param aStreamWriter -
	 *            the {@link GOMOutputWriter} implementation to write the output
	 * @throws XMLStreamException -
	 *             if the {@link GOMOutputWriter} throws an exception
	 */
	public abstract void writeRssOutput(final GOMOutputWriter aStreamWriter,
			String rssName) throws XMLStreamException;

}
