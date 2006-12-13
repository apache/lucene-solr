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

import org.apache.lucene.gdata.gom.writer.GOMOutputWriter;

/**
 * <p>
 * GOMDocument acts as a container for GOMElements to render the containing
 * GOMElement as a valid xml document. This class renderes the
 * 
 * <pre>
 *  &lt;?xml version=&quot;1.0&quot; encoding=&quot;UTF-8&quot;?&gt;
 * </pre>
 * 
 * header to the outputstream before the containing element will be rendered.
 * </p>
 * 
 * @author Simon Willnauer
 * 
 * @param <T>
 */
public interface GOMDocument<T extends GOMElement> {
	/**
	 * setter for the root element of the xml e.g GOMDocument
	 * 
	 * @param aRootElement -
	 *            the root element to set
	 */
	public abstract void setRootElement(T aRootElement);

	/**
	 * Getter for the root element of the xml e.g GOMDocument
	 * 
	 * @return - the root elmenent
	 */
	public abstract T getRootElement();

	/**
	 * Sets the xml version
	 * 
	 * @param aVersion -
	 *            the version string
	 */
	public abstract void setVersion(String aVersion);

	/**
	 * Gets the xml version
	 * 
	 * @return - the xml version string
	 */
	public abstract String getVersion();

	/**
	 * Gets the xml charset encoding
	 * 
	 * @return - the specified char encoding
	 */
	public abstract String getCharacterEncoding();

	/**
	 * Sets the xml charset encoding
	 * 
	 * @param aEncoding -
	 *            the charset encoding to set
	 */
	public abstract void setCharacterEncoding(String aEncoding);

	/**
	 * Generates a complete xml document starting with the header followed by
	 * the output of the specified root element in the ATOM 1.0 formate. 
	 * 
	 * @param aStreamWriter -
	 *            the {@link GOMOutputWriter} implementation to write the output
	 * @throws XMLStreamException -
	 *             if the {@link GOMOutputWriter} throws an exception
	 */
	public abstract void writeAtomOutput(final GOMOutputWriter aStreamWriter)
			throws XMLStreamException;

	/**
	 *
	 * Generates a complete xml document starting with the header followed by
	 * the output of the specified root element in the RSS 2.0 formate. 
	 * 
	 * @param aStreamWriter -
	 *            the {@link GOMOutputWriter} implementation to write the output
	 * @throws XMLStreamException -
	 *             if the {@link GOMOutputWriter} throws an exception
	 */
	public abstract void writeRssOutput(final GOMOutputWriter aStreamWriter)
			throws XMLStreamException;

}
