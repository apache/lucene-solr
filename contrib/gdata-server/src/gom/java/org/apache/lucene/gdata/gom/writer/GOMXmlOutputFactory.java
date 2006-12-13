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

import java.io.Writer;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

import com.bea.xml.stream.ConfigurationContextBase;
import com.bea.xml.stream.XMLOutputFactoryBase;

/**
 * @author Simon Willnauer
 * 
 */
public class GOMXmlOutputFactory extends XMLOutputFactoryBase {
	protected ConfigurationContextBase config = new ConfigurationContextBase();

	/**
	 * @see com.bea.xml.stream.XMLOutputFactoryBase#createXMLStreamWriter(java.io.OutputStream,
	 *      java.lang.String)
	 */
	@Override
	public XMLStreamWriter createXMLStreamWriter(Writer aWriter)
			throws XMLStreamException {
		GOMXmlWriter b = new GOMXmlWriter(aWriter);
		b.setConfigurationContext(config);
		return b;
	}

	/**
	 * @see com.bea.xml.stream.XMLOutputFactoryBase#setProperty(java.lang.String,
	 *      java.lang.Object)
	 */
	public void setProperty(java.lang.String name, Object value) {
		config.setProperty(name, value);
	}

	/**
	 * @see com.bea.xml.stream.XMLOutputFactoryBase#getProperty(java.lang.String)
	 */
	public Object getProperty(java.lang.String name) {
		return config.getProperty(name);
	}

	/**
	 * @see com.bea.xml.stream.XMLOutputFactoryBase#isPrefixDefaulting()
	 */
	public boolean isPrefixDefaulting() {
		return config.isPrefixDefaulting();
	}

	/**
	 * @see com.bea.xml.stream.XMLOutputFactoryBase#setPrefixDefaulting(boolean)
	 */
	public void setPrefixDefaulting(boolean value) {
		config.setPrefixDefaulting(value);
	}

	/**
	 * @see com.bea.xml.stream.XMLOutputFactoryBase#isPropertySupported(java.lang.String)
	 */
	public boolean isPropertySupported(String name) {
		return config.isPropertySupported(name);
	}

}
