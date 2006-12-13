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

import java.io.StringWriter;

import javax.xml.namespace.QName;
import javax.xml.stream.FactoryConfigurationError;
import javax.xml.stream.XMLStreamException;

import org.apache.lucene.gdata.gom.writer.GOMStaxWriter;
import org.apache.lucene.gdata.gom.writer.GOMOutputWriter;

import junit.framework.TestCase;

/**
 * @author Simon Willnauer
 * 
 */
public class GOMDocumentImplTest extends TestCase {

	/*
	 * Test method for
	 * 'org.apache.lucene.gdata.gom.core.GOMDocumentImpl.writeAtomOutput(GOMWriter)'
	 */
	public void testWriteAtomOutput() throws XMLStreamException,
			FactoryConfigurationError {
		GOMDocumentImpl<ArbitraryGOMXml> impl = new GOMDocumentImpl<ArbitraryGOMXml>();
		impl.setRootElement(new ArbitraryGOMXml(new QName("test")));
		{
			StringWriter strWriter = new StringWriter();
			GOMOutputWriter writer = new GOMStaxWriter(strWriter);
			impl.writeAtomOutput(writer);
			assertEquals("<?xml version='1.0' encoding='UTF-8'?><test/>",
					strWriter.toString());
		}
		impl.setRootElement(null);
		{
			StringWriter strWriter = new StringWriter();
			GOMOutputWriter writer = new GOMStaxWriter(strWriter);
			impl.writeAtomOutput(writer);
			assertEquals("<?xml version='1.0' encoding='UTF-8'?>", strWriter
					.toString());
		}

		impl.setVersion("2.0");
		impl.setCharacterEncoding("ISO-8859-1");
		{
			StringWriter strWriter = new StringWriter();
			GOMOutputWriter writer = new GOMStaxWriter(strWriter);
			impl.writeAtomOutput(writer);
			assertEquals("<?xml version='2.0' encoding='ISO-8859-1'?>",
					strWriter.toString());
		}
	}

	/*
	 * Test method for
	 * 'org.apache.lucene.gdata.gom.core.GOMDocumentImpl.writeRssOutput(GOMWriter)'
	 */
	public void testWriteRssOutput() throws XMLStreamException,
			FactoryConfigurationError {
		GOMDocumentImpl<ArbitraryGOMXml> impl = new GOMDocumentImpl<ArbitraryGOMXml>();
		impl.setRootElement(new ArbitraryGOMXml(new QName("test")));
		{
			StringWriter strWriter = new StringWriter();
			GOMOutputWriter writer = new GOMStaxWriter(strWriter);
			impl.writeRssOutput(writer);
			assertEquals("<?xml version='1.0' encoding='UTF-8'?><test/>",
					strWriter.toString());
		}
		impl.setRootElement(null);
		{
			StringWriter strWriter = new StringWriter();
			GOMOutputWriter writer = new GOMStaxWriter(strWriter);
			impl.writeRssOutput(writer);
			assertEquals("<?xml version='1.0' encoding='UTF-8'?>", strWriter
					.toString());
		}

		impl.setVersion("2.0");
		impl.setCharacterEncoding("ISO-8859-1");
		{
			StringWriter strWriter = new StringWriter();
			GOMOutputWriter writer = new GOMStaxWriter(strWriter);
			impl.writeRssOutput(writer);
			assertEquals("<?xml version='2.0' encoding='ISO-8859-1'?>",
					strWriter.toString());
		}
	}

}
