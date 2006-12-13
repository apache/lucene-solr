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
import java.util.Date;

import javax.xml.stream.FactoryConfigurationError;
import javax.xml.stream.XMLStreamException;

import junit.framework.TestCase;

import org.apache.lucene.gdata.gom.writer.GOMStaxWriter;
import org.apache.lucene.gdata.gom.writer.GOMOutputWriter;

/**
 * @author Simon Willnauer
 * 
 */
public class GOMDateConstructImplTest extends TestCase {
	private static final String DATE = "2003-12-13T18:30:02+02:00";

	private static final String DATE_RSS = "Sat, 13 Dec 2003 16:30:02 +0000";

	private static final String DATE1 = "2003-12-13T18:30:02.25Z";

	private GOMUpdatedImpl updateImpl;

	private GOMPublishedImpl publishImpl;

	protected void setUp() throws Exception {
		this.updateImpl = new GOMUpdatedImpl();
		this.publishImpl = new GOMPublishedImpl();
	}

	/*
	 * Test method for
	 * 'org.apache.lucene.gdata.gom.core.GOMDateConstructImpl.processElementValue(String)'
	 */
	public void testProcessElementValue() {
		try {
			this.updateImpl.processElementValue(null);
			fail("must not be null");
		} catch (IllegalArgumentException e) {
			// 
		}
		try {
			this.updateImpl.processElementValue("not a date");
			fail("illegal string");
		} catch (GDataParseException e) {
			// 
		}

		this.updateImpl.processElementValue(DATE);
		assertNotNull(this.updateImpl.getDate());
		this.updateImpl.processElementValue(DATE1);
		assertNotNull(this.updateImpl.getDate());

	}

	/*
	 * Test method for
	 * 'org.apache.lucene.gdata.gom.core.GOMDateConstructImpl.processEndElement()'
	 */
	public void testProcessEndElement() {
		try {
			this.updateImpl.processEndElement();
			fail("no element value");
		} catch (GDataParseException e) {
			// 
		}
		this.updateImpl.setDate(new Date());
		this.updateImpl.processEndElement();
	}

	/*
	 * Test method for
	 * 'org.apache.lucene.gdata.gom.core.GOMDateConstructImpl.writeAtomOutput(GOMWriter)'
	 */
	public void testWriteAtomOutput() throws XMLStreamException,
			FactoryConfigurationError {
		{
			StringWriter strWriter = new StringWriter();
			GOMOutputWriter writer = new GOMStaxWriter(strWriter);
			this.updateImpl.writeAtomOutput(writer);
			assertTrue(strWriter.toString().startsWith("<updated>"));
			assertTrue(strWriter.toString().endsWith("</updated>"));
		}
		{
			this.updateImpl.processElementValue(DATE);
			StringWriter strWriter = new StringWriter();
			GOMOutputWriter writer = new GOMStaxWriter(strWriter);
			this.updateImpl.writeAtomOutput(writer);
			assertEquals("<updated>" + DATE + "</updated>", strWriter
					.toString());
		}

		{
			StringWriter strWriter = new StringWriter();
			GOMOutputWriter writer = new GOMStaxWriter(strWriter);
			this.publishImpl.writeAtomOutput(writer);
			assertTrue(strWriter.toString().startsWith("<published>"));
			assertTrue(strWriter.toString().endsWith("</published>"));
		}
		{
			this.publishImpl.processElementValue(DATE);
			StringWriter strWriter = new StringWriter();
			GOMOutputWriter writer = new GOMStaxWriter(strWriter);
			this.publishImpl.writeAtomOutput(writer);
			assertEquals("<published>" + DATE + "</published>", strWriter
					.toString());
		}

	}

	/*
	 * Test method for
	 * 'org.apache.lucene.gdata.gom.core.GOMDateConstructImpl.writeRssOutput(GOMWriter)'
	 */
	public void testWriteRssOutput() throws XMLStreamException,
			FactoryConfigurationError {
		{
			StringWriter strWriter = new StringWriter();
			GOMOutputWriter writer = new GOMStaxWriter(strWriter);
			this.updateImpl.writeRssOutput(writer);
			assertTrue(strWriter.toString().startsWith("<atom:updated>"));
			assertTrue(strWriter.toString().endsWith("</atom:updated>"));
		}
		{
			this.updateImpl.processElementValue(DATE);
			StringWriter strWriter = new StringWriter();
			GOMOutputWriter writer = new GOMStaxWriter(strWriter);
			this.updateImpl.writeRssOutput(writer);
			assertEquals("<atom:updated>" + DATE + "</atom:updated>", strWriter
					.toString());
		}

		{
			StringWriter strWriter = new StringWriter();
			GOMOutputWriter writer = new GOMStaxWriter(strWriter);
			this.publishImpl.writeRssOutput(writer);

			assertTrue(strWriter.toString().startsWith("<pubDate>"));
			assertTrue(strWriter.toString().endsWith("</pubDate>"));
		}
		{
			this.publishImpl.processElementValue(DATE);
			StringWriter strWriter = new StringWriter();
			GOMOutputWriter writer = new GOMStaxWriter(strWriter);
			this.publishImpl.writeRssOutput(writer);
			assertEquals("<pubDate>" + DATE_RSS + "</pubDate>", strWriter
					.toString());
		}

	}

}
