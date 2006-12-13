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

import javax.xml.stream.XMLStreamException;

import org.apache.lucene.gdata.gom.writer.GOMOutputWriter;
import org.apache.lucene.gdata.gom.writer.GOMStaxWriter;

import junit.framework.TestCase;

/**
 * @author Simon Willnauer
 * 
 */
public class GOMIdImplTest extends TestCase {
	GOMIdImpl impl;

	protected void setUp() throws Exception {
		this.impl = new GOMIdImpl();
	}

	/*
	 * Test method for
	 * 'org.apache.lucene.gdata.gom.core.GOMIdImpl.writeAtomOutput(GOMOutputWriter)'
	 */
	public void testWriteAtomOutput() throws XMLStreamException {
		{
			StringWriter stW = new StringWriter();
			GOMOutputWriter writer = new GOMStaxWriter(stW);
			this.impl.writeAtomOutput(writer);

			assertEquals("<id/>", stW.toString());
		}

		{
			this.impl.setTextValue("testme");
			StringWriter stW = new StringWriter();
			GOMOutputWriter writer = new GOMStaxWriter(stW);
			this.impl.writeAtomOutput(writer);

			assertEquals("<id>testme</id>", stW.toString());

		}

	}

	/*
	 * Test method for
	 * 'org.apache.lucene.gdata.gom.core.GOMIdImpl.writeRssOutput(GOMOutputWriter)'
	 */
	public void testWriteRssOutputGOMOutputWriter() throws XMLStreamException {
		{
			StringWriter stW = new StringWriter();
			GOMOutputWriter writer = new GOMStaxWriter(stW);
			this.impl.writeRssOutput(writer);

			assertEquals("<atom:id/>", stW.toString());
		}

		{
			this.impl.setTextValue("testme");
			StringWriter stW = new StringWriter();
			GOMOutputWriter writer = new GOMStaxWriter(stW);
			this.impl.writeRssOutput(writer);

			assertEquals("<atom:id>testme</atom:id>", stW.toString());

		}

		{
			this.impl.setTextValue("testme");
			StringWriter stW = new StringWriter();
			GOMOutputWriter writer = new GOMStaxWriter(stW);
			this.impl.writeRssOutput(writer, "guid");

			assertEquals("<guid>testme</guid>", stW.toString());

		}

	}

	public void testProcessElementValue() {
		this.impl.processElementValue("test");
		assertEquals("test", this.impl.getTextValue());
	}

	public void testProcessEndElement() {
		try {
			this.impl.processEndElement();
			fail("not set");
		} catch (GDataParseException e) {
			// 
		}
		this.impl.setTextValue("testme");
		this.impl.processEndElement();

	}
}
