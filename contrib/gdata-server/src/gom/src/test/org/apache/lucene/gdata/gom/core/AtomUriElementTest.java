package org.apache.lucene.gdata.gom.core;

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

import java.io.StringWriter;

import javax.xml.stream.FactoryConfigurationError;
import javax.xml.stream.XMLStreamException;

import org.apache.lucene.gdata.gom.GOMIcon;
import org.apache.lucene.gdata.gom.GOMLogo;
import org.apache.lucene.gdata.gom.writer.GOMStaxWriter;
import org.apache.lucene.gdata.gom.writer.GOMOutputWriter;

import junit.framework.TestCase;

/**
 * @author Simon Willnauer
 * 
 */
public class AtomUriElementTest extends TestCase {
	GOMIconImpl iconImpl;

	GOMLogoImpl logoImpl;

	protected void setUp() throws Exception {
		this.iconImpl = new GOMIconImpl();
		this.logoImpl = new GOMLogoImpl();

	}

	/*
	 * Test method for
	 * 'org.apache.lucene.gdata.gom.core.AtomUriElement.processEndElement()'
	 */
	public void testProcessEndElement() {
		try {
			this.iconImpl.processEndElement();
			fail("no text value");
		} catch (GDataParseException e) {
			assertTrue(e.getMessage().indexOf("requires a element value") > 0);
		}

		try {
			this.iconImpl.setTextValue("test");

			this.iconImpl.processEndElement();
			fail("no text value");
		} catch (GDataParseException e) {
			assertTrue(e.getMessage().indexOf("must be a") > 0);
		}
		try {
			this.iconImpl.setTextValue("/test");

			this.iconImpl.processEndElement();
			fail("no text value");
		} catch (GDataParseException e) {
			assertTrue(e.getMessage().indexOf("must be a") > 0);
		}
		this.iconImpl.xmlBase = "http://www.apache.org";
		this.iconImpl.setTextValue("/test");

		this.iconImpl.processEndElement();

		this.iconImpl.xmlBase = null;
		this.iconImpl.setTextValue("http://www.apache.org/test");

	}

	public void testConstructor() {
		assertEquals(GOMIcon.LOCALNAME, this.iconImpl.getLocalName());
		assertEquals(GOMIcon.LOCALNAME, this.iconImpl.getQname().getLocalPart());
		assertEquals(GOMLogo.LOCALNAME, this.logoImpl.getLocalName());
		assertEquals(GOMLogo.LOCALNAME, this.logoImpl.getQname().getLocalPart());
	}

	public void testWriteRssOutput() throws XMLStreamException,
			FactoryConfigurationError {

		{
			StringWriter stW = new StringWriter();
			GOMOutputWriter writer = new GOMStaxWriter(stW);
			this.iconImpl.writeRssOutput(writer);
			assertEquals("<url/>", stW.toString());
		}
		{
			StringWriter stW = new StringWriter();
			GOMOutputWriter writer = new GOMStaxWriter(stW);
			this.iconImpl.setTextValue("test");
			this.iconImpl.writeRssOutput(writer);
			assertEquals("<url>test</url>", stW.toString());
		}

		{
			StringWriter stW = new StringWriter();
			GOMOutputWriter writer = new GOMStaxWriter(stW);
			this.logoImpl.writeRssOutput(writer);
			assertEquals("<url/>", stW.toString());
		}
		{
			StringWriter stW = new StringWriter();
			GOMOutputWriter writer = new GOMStaxWriter(stW);
			this.logoImpl.setTextValue("test");
			this.logoImpl.writeRssOutput(writer);
			assertEquals("<url>test</url>", stW.toString());
		}
	}

	public void testWriteAtomOutput() throws XMLStreamException {
		{
			StringWriter stW = new StringWriter();
			GOMOutputWriter writer = new GOMStaxWriter(stW);
			this.iconImpl.writeAtomOutput(writer);
			assertEquals("<icon/>", stW.toString());
		}
		{
			StringWriter stW = new StringWriter();
			GOMOutputWriter writer = new GOMStaxWriter(stW);
			this.iconImpl.setTextValue("test");
			this.iconImpl.writeAtomOutput(writer);
			assertEquals("<icon>test</icon>", stW.toString());
		}

		{
			StringWriter stW = new StringWriter();
			GOMOutputWriter writer = new GOMStaxWriter(stW);
			this.logoImpl.writeAtomOutput(writer);
			assertEquals("<logo/>", stW.toString());
		}
		{
			StringWriter stW = new StringWriter();
			GOMOutputWriter writer = new GOMStaxWriter(stW);
			this.logoImpl.setTextValue("test");
			this.logoImpl.writeAtomOutput(writer);
			assertEquals("<logo>test</logo>", stW.toString());
		}
	}
}
