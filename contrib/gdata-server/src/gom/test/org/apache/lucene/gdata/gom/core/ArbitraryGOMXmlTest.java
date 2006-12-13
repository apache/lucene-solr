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
import java.util.List;

import javax.xml.namespace.QName;
import javax.xml.stream.FactoryConfigurationError;
import javax.xml.stream.XMLStreamException;

import junit.framework.TestCase;

import org.apache.lucene.gdata.gom.GOMAttribute;
import org.apache.lucene.gdata.gom.writer.GOMStaxWriter;
import org.apache.lucene.gdata.gom.writer.GOMOutputWriter;

/**
 * @author Simon Willnauer
 * 
 */
public class ArbitraryGOMXmlTest extends TestCase {
	private ArbitraryGOMXml arbXML;

	private QName name = new QName("testme");

	protected void setUp() throws Exception {
		super.setUp();

		arbXML = new ArbitraryGOMXml(name);
	}

	protected void tearDown() throws Exception {
		super.tearDown();
	}

	/*
	 * Test method for
	 * 'org.apache.lucene.gdata.gom.core.ArbitraryGOMXml.getChildParser(QName)'
	 */
	public void testGetChildParser() {

		try {
			this.arbXML.getChildParser(null);
			fail("qname is null");
		} catch (GDataParseException e) {
			assertEquals(0, this.arbXML.getChildren().size());
		}
		AtomParser childParser = this.arbXML.getChildParser(this.name);
		assertTrue(childParser instanceof ArbitraryGOMXml);

		assertEquals(name, ((ArbitraryGOMXml) childParser).getQname());
		assertEquals(name.getLocalPart(), ((ArbitraryGOMXml) childParser)
				.getLocalName());
		assertEquals(1, this.arbXML.getChildren().size());
	}

	/*
	 * Test method for
	 * 'org.apache.lucene.gdata.gom.core.ArbitraryGOMXml.processAttribute(QName,
	 * String)'
	 */
	public void testProcessAttribute() {
		try {
			this.arbXML.processAttribute(null, "test");
			fail("qname is null");

		} catch (GDataParseException e) {
			assertTrue(this.arbXML.getAttributes().size() == 0);
		}
		this.arbXML.processAttribute(name, "testme");
		{
			List<GOMAttribute> attributes = this.arbXML.getAttributes();
			assertTrue(attributes.size() == 1);
			GOMAttribute attribute = attributes.get(0);
			assertNotNull(attribute);
			assertEquals(name, attribute.getQname());
			assertEquals(name.getLocalPart(), attribute.getLocalName());
			assertEquals("testme", attribute.getTextValue());
		}

		{
			this.arbXML.processAttribute(name, null);
			List<GOMAttribute> attributes = this.arbXML.getAttributes();
			assertTrue(attributes.size() == 2);
			GOMAttribute attribute = attributes.get(1);
			assertNotNull(attribute);
			assertEquals(name, attribute.getQname());
			assertEquals(name.getLocalPart(), attribute.getLocalName());
			assertEquals("", attribute.getTextValue());
		}
	}

	/*
	 * Test method for
	 * 'org.apache.lucene.gdata.gom.core.ArbitraryGOMXml.processElementValue(String)'
	 */
	public void testProcessElementValue() {
		try {
			this.arbXML.processElementValue(null);
			fail("name is null");
		} catch (GDataParseException e) {
			// 
		}
		this.arbXML.processElementValue("test value");

		try {
			this.arbXML.processElementValue("test value");
			fail("value is already set");
		} catch (GDataParseException e) {
			//
		}
		assertEquals("test value", this.arbXML.getTextValue());

	}

	/*
	 * Test method for
	 * 'org.apache.lucene.gdata.gom.core.ArbitraryGOMXml.ArbitraryGOMXml(QName)'
	 */
	public void testArbitraryGOMXml() {
		try {
			new ArbitraryGOMXml(null);
			fail("qname is null");
		} catch (IllegalArgumentException e) {

		}
		ArbitraryGOMXml xml = new ArbitraryGOMXml(name);
		assertEquals(name, xml.getQname());
		assertEquals(name.getLocalPart(), xml.getLocalName());
	}

	/*
	 * Test method for
	 * 'org.apache.lucene.gdata.gom.core.ArbitraryGOMXml.writeAtomOutput(GOMWriter)'
	 */
	public void testWriteAtomOutput() throws XMLStreamException,
			FactoryConfigurationError {
		{
			StringWriter writer = new StringWriter();
			GOMOutputWriter w = new GOMStaxWriter(writer);
			this.arbXML.writeAtomOutput(w);
			assertNotNull(w.toString());
			assertEquals("<" + this.name.getLocalPart() + "/>", writer
					.toString());
		}
		try {
			this.arbXML.writeAtomOutput(null);
			fail("writer is null");

		} catch (NullPointerException e) {
			// 
		}
		{
			this.arbXML.processAttribute(name, "testme1");
			this.arbXML.processElementValue("testme2");
			StringWriter writer = new StringWriter();
			GOMOutputWriter w = new GOMStaxWriter(writer);
			this.arbXML.writeAtomOutput(w);
			assertEquals("<" + this.name.getLocalPart()
					+ " testme=\"testme1\">" + "testme2" + "</"
					+ this.name.getLocalPart() + ">", writer.toString());
		}
	}
}
