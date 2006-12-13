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

import org.apache.lucene.gdata.gom.AtomMediaType;
import org.apache.lucene.gdata.gom.ContentType;
import org.apache.lucene.gdata.gom.GOMContent;
import org.apache.lucene.gdata.gom.GOMNamespace;
import org.apache.lucene.gdata.gom.core.GOMTextContructImpl.XMLBlobContentParser;
import org.apache.lucene.gdata.gom.writer.GOMOutputWriter;
import org.apache.lucene.gdata.gom.writer.GOMStaxWriter;

import junit.framework.TestCase;

/**
 * @author Simon Willnauer
 * 
 */
public class GOMContentImplTest extends TestCase {

	private GOMContentImpl impl;

	protected void setUp() throws Exception {
		super.setUp();
		this.impl = new GOMContentImpl();
	}

	/*
	 * Test method for
	 * 'org.apache.lucene.gdata.gom.core.GOMContentImpl.getChildParser(QName)'
	 */
	public void testGetChildParser() {
		try {
			this.impl.getChildParser(new QName("test"));
			fail("no blob specified");
		} catch (GDataParseException e) {
			// 
		}

		this.impl.setAtomMediaType(AtomMediaType.XML);
		AtomParser childParser = this.impl.getChildParser(new QName("test"));
		assertNotNull(childParser);
		assertTrue(childParser instanceof XMLBlobContentParser);

	}

	/*
	 * Test method for
	 * 'org.apache.lucene.gdata.gom.core.GOMContentImpl.processAttribute(QName,
	 * String)'
	 */
	public void testProcessAttribute() {
		try {
			this.impl.processAttribute(null, "test");
			fail("qname is null");
		} catch (GDataParseException e) {
			// 
		}
		try {
			this.impl.processAttribute(new QName("test"), null);
			fail("value is null");
		} catch (GDataParseException e) {
			// 
		}
		this.impl.processAttribute(new QName(GOMNamespace.ATOM_NS_URI, "type"),
				"text/xml");
		assertSame(AtomMediaType.XML, this.impl.getAtomMediaType());
		try {
			this.impl.processAttribute(new QName(GOMNamespace.ATOM_NS_URI,
					"type"), "text/xml");
			fail("duplicated attribute");
		} catch (GDataParseException e) {
			// 
		}
		this.impl.setAtomMediaType(null);
		this.impl.processAttribute(new QName(GOMNamespace.ATOM_NS_URI, "type"),
				"text/plain");
		assertSame(AtomMediaType.TEXT, this.impl.getAtomMediaType());

		this.impl.setAtomMediaType(null);
		this.impl.processAttribute(new QName(GOMNamespace.ATOM_NS_URI, "type"),
				"image/jpeg");
		assertSame(AtomMediaType.BINARY, this.impl.getAtomMediaType());

		// test if super is called
		this.impl.setAtomMediaType(null);
		this.impl.processAttribute(new QName(GOMNamespace.ATOM_NS_URI, "type"),
				"xhtml");
		assertNull(this.impl.getAtomMediaType());
		assertSame(ContentType.XHTML, this.impl.getContentType());

		this.impl.processAttribute(new QName(GOMNamespace.ATOM_NS_URI, "src"),
				"test");
		assertEquals("test", this.impl.getSrc());
		try {
			this.impl.processAttribute(new QName(GOMNamespace.ATOM_NS_URI,
					"src"), "text/xml");
			fail("duplicated attribute");
		} catch (GDataParseException e) {
			// 
		}

	}

	/*
	 * Test method for
	 * 'org.apache.lucene.gdata.gom.core.GOMContentImpl.processElementValue(String)'
	 */
	public void testProcessElementValue() {
		assertNull(this.impl.getTextValue());
		this.impl.processElementValue("test");
		assertEquals("test", this.impl.getTextValue());
		this.impl.setSrc("http://www.apache.org");
		try {
			this.impl.processElementValue("test");
			fail("src is set no element value allowed");
		} catch (GDataParseException e) {
			// 
		}

	}

	/*
	 * Test method for
	 * 'org.apache.lucene.gdata.gom.core.GOMContentImpl.processEndElement()'
	 */
	public void testProcessEndElement() {
		try {
			this.impl.processEndElement();
			fail("no type attribute");
		} catch (GDataParseException e) {
			// 
		}
		this.impl.processAttribute(new QName(GOMNamespace.ATOM_NS_URI, "type"),
				"text/plain");
		this.impl.processEndElement();
		this.impl.setSrc("http://www.apache.org");
		this.impl.processEndElement();

		this.impl.setSrc("/test");
		try {
			this.impl.processEndElement();
			fail("must be absolut uri");
		} catch (GDataParseException e) {
			// 
		}
		this.impl.xmlBase = "http://www.apache.org";
		this.impl.processEndElement();
	}

	/*
	 * Test method for
	 * 'org.apache.lucene.gdata.gom.core.GOMContentImpl.writeAtomOutput(GOMOutputWriter)'
	 */
	public void testWriteAtomOutput() throws XMLStreamException,
			FactoryConfigurationError {
		{
			StringWriter stW = new StringWriter();
			GOMOutputWriter writer = new GOMStaxWriter(stW);
			this.impl.writeAtomOutput(writer);
			assertEquals("<content type=\"text\"/>", stW.toString());
		}

		{
			this.impl.processAttribute(new QName(GOMNamespace.ATOM_NS_URI,
					"type"), "image/jpeg");
			StringWriter stW = new StringWriter();
			GOMOutputWriter writer = new GOMStaxWriter(stW);
			this.impl.writeAtomOutput(writer);
			assertEquals("<content type=\"image/jpeg\"/>", stW.toString());
		}

		{
			this.impl.setSrc("http://www.apache.org");
			this.impl.setTextValue("hello world");
			StringWriter stW = new StringWriter();
			GOMOutputWriter writer = new GOMStaxWriter(stW);
			this.impl.writeAtomOutput(writer);
			assertEquals(
					"<content type=\"image/jpeg\" src=\"http://www.apache.org\"/>",
					stW.toString());
		}

		{
			this.impl.setSrc(null);
			this.impl.setTextValue("hello world");
			StringWriter stW = new StringWriter();
			GOMOutputWriter writer = new GOMStaxWriter(stW);
			this.impl.writeAtomOutput(writer);
			assertEquals("<content type=\"image/jpeg\">hello world</content>",
					stW.toString());
		}

	}

	/*
	 * Test method for
	 * 'org.apache.lucene.gdata.gom.core.GOMContentImpl.writeRssOutput(GOMOutputWriter)'
	 */
	public void testWriteRssOutputGOMOutputWriter() throws XMLStreamException {
		{
			StringWriter stW = new StringWriter();
			GOMOutputWriter writer = new GOMStaxWriter(stW);
			this.impl.writeRssOutput(writer);
			assertEquals("<description/>", stW.toString());
		}

		{
			this.impl.setSrc("http://www.apache.org");
			this.impl.setAtomMediaType(AtomMediaType.TEXT);
			StringWriter stW = new StringWriter();
			GOMOutputWriter writer = new GOMStaxWriter(stW);
			this.impl.writeRssOutput(writer);
			assertEquals("<link>http://www.apache.org</link>", stW.toString());
		}

		{
			this.impl.setSrc(null);
			this.impl.setAtomMediaType(AtomMediaType.TEXT);
			this.impl.setTextValue("test");
			StringWriter stW = new StringWriter();
			GOMOutputWriter writer = new GOMStaxWriter(stW);
			this.impl.writeRssOutput(writer);
			assertEquals("<description>test</description>", stW.toString());
		}

		{
			this.impl.setAtomMediaType(null);

			this.impl.processAttribute(new QName(GOMNamespace.ATOM_NS_URI,
					"type"), "image/jpeg");
			StringWriter stW = new StringWriter();
			GOMOutputWriter writer = new GOMStaxWriter(stW);
			this.impl.writeRssOutput(writer);
			assertEquals("<content type=\"image/jpeg\">test</content>", stW
					.toString());
		}

	}

	/*
	 * Test method for
	 * 'org.apache.lucene.gdata.gom.core.GOMContentImpl.GOMContentImpl()'
	 */
	public void testGOMContentImpl() {
		GOMContentImpl impl2 = new GOMContentImpl();
		assertEquals(GOMContent.LOCALNAME, impl2.getLocalName());
		assertEquals(GOMContent.LOCALNAME, impl2.getQname().getLocalPart());
		assertEquals(GOMNamespace.ATOM_NS_URI, impl2.getQname()
				.getNamespaceURI());

	}

}
