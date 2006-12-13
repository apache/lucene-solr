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
import javax.xml.stream.XMLStreamException;

import org.apache.lucene.gdata.gom.GOMLink;
import org.apache.lucene.gdata.gom.writer.GOMStaxWriter;
import org.apache.lucene.gdata.gom.writer.GOMOutputWriter;

import junit.framework.TestCase;

/**
 * @author Simon Willnauer
 * 
 */
public class GOMLinkImplTest extends TestCase {

	private GOMLinkImpl impl;

	/**
	 * @see junit.framework.TestCase#setUp()
	 */
	@Override
	protected void setUp() throws Exception {
		impl = new GOMLinkImpl();
	}

	public void testCommonFields() {
		assertNotNull(this.impl.getQname());
		QName qname = this.impl.getQname();
		assertEquals(qname, new QName(GOMLink.LOCALNAME));
		assertEquals(qname.getLocalPart(), this.impl.getLocalName());
	}

	/*
	 * Test method for
	 * 'org.apache.lucene.gdata.gom.core.GOMLinkImpl.processAttribute(QName,
	 * String)'
	 */
	public void testProcessAttribute() {
		// title
		this.impl.processAttribute(new QName("title"), "title");
		assertEquals("title", this.impl.getTitle());
		try {
			this.impl.processAttribute(new QName("title"), "title");
			fail("duplicated attribute");
		} catch (GDataParseException e) {
			// 
		}

		// hreflang
		this.impl.processAttribute(new QName("hreflang"), "hreflang");
		assertEquals("hreflang", this.impl.getHrefLang());
		try {
			this.impl.processAttribute(new QName("hreflang"), "hreflang");
			fail("duplicated attribute");
		} catch (GDataParseException e) {
			// 
		}

		// href
		this.impl.processAttribute(new QName("href"), "href");
		assertEquals("href", this.impl.getHref());
		try {
			this.impl.processAttribute(new QName("href"), "href");
			fail("duplicated attribute");
		} catch (GDataParseException e) {
			// 
		}
		// type
		this.impl.processAttribute(new QName("type"), "type");
		assertEquals("type", this.impl.getType());
		try {
			this.impl.processAttribute(new QName("type"), "type");
			fail("duplicated attribute");
		} catch (GDataParseException e) {
			// 
		}

		// lenght
		try {
			this.impl.processAttribute(new QName("length"), "noint");
			fail("must be an integer");
		} catch (GDataParseException e) {
			// 
		}

		this.impl.processAttribute(new QName("length"), "1");
		assertEquals(new Integer(1), this.impl.getLength());
		try {
			this.impl.processAttribute(new QName("length"), "1");
			fail("duplicated attribute");
		} catch (GDataParseException e) {
			// 
		}

		// 
		// rel
		this.impl.processAttribute(new QName("rel"), "relation");
		assertEquals("relation", this.impl.getRel());
		try {
			this.impl.processAttribute(new QName("rel"), "relation");
			fail("duplicated attribute");
		} catch (GDataParseException e) {
			// 
		}

	}

	/*
	 * Test method for
	 * 'org.apache.lucene.gdata.gom.core.GOMLinkImpl.processEndElement()'
	 */
	public void testProcessEndElement() {
		try {
			this.impl.processEndElement();
			fail("href is requiered but not set");
		} catch (GDataParseException e) {
			// 
		}

		this.impl.setHref("/helloworld");
		try {
			this.impl.processEndElement();
			fail("href is not an absolute url");
		} catch (GDataParseException e) {
			// 
		}
		this.impl.xmlBase = "http://url";
		this.impl.processEndElement();
		this.impl.xmlBase = null;
		this.impl.setHref("http://www.apache.org");
		this.impl.processEndElement();

	}

	/*
	 * Test method for
	 * 'org.apache.lucene.gdata.gom.core.GOMLinkImpl.writeAtomOutput(GOMWriter)'
	 */
	public void testWriteAtomOutput() throws XMLStreamException {
		{
			StringWriter strWriter = new StringWriter();
			GOMOutputWriter writer = new GOMStaxWriter(strWriter);
			this.impl.writeAtomOutput(writer);
			assertEquals("<link href=\"\"/>", strWriter.toString());
		}
		{
			StringWriter strWriter = new StringWriter();
			GOMOutputWriter writer = new GOMStaxWriter(strWriter);
			this.impl.setHref("test");
			this.impl.setHrefLang("test1");
			this.impl.setLength(2);
			this.impl.setRel("NEXT");
			this.impl.setTitle("myTitle");
			this.impl.setType("myType");
			this.impl.writeAtomOutput(writer);
			assertTrue(strWriter.toString().contains("href=\"test\""));
			assertTrue(strWriter.toString().contains("title=\"myTitle\""));
			assertTrue(strWriter.toString().contains("hreflang=\"test1\""));
			assertTrue(strWriter.toString().contains("type=\"myType\""));
			assertTrue(strWriter.toString().contains("rel=\"NEXT\""));
			assertTrue(strWriter.toString().contains("length=\"2\""));
		}
	}

	/*
	 * Test method for
	 * 'org.apache.lucene.gdata.gom.core.GOMLinkImpl.writeRssOutput(GOMWriter)'
	 */
	public void testWriteRssOutput() throws XMLStreamException {
		{
			StringWriter strWriter = new StringWriter();
			GOMOutputWriter writer = new GOMStaxWriter(strWriter);
			this.impl.writeRssOutput(writer);
			assertEquals("", strWriter.toString());
		}

		{
			this.impl.setHref("test");
			this.impl.setType("testType");
			this.impl.setRel("enclosure");
			StringWriter strWriter = new StringWriter();
			GOMOutputWriter writer = new GOMStaxWriter(strWriter);
			this.impl.writeRssOutput(writer);
			assertEquals("<enclosure type=\"testType\" href=\"test\"/>",
					strWriter.toString());
		}

		{
			this.impl.setRel("comments");
			StringWriter strWriter = new StringWriter();
			GOMOutputWriter writer = new GOMStaxWriter(strWriter);
			this.impl.writeRssOutput(writer);
			assertEquals("<comments>test</comments>", strWriter.toString());
		}

		{
			this.impl.setRel("alternate");
			StringWriter strWriter = new StringWriter();
			GOMOutputWriter writer = new GOMStaxWriter(strWriter);
			this.impl.writeRssOutput(writer);
			assertEquals("<link>test</link>", strWriter.toString());
		}

	}

	/*
	 * Test method for
	 * 'org.apache.lucene.gdata.gom.core.AbstractGOMElement.processElementValue(String)'
	 */
	public void testProcessElementValue() {
		try {
			this.impl.processElementValue("hello world");
			fail("no content");
		} catch (GDataParseException e) {
			// TODO: handle exception
		}
	}

}
