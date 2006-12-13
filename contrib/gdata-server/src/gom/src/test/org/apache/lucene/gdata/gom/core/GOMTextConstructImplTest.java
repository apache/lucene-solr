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

import org.apache.lucene.gdata.gom.ContentType;
import org.apache.lucene.gdata.gom.core.GOMTextContructImpl.XMLBlobContentParser;
import org.apache.lucene.gdata.gom.writer.GOMStaxWriter;
import org.apache.lucene.gdata.gom.writer.GOMOutputWriter;

import junit.framework.TestCase;

/**
 * @author Simon Willnauer
 * 
 */
public class GOMTextConstructImplTest extends TestCase {
	GOMTitleImpl titleImpl;

	GOMSubtitleImpl subTitleImpl;

	protected void setUp() throws Exception {
		this.titleImpl = new GOMTitleImpl();
		this.subTitleImpl = new GOMSubtitleImpl();
	}

	/*
	 * Test method for
	 * 'org.apache.lucene.gdata.gom.core.GOMTextContructImpl.getChildParser(QName)'
	 */
	public void testGetChildParser() {
		try {
			this.titleImpl.getChildParser(null);
			fail("qname must not be null");
		} catch (GDataParseException e) {
			// 
		}
		try {
			this.titleImpl.getChildParser(new QName("test"));
			fail("no such child supported");
		} catch (GDataParseException e) {
			// 
		}

		try {
			this.titleImpl.getChildParser(new QName("div"));
			fail("content type not set");
		} catch (GDataParseException e) {
			// 
		}
		this.titleImpl.contentType = ContentType.XHTML;
		AtomParser childParser = this.titleImpl
				.getChildParser(new QName("div"));
		assertNotNull(childParser);
		assertTrue(childParser instanceof XMLBlobContentParser);
		try {
			this.titleImpl.getChildParser(new QName("div"));
			fail("duplicated element");
		} catch (GDataParseException e) {
			// 
		}

	}

	/*
	 * Test method for
	 * 'org.apache.lucene.gdata.gom.core.GOMTextContructImpl.processAttribute(QName,
	 * String)' includes Test method for
	 * 'org.apache.lucene.gdata.gom.core.GOMTextContructImpl.getContentType()
	 */
	public void testProcessAttribute() {
		try {
			this.titleImpl.processAttribute(null, "test");
			fail("qname is null");
		} catch (GDataParseException e) {
			// 
		}
		this.titleImpl.processAttribute(new QName("type"), "text");
		assertEquals(ContentType.TEXT, this.titleImpl.getContentType());
		this.titleImpl.contentType = null;
		this.titleImpl.processAttribute(new QName("type"), "html");
		assertEquals(ContentType.HTML, this.titleImpl.getContentType());
		this.titleImpl.contentType = null;
		this.titleImpl.processAttribute(new QName("type"), "xhtml");
		assertEquals(ContentType.XHTML, this.titleImpl.getContentType());

	}

	/*
	 * Test method for
	 * 'org.apache.lucene.gdata.gom.core.GOMTextContructImpl.processElementValue(String)'
	 */
	public void testProcessElementValue() {
		this.titleImpl.processElementValue("test");
		assertEquals("test", this.titleImpl.getTextValue());
		assertNull(this.titleImpl.htmlBuilder);
		this.titleImpl.processAttribute(new QName("type"), "html");
		assertNotNull(this.titleImpl.htmlBuilder);

		this.titleImpl.processElementValue("test");
		assertEquals("test", this.titleImpl.getTextValue());
		assertEquals("test", this.titleImpl.htmlBuilder.toString());

	}

	/*
	 * Test method for
	 * 'org.apache.lucene.gdata.gom.core.GOMTextContructImpl.processEndElement()'
	 */
	public void testProcessEndElement() {
		try {
			this.titleImpl.processEndElement();
			fail("no content type");
		} catch (GDataParseException e) {
			// 
		}
		this.titleImpl.contentType = ContentType.TEXT;
		this.titleImpl.processEndElement();
	}

	/*
	 * Test method for
	 * 'org.apache.lucene.gdata.gom.core.GOMTextContructImpl.writeAtomOutput(GOMWriter)'
	 */
	public void testWriteAtomOutput() throws XMLStreamException,
			FactoryConfigurationError {
		{
			this.titleImpl.contentType = ContentType.TEXT;
			StringWriter strWriter = new StringWriter();
			GOMOutputWriter writer = new GOMStaxWriter(strWriter);
			this.titleImpl.writeAtomOutput(writer);
			assertEquals("<title type=\"text\"/>", strWriter.toString());
		}
		{
			this.titleImpl.setTextValue("><hello world");
			StringWriter strWriter = new StringWriter();
			GOMOutputWriter writer = new GOMStaxWriter(strWriter);
			this.titleImpl.writeAtomOutput(writer);
			assertEquals("<title type=\"text\">&gt;&lt;hello world</title>",
					strWriter.toString());
		}

		{
			this.titleImpl.contentType = ContentType.HTML;
			StringWriter strWriter = new StringWriter();
			GOMOutputWriter writer = new GOMStaxWriter(strWriter);
			this.titleImpl.writeAtomOutput(writer);
			assertEquals("<title type=\"html\">&gt;&lt;hello world</title>",
					strWriter.toString());
		}

		{
			this.titleImpl.contentType = ContentType.XHTML;
			StringWriter strWriter = new StringWriter();
			GOMOutputWriter writer = new GOMStaxWriter(strWriter);
			this.titleImpl.writeAtomOutput(writer);
			assertEquals("<title type=\"xhtml\">><hello world</title>",
					strWriter.toString());
		}

	}

	/*
	 * Test method for
	 * 'org.apache.lucene.gdata.gom.core.GOMTextContructImpl.writeRssOutput(GOMWriter)'
	 */
	public void testWriteRssOutput() throws XMLStreamException {

		{
			this.titleImpl.contentType = ContentType.TEXT;
			StringWriter strWriter = new StringWriter();
			GOMOutputWriter writer = new GOMStaxWriter(strWriter);
			this.titleImpl.writeRssOutput(writer);
			assertEquals("<title/>", strWriter.toString());
		}
		{
			this.titleImpl.setTextValue("><hello world");
			StringWriter strWriter = new StringWriter();
			GOMOutputWriter writer = new GOMStaxWriter(strWriter);
			this.titleImpl.writeRssOutput(writer);
			assertEquals("<title>&gt;&lt;hello world</title>", strWriter
					.toString());
		}

		{
			this.titleImpl.contentType = ContentType.HTML;
			StringWriter strWriter = new StringWriter();
			GOMOutputWriter writer = new GOMStaxWriter(strWriter);
			this.titleImpl.writeRssOutput(writer);
			assertEquals("<title>&gt;&lt;hello world</title>", strWriter
					.toString());
		}

		{
			this.titleImpl.contentType = ContentType.XHTML;
			StringWriter strWriter = new StringWriter();
			GOMOutputWriter writer = new GOMStaxWriter(strWriter);
			this.titleImpl.writeRssOutput(writer);
			// no markup in rss
			assertEquals("<title>&gt;&lt;hello world</title>", strWriter
					.toString());
		}

		{
			this.titleImpl.contentType = ContentType.XHTML;
			this.titleImpl.xmlBase = "http://www.apache.org";
			this.titleImpl.xmlLang = "en";
			StringWriter strWriter = new StringWriter();
			GOMOutputWriter writer = new GOMStaxWriter(strWriter);
			this.titleImpl.writeRssOutput(writer);
			// no markup in rss
			assertEquals(
					"<title xml:base=\"http://www.apache.org\" xml:lang=\"en\">&gt;&lt;hello world</title>",
					strWriter.toString());
		}

		{
			this.subTitleImpl.contentType = ContentType.XHTML;
			StringWriter strWriter = new StringWriter();
			GOMOutputWriter writer = new GOMStaxWriter(strWriter);
			this.subTitleImpl.writeRssOutput(writer);

			assertEquals("<description/>", strWriter.toString());
		}

		{
			this.subTitleImpl.contentType = ContentType.XHTML;
			this.subTitleImpl.setTextValue("><hello world");
			StringWriter strWriter = new StringWriter();
			GOMOutputWriter writer = new GOMStaxWriter(strWriter);
			this.subTitleImpl.writeRssOutput(writer);

			assertEquals("<description>&gt;&lt;hello world</description>",
					strWriter.toString());
		}
	}

}
