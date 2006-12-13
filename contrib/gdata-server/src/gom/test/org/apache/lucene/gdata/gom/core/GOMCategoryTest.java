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

import org.apache.lucene.gdata.gom.GOMNamespace;
import org.apache.lucene.gdata.gom.writer.GOMStaxWriter;
import org.apache.lucene.gdata.gom.writer.GOMOutputWriter;

import junit.framework.TestCase;

/**
 * @author Simon Willnauer
 * 
 */
public class GOMCategoryTest extends TestCase {
	private QName qname = new QName(GOMNamespace.ATOM_NS_URI, "testme", "");

	GOMCategoryImpl cat;

	protected void setUp() throws Exception {
		this.cat = new GOMCategoryImpl();
	}

	protected void tearDown() throws Exception {
		super.tearDown();
	}

	/*
	 * Test method for
	 * 'org.apache.lucene.gdata.gom.core.GOMCategoryImpl.writeAtomOutput(GOMWriter)'
	 */
	public void testWriteAtomOutput() throws XMLStreamException {
		try {
			this.cat.writeAtomOutput(null);
			fail("wirter is null");
		} catch (NullPointerException e) {
			// 
		}
		{
			StringWriter strWriter = new StringWriter();
			GOMOutputWriter writer = new GOMStaxWriter(strWriter);
			this.cat.writeAtomOutput(writer);
			assertEquals("<category term=\"\"/>", strWriter.toString());

		}

		{
			StringWriter strWriter = new StringWriter();
			GOMOutputWriter writer = new GOMStaxWriter(strWriter);
			this.cat.term = "test";
			this.cat.writeAtomOutput(writer);
			assertEquals("<category term=\"test\"/>", strWriter.toString());

		}

		{
			StringWriter strWriter = new StringWriter();
			GOMOutputWriter writer = new GOMStaxWriter(strWriter);
			this.cat.label = "python";
			this.cat.scheme = "monty";

			this.cat.writeAtomOutput(writer);
			assertEquals(
					"<category term=\"test\" scheme=\"monty\" label=\"python\"/>",
					strWriter.toString());

		}

	}

	/*
	 * Test method for
	 * 'org.apache.lucene.gdata.gom.core.GOMCategoryImpl.writeRssOutput(GOMWriter)'
	 */
	public void testWriteRssOutput() throws XMLStreamException {
		try {
			this.cat.writeRssOutput(null);
			fail("wirter is null");
		} catch (NullPointerException e) {
			// 
		}

		{
			StringWriter strWriter = new StringWriter();
			GOMOutputWriter writer = new GOMStaxWriter(strWriter);
			this.cat.writeRssOutput(writer);
			assertEquals("<category domain=\"\"/>", strWriter.toString());
		}

		{
			StringWriter strWriter = new StringWriter();
			GOMOutputWriter writer = new GOMStaxWriter(strWriter);
			this.cat.scheme = "www.apache.org";
			this.cat.writeRssOutput(writer);
			assertEquals("<category domain=\"www.apache.org\"/>", strWriter
					.toString());
		}

		{
			StringWriter strWriter = new StringWriter();
			GOMOutputWriter writer = new GOMStaxWriter(strWriter);
			this.cat.scheme = "www.apache.org";
			this.cat.term = "Goo Data";
			this.cat.writeRssOutput(writer);
			assertEquals(
					"<category domain=\"www.apache.org\">Goo Data</category>",
					strWriter.toString());
		}

		{
			StringWriter strWriter = new StringWriter();
			GOMOutputWriter writer = new GOMStaxWriter(strWriter);
			this.cat.scheme = "www.apache.org";
			this.cat.term = "Goo Data";
			this.cat.label = "ignore";
			this.cat.writeRssOutput(writer);
			assertEquals(
					"<category domain=\"www.apache.org\">Goo Data</category>",
					strWriter.toString());
		}

	}

	/*
	 * Test method for
	 * 'org.apache.lucene.gdata.gom.core.AbstractGOMElement.processAttribute(QName,
	 * String)'
	 */
	public void testProcessAttribute() {
		try {
			this.cat.processAttribute(null, "test");
			fail("qname is null");
		} catch (GDataParseException e) {
			// 
		}
		{
			QName name = new QName("term");
			this.cat.processAttribute(name, "helloworld");
			assertEquals("helloworld", this.cat.getTerm());

			try {
				this.cat.processAttribute(name, "helloworld");
				fail("duplicated attribute");
			} catch (GDataParseException e) {
				// 
			}
		}

		{
			QName name = new QName("scheme");
			this.cat.processAttribute(name, "helloworld1");
			assertEquals("helloworld1", this.cat.getScheme());

			try {
				this.cat.processAttribute(name, "helloworld1");
				fail("duplicated attribute");
			} catch (GDataParseException e) {
				// 
			}
		}

		{
			QName name = new QName("label");
			this.cat.processAttribute(name, "John Cleese");
			assertEquals("John Cleese", this.cat.getLabel());

			try {
				this.cat.processAttribute(name, "John Cleese");
				fail("duplicated attribute");
			} catch (GDataParseException e) {
				// 
			}
		}

	}

	/*
	 * Test method for
	 * 'org.apache.lucene.gdata.gom.core.AbstractGOMElement.processElementValue(String)'
	 */
	public void testProcessElementValue() {
		try {
			this.cat.processElementValue(null);
			fail("element value is null");
		} catch (GDataParseException e) {
			// 
		}

		try {
			this.cat.processElementValue("and again");
			fail("can't contain a text value");
		} catch (GDataParseException e) {
			//
			assertNull(this.cat.getTextValue());
		}

	}

	/*
	 * Test method for
	 * 'org.apache.lucene.gdata.gom.core.AbstractGOMElement.processEndElement()'
	 */
	public void testProcessEndElement() {
		try {
			this.cat.processEndElement();
			fail("term is not set");
		} catch (GDataParseException e) {
			// 
		}
		this.cat.setTerm("my Term");
		this.cat.processEndElement();
		this.cat.setScheme("test");

		try {
			this.cat.processEndElement();
			fail("scheme is not a absoulte uri");
		} catch (GDataParseException e) {
			// 
		}

		this.cat.setScheme("/test");

		try {
			this.cat.processEndElement();
			fail("scheme is not a absoulte uri and no xmlbase is set");
		} catch (GDataParseException e) {
			// 
		}
		{
			this.cat.xmlBase = "http://www.apache.org";
			this.cat.processEndElement();
		}

		{
			this.cat.xmlBase = null;
			this.cat.setScheme("http://www.apache.org/test");
			this.cat.processEndElement();
		}

	}

}
