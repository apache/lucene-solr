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

import javax.xml.namespace.QName;
import javax.xml.stream.FactoryConfigurationError;
import javax.xml.stream.XMLStreamException;

import org.apache.lucene.gdata.gom.GOMGenerator;
import org.apache.lucene.gdata.gom.GOMNamespace;
import org.apache.lucene.gdata.gom.writer.GOMStaxWriter;
import org.apache.lucene.gdata.gom.writer.GOMOutputWriter;

import junit.framework.TestCase;

public class GOMGenereatorImplTest extends TestCase {
	private GOMGeneratorImpl impl;

	protected void setUp() throws Exception {
		this.impl = new GOMGeneratorImpl();
	}

	/*
	 * Test method for
	 * 'org.apache.lucene.gdata.gom.core.GOMGeneratorImpl.processAttribute(QName,
	 * String)'
	 */
	public void testProcessAttribute() {
		this.impl.processAttribute(new QName(GOMNamespace.ATOM_NS_URI, "uri"),
				"test");
		assertEquals("test", this.impl.getUri());
		try {
			this.impl.processAttribute(new QName(GOMNamespace.ATOM_NS_URI,
					"uri"), "test");
			fail("duplicated");
		} catch (GDataParseException e) {

		}
		this.impl.processAttribute(new QName(GOMNamespace.ATOM_NS_URI,
				"version"), "test");
		assertEquals("test", this.impl.getGeneratorVersion());

		try {
			this.impl.processAttribute(new QName(GOMNamespace.ATOM_NS_URI,
					"version"), "test");
			fail("duplicated");
		} catch (GDataParseException e) {

		}

		// check call to super.processAttribute
		this.impl.processAttribute(new QName(GOMNamespace.XML_NS_URI, "base",
				GOMNamespace.XML_NS_PREFIX), "test");
		assertEquals("test", this.impl.xmlBase);

		try {
			this.impl.processAttribute(null, "test");
			fail("qname is null");
		} catch (IllegalArgumentException e) {
			// 
		}
		try {
			this.impl.processAttribute(new QName(GOMNamespace.XML_NS_URI,
					"base", GOMNamespace.XML_NS_PREFIX), null);
			fail("value is null");
		} catch (IllegalArgumentException e) {
			// 
		}

	}

	/*
	 * Test method for
	 * 'org.apache.lucene.gdata.gom.core.GOMGeneratorImpl.processElementValue(String)'
	 */
	public void testProcessElementValue() {
		this.impl.processElementValue("myGenerator");
		assertEquals("myGenerator", this.impl.getTextValue());

		try {
			this.impl.processElementValue("testme");
			fail("duplicated");

		} catch (GDataParseException e) {
			//
		}

	}

	/*
	 * Test method for
	 * 'org.apache.lucene.gdata.gom.core.GOMGeneratorImpl.processEndElement()'
	 */
	public void testProcessEndElement() {
		this.impl.processEndElement();
		{
			this.impl.setUri("some invalid uri");
			try {
				this.impl.processEndElement();
				fail("must be an absolute uri");
			} catch (GDataParseException e) {
				// 
			}

		}
		{
			this.impl.setUri("/uri");
			try {
				this.impl.processEndElement();
				fail("must be an absolute uri or xml:base must be set");
			} catch (GDataParseException e) {
				// 
			}

		}
		this.impl.xmlBase = "http://apache.org";
		this.impl.processEndElement();

		this.impl.xmlBase = null;
		this.impl.setUri("http://apache.org/uri");
		this.impl.processEndElement();
	}

	/*
	 * Test method for
	 * 'org.apache.lucene.gdata.gom.core.GOMGeneratorImpl.GOMGeneratorImpl()'
	 */
	public void testGOMGeneratorImpl() {
		this.impl = new GOMGeneratorImpl();
		assertEquals(GOMGenerator.LOCALNAME, this.impl.getLocalName());
		assertEquals(GOMGenerator.LOCALNAME, this.impl.getQname()
				.getLocalPart());
		assertEquals(GOMNamespace.ATOM_NS_URI, this.impl.getQname()
				.getNamespaceURI());
	}

	/*
	 * Test method for
	 * 'org.apache.lucene.gdata.gom.core.GOMGeneratorImpl.writeAtomOutput(GOMWriter)'
	 */
	public void testWriteAtomOutput() throws XMLStreamException {
		{
			StringWriter stW = new StringWriter();
			GOMOutputWriter writer = new GOMStaxWriter(stW);
			this.impl.writeAtomOutput(writer);
			assertEquals("<" + this.impl.getLocalName() + "/>", stW.toString());
		}

		{
			StringWriter stW = new StringWriter();
			GOMOutputWriter writer = new GOMStaxWriter(stW);
			this.impl.setTextValue("Lucene");
			this.impl.writeAtomOutput(writer);
			assertEquals("<" + this.impl.getLocalName() + ">Lucene</"
					+ this.impl.getLocalName() + ">", stW.toString());
		}
		{
			StringWriter stW = new StringWriter();
			GOMOutputWriter writer = new GOMStaxWriter(stW);
			this.impl.setUri("http://apache.org");
			this.impl.writeAtomOutput(writer);
			assertEquals("<" + this.impl.getLocalName()
					+ " uri=\"http://apache.org\">Lucene</"
					+ this.impl.getLocalName() + ">", stW.toString());
		}

		{
			StringWriter stW = new StringWriter();
			GOMOutputWriter writer = new GOMStaxWriter(stW);
			this.impl.setGeneratorVersion("1");
			this.impl.writeAtomOutput(writer);
			assertEquals("<" + this.impl.getLocalName()
					+ " uri=\"http://apache.org\" version=\"1\">Lucene</"
					+ this.impl.getLocalName() + ">", stW.toString());
		}
	}

	/*
	 * Test method for
	 * 'org.apache.lucene.gdata.gom.core.GOMGeneratorImpl.writeRssOutput(GOMWriter)'
	 */
	public void testWriteRssOutput() throws XMLStreamException,
			FactoryConfigurationError {
		{
			StringWriter stW = new StringWriter();
			GOMOutputWriter writer = new GOMStaxWriter(stW);
			this.impl.setTextValue("Lucene");
			this.impl.writeRssOutput(writer);
			assertEquals("<" + this.impl.getLocalName() + ">Lucene</"
					+ this.impl.getLocalName() + ">", stW.toString());
		}

		{
			StringWriter stW = new StringWriter();
			GOMOutputWriter writer = new GOMStaxWriter(stW);
			this.impl.setUri("http://apache.org");
			this.impl.setGeneratorVersion("1");
			this.impl.writeRssOutput(writer);
			assertEquals("<" + this.impl.getLocalName() + ">Lucene</"
					+ this.impl.getLocalName() + ">", stW.toString());
		}
	}

}
