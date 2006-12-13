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

import javax.xml.stream.FactoryConfigurationError;
import javax.xml.stream.XMLStreamException;

import org.apache.lucene.gdata.gom.GOMNamespace;
import org.apache.lucene.gdata.gom.writer.GOMStaxWriter;
import org.apache.lucene.gdata.gom.writer.GOMOutputWriter;

import junit.framework.TestCase;

/**
 * @author Simon Willnauer
 * 
 */
public class SimpleGOMElementImplTest extends TestCase {
	String localName = "test";

	SimpleGOMElementImpl impl;

	/**
	 * @see junit.framework.TestCase#setUp()
	 */
	@Override
	protected void setUp() throws Exception {
		this.impl = new SimpleGOMElementImpl(localName,
				GOMNamespace.ATOM_NAMESPACE);
	}

	/*
	 * Test method for
	 * 'org.apache.lucene.gdata.gom.core.SimpleGOMElementImpl.processElementValue(String)'
	 */
	public void testProcessElementValue() {
		this.impl.processElementValue("myValue");
		assertEquals("myValue", this.impl.getTextValue());

		try {
			this.impl.processElementValue("myValue");
			fail("duplicated");

		} catch (GDataParseException e) {
			// 
		}
	}

	/*
	 * Test method for
	 * 'org.apache.lucene.gdata.gom.core.SimpleGOMElementImpl.processEndElement()'
	 */
	public void testProcessEndElement() {
		// depends validator
		this.impl.processEndElement();
		this.impl
				.setValidator(new GOMFeedImpl.PositiveIntegerValidator("test"));
		try {
			this.impl.processEndElement();
			fail("value is null");
		} catch (GDataParseException e) {
			assertTrue(e.getMessage().indexOf("requires a element value") > 0);
		}
		this.impl.setTextValue("1");
		this.impl.processEndElement();

	}

	/*
	 * Test method for
	 * 'org.apache.lucene.gdata.gom.core.SimpleGOMElementImpl.SimpleGOMElementImpl(String,
	 * GOMNamespace)'
	 */
	public void testSimpleGOMElementImpl() {
		try {
			new SimpleGOMElementImpl(null, GOMNamespace.ATOM_NAMESPACE);
			fail("localname is null");
		} catch (IllegalArgumentException e) {
			// 
		}
		try {
			new SimpleGOMElementImpl("test", null);
			fail("namespace is null");
		} catch (IllegalArgumentException e) {
			// 
		}

		SimpleGOMElementImpl impl2 = new SimpleGOMElementImpl(this.localName,
				GOMNamespace.ATOM_NAMESPACE);
		assertEquals(impl2.getQname().getNamespaceURI(),
				GOMNamespace.ATOM_NS_URI);
		assertEquals(impl2.getQname().getPrefix(), GOMNamespace.ATOM_NS_PREFIX);
		assertEquals(impl2.getQname().getLocalPart(), this.localName);
		assertEquals(impl2.getLocalName(), this.localName);

	}

	/*
	 * Test method for
	 * 'org.apache.lucene.gdata.gom.core.SimpleGOMElementImpl.writeAtomOutput(GOMWriter)'
	 */
	public void testWriteAtomOutput() throws XMLStreamException,
			FactoryConfigurationError {
		{
			StringWriter strWriter = new StringWriter();
			GOMOutputWriter writer = new GOMStaxWriter(strWriter);
			this.impl.writeAtomOutput(writer);
			assertEquals("<atom:" + this.localName + "/>", strWriter.toString());
		}
		{
			StringWriter strWriter = new StringWriter();
			GOMOutputWriter writer = new GOMStaxWriter(strWriter);
			this.impl.setTextValue("hello world");
			this.impl.writeAtomOutput(writer);
			assertEquals("<atom:" + this.localName + ">hello world</atom:"
					+ this.localName + ">", strWriter.toString());
		}
	}

}
