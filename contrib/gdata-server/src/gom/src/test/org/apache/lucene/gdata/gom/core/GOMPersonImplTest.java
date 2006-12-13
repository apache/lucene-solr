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

import javax.naming.NameParser;
import javax.xml.namespace.QName;
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
public class GOMPersonImplTest extends TestCase {

	/**
	 * @see junit.framework.TestCase#setUp()
	 */
	@Override
	protected void setUp() throws Exception {
		// TODO Auto-generated method stub
		super.setUp();
	}

	/*
	 * Test method for
	 * 'org.apache.lucene.gdata.gom.core.GOMPersonImpl.writeAtomOutput(GOMWriter)'
	 */
	public void testWriteAtomOutput() throws XMLStreamException,
			FactoryConfigurationError {
		StringWriter stW = new StringWriter();
		GOMOutputWriter writer = new GOMStaxWriter(stW);
		GOMPersonImpl impl = new GOMPersonImpl();
		impl.writeRssOutput(writer);
		writer.flush();
		writer.close();

		// test with name
		stW = new StringWriter();
		writer = new GOMStaxWriter(stW);
		impl.setName("test");
		impl.writeAtomOutput(writer);
		writer.flush();
		assertEquals("<person><name>test</name></person>", stW.toString());

		writer.close();

		// test with name
		stW = new StringWriter();
		writer = new GOMStaxWriter(stW);
		impl.setEmail("simonw@apache.org");
		impl.setUri("http://www.apache.org");
		impl.writeAtomOutput(writer);
		writer.flush();
		assertEquals(
				"<person><name>test</name><email>simonw@apache.org</email><uri>http://www.apache.org</uri></person>",
				stW.toString());
		try {
			impl.writeAtomOutput(null);
			fail("must not be null");

		} catch (GDataParseException e) {
			//
		}
	}

	/*
	 * Test method for
	 * 'org.apache.lucene.gdata.gom.core.GOMPersonImpl.writeRssOutput(GOMWriter)'
	 */
	public void testWriteRssOutput() throws XMLStreamException {
		StringWriter stW = new StringWriter();
		GOMOutputWriter writer = new GOMStaxWriter(stW);
		GOMPersonImpl impl = new GOMPersonImpl();
		impl.writeRssOutput(writer);
		writer.flush();
		assertEquals(0, stW.toString().length());
	}

	public void testParseAtom() {
		GOMPersonImpl impl = new GOMPersonImpl();
		QName name = new QName(GOMNamespace.ATOM_NS_URI, "name");
		AtomParser childParser = impl.getChildParser(name);
		String nameValue = "simonw";
		{
			assertTrue(childParser instanceof GOMPersonImpl.NameParser);

			childParser.processElementValue(nameValue);
			childParser.processEndElement();
			assertEquals(impl.getName(), nameValue);
			try {
				childParser.processElementValue(nameValue);
				childParser.processEndElement();
				fail("duplicated element");

			} catch (GDataParseException e) {
				// 

			}
		}
		{
			name = new QName(GOMNamespace.ATOM_NS_URI, "uri");
			childParser = impl.getChildParser(name);
			assertTrue(childParser instanceof GOMPersonImpl.UriParser);

			childParser.processElementValue(nameValue);
			childParser.processEndElement();
			assertEquals(impl.getUri(), nameValue);

			try {
				childParser.processElementValue(nameValue);
				childParser.processEndElement();
				fail("duplicated element");

			} catch (GDataParseException e) {
				// 

			}

		}
		{
			name = new QName(GOMNamespace.ATOM_NS_URI, "email");
			childParser = impl.getChildParser(name);
			assertTrue(childParser instanceof GOMPersonImpl.EmailParser);

			childParser.processElementValue(nameValue);
			childParser.processEndElement();
			assertEquals(impl.getEmail(), nameValue);

			try {
				childParser.processElementValue(nameValue);
				childParser.processEndElement();
				fail("duplicated element");

			} catch (GDataParseException e) {
				// 

			}
		}

	}

	public void testProcessEndElement() {
		GOMPersonImpl impl = new GOMPersonImpl();
		try {
			impl.processEndElement();
			fail("name must be set");
		} catch (GDataParseException e) {
			// 
		}

	}

}
