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
import java.util.List;

import javax.xml.namespace.QName;
import javax.xml.stream.FactoryConfigurationError;
import javax.xml.stream.XMLStreamException;

import org.apache.lucene.gdata.gom.GOMAuthor;
import org.apache.lucene.gdata.gom.GOMCategory;
import org.apache.lucene.gdata.gom.GOMContributor;
import org.apache.lucene.gdata.gom.GOMElement;
import org.apache.lucene.gdata.gom.GOMEntry;
import org.apache.lucene.gdata.gom.GOMExtension;
import org.apache.lucene.gdata.gom.GOMFeed;
import org.apache.lucene.gdata.gom.GOMGenerator;
import org.apache.lucene.gdata.gom.GOMIcon;
import org.apache.lucene.gdata.gom.GOMId;
import org.apache.lucene.gdata.gom.GOMLink;
import org.apache.lucene.gdata.gom.GOMLogo;
import org.apache.lucene.gdata.gom.GOMNamespace;
import org.apache.lucene.gdata.gom.GOMRights;
import org.apache.lucene.gdata.gom.GOMSource;
import org.apache.lucene.gdata.gom.GOMSubtitle;
import org.apache.lucene.gdata.gom.GOMTitle;
import org.apache.lucene.gdata.gom.GOMUpdated;
import org.apache.lucene.gdata.gom.core.GOMFeedImplTest.TestExtendsionFactory;
import org.apache.lucene.gdata.gom.core.GOMFeedImplTest.TestExtension;
import org.apache.lucene.gdata.gom.core.extension.GOMExtensionFactory;
import org.apache.lucene.gdata.gom.writer.GOMOutputWriter;
import org.apache.lucene.gdata.gom.writer.GOMStaxWriter;

import junit.framework.TestCase;

public class GOMSourceImplTest extends TestCase {

	static final String TEST_LOCAL_NAME = "testelement";

	GOMSourceImpl impl;

	protected void setUp() throws Exception {
		this.impl = new GOMSourceImpl();
	}

	/*
	 * Test method for
	 * 'org.apache.lucene.gdata.gom.core.GOMFeedImpl.getChildParser(QName)'
	 */
	public void testGetChildParser() {

		{
			// atomAuthor*
			AtomParser parser = this.impl.getChildParser(new QName(
					GOMNamespace.ATOM_NS_URI, "author"));
			assertNotNull(parser);
			assertTrue(parser instanceof GOMAuthor);
			assertEquals(1, this.impl.getAuthors().size());
			this.impl.getChildParser(new QName(GOMNamespace.ATOM_NS_URI,
					"author"));
			assertEquals(2, this.impl.getAuthors().size());
		}

		{
			// atomCategory*
			AtomParser parser = this.impl.getChildParser(new QName(
					GOMNamespace.ATOM_NS_URI, "category"));
			assertNotNull(parser);
			assertTrue(parser instanceof GOMCategory);
			assertEquals(1, this.impl.getCategories().size());
			this.impl.getChildParser(new QName(GOMNamespace.ATOM_NS_URI,
					"category"));
			assertEquals(2, this.impl.getCategories().size());
		}

		{
			// atomContributor*
			AtomParser parser = this.impl.getChildParser(new QName(
					GOMNamespace.ATOM_NS_URI, "contributor"));
			assertNotNull(parser);
			assertTrue(parser instanceof GOMContributor);
			assertEquals(1, this.impl.getContributor().size());
			this.impl.getChildParser(new QName(GOMNamespace.ATOM_NS_URI,
					"contributor"));
			assertEquals(2, this.impl.getContributor().size());
		}
		{
			// atomGenerator?
			AtomParser parser = this.impl.getChildParser(new QName(
					GOMNamespace.ATOM_NS_URI, "generator"));
			assertNotNull(parser);
			assertTrue(parser instanceof GOMGenerator);
			assertSame(parser, this.impl.getGenerator());
			try {
				this.impl.getChildParser(new QName(GOMNamespace.ATOM_NS_URI,
						"generator"));
				fail("one or zero");
			} catch (GDataParseException e) {
				// 
			}
		}

		{
			// atomIcon?
			AtomParser parser = this.impl.getChildParser(new QName(
					GOMNamespace.ATOM_NS_URI, "icon"));
			assertNotNull(parser);
			assertTrue(parser instanceof GOMIcon);
			assertSame(parser, this.impl.getIcon());
			try {
				this.impl.getChildParser(new QName(GOMNamespace.ATOM_NS_URI,
						"icon"));
				fail("one or zero");
			} catch (GDataParseException e) {
				// 
			}

		}

		{
			// atomId
			AtomParser parser = this.impl.getChildParser(new QName(
					GOMNamespace.ATOM_NS_URI, "id"));
			assertNotNull(parser);
			assertTrue(parser instanceof GOMId);
			assertSame(parser, this.impl.getId());
			try {
				this.impl.getChildParser(new QName(GOMNamespace.ATOM_NS_URI,
						"id"));
				fail("exactly one time ");
			} catch (GDataParseException e) {
				// 
			}

		}

		{
			// atomLink*
			AtomParser parser = this.impl.getChildParser(new QName(
					GOMNamespace.ATOM_NS_URI, "link"));
			assertNotNull(parser);
			assertTrue(parser instanceof GOMLink);
			assertEquals(1, this.impl.getLinks().size());
			this.impl
					.getChildParser(new QName(GOMNamespace.ATOM_NS_URI, "link"));
			assertEquals(2, this.impl.getLinks().size());

		}

		{
			// atomLogo?
			AtomParser parser = this.impl.getChildParser(new QName(
					GOMNamespace.ATOM_NS_URI, "logo"));
			assertNotNull(parser);
			assertTrue(parser instanceof GOMLogo);
			assertSame(parser, this.impl.getLogo());
			try {
				this.impl.getChildParser(new QName(GOMNamespace.ATOM_NS_URI,
						"logo"));
				fail("zero or one");
			} catch (GDataParseException e) {
				// 
			}

		}

		{
			// atomRights?
			AtomParser parser = this.impl.getChildParser(new QName(
					GOMNamespace.ATOM_NS_URI, "rights"));
			assertNotNull(parser);
			assertTrue(parser instanceof GOMRights);
			assertSame(parser, this.impl.getRights());
			try {
				this.impl.getChildParser(new QName(GOMNamespace.ATOM_NS_URI,
						"rights"));
				fail("zero or one");
			} catch (GDataParseException e) {
				// 
			}

		}

		{
			// atomSubtitle?
			AtomParser parser = this.impl.getChildParser(new QName(
					GOMNamespace.ATOM_NS_URI, "subtitle"));
			assertNotNull(parser);
			assertTrue(parser instanceof GOMSubtitle);
			assertSame(parser, this.impl.getSubtitle());
			try {
				this.impl.getChildParser(new QName(GOMNamespace.ATOM_NS_URI,
						"subtitle"));
				fail("zero or one");
			} catch (GDataParseException e) {
				// 
			}

		}

		{
			// atomTitle
			AtomParser parser = this.impl.getChildParser(new QName(
					GOMNamespace.ATOM_NS_URI, "title"));
			assertNotNull(parser);
			assertTrue(parser instanceof GOMTitle);
			assertSame(parser, this.impl.getTitle());
			try {
				this.impl.getChildParser(new QName(GOMNamespace.ATOM_NS_URI,
						"title"));
				fail("exactly one time ");
			} catch (GDataParseException e) {
				// 
			}

		}

		{
			// atomUpdated
			AtomParser parser = this.impl.getChildParser(new QName(
					GOMNamespace.ATOM_NS_URI, "updated"));
			assertNotNull(parser);
			assertTrue(parser instanceof GOMUpdated);
			assertSame(parser, this.impl.getUpdated());
			try {
				this.impl.getChildParser(new QName(GOMNamespace.ATOM_NS_URI,
						"updated"));
				fail("exactly one time ");
			} catch (GDataParseException e) {
				// 
			}

		}

	}

	/*
	 * Test method for
	 * 'org.apache.lucene.gdata.gom.core.GOMFeedImpl.processElementValue(String)'
	 */
	public void testProcessElementValue() {
		try {
			this.impl.processElementValue("some");
			fail("no element text");
		} catch (GDataParseException e) {
			//
		}

	}

	/*
	 * Test method for
	 * 'org.apache.lucene.gdata.gom.core.GOMFeedImpl.processEndElement()'
	 * 
	 * atomFeed = element atom:feed { atomCommonAttributes, (atomAuthor* &
	 * atomCategory* & atomContributor* & atomGenerator? & atomIcon? & atomId &
	 * atomLink* & atomLogo? & atomRights? & atomSubtitle? & atomTitle &
	 * atomUpdated & extensionElement*), atomEntry* }
	 */
	public void testProcessEndElement() {
		this.impl.addAuthor(new GOMAuthorImpl());
		this.impl.setId(new GOMIdImpl());
		this.impl.setUpdated(new GOMUpdatedImpl());
		this.impl.setTitle(new GOMTitleImpl());

		this.impl.processEndElement();
		{
			// author missing
			this.impl.getAuthors().clear();
			try {
				this.impl.processEndElement();
				fail("missing elements");
			} catch (GDataParseException e) {
				// 
			}
			this.impl.addAuthor(new GOMAuthorImpl());
		}

		{
			// id missing
			this.impl.setId(null);
			try {
				this.impl.processEndElement();
				fail("missing elements");
			} catch (GDataParseException e) {
				// 
			}
			this.impl.setId(new GOMIdImpl());
		}

		{
			// title missing
			this.impl.setTitle(null);
			try {
				this.impl.processEndElement();
				fail("missing elements");
			} catch (GDataParseException e) {
				// 
			}
			this.impl.setTitle(new GOMTitleImpl());
		}
		{
			// updated missing
			this.impl.setUpdated(null);
			try {
				this.impl.processEndElement();
				fail("missing elements");
			} catch (GDataParseException e) {
				// 
			}
			this.impl.setUpdated(new GOMUpdatedImpl());
		}

		/*
		 * atom:feed elements MUST NOT contain more than one atom:link element
		 * with a rel attribute value of "alternate" that has the same
		 * combination of type and hreflang attribute values.
		 */

		{
			// two identical alternate links missing
			GOMLink link = new GOMLinkImpl();
			link.setRel("alternate");
			link.setHrefLang("http://www.apache.org");
			link.setType("text/html");
			this.impl.addLink(link);
			// one is allowed
			this.impl.processEndElement();
			// add a second link
			link = new GOMLinkImpl();
			this.impl.addLink(link);
			link.setRel("next");
			link.setHrefLang("http://www.apache.org");
			link.setType("text/html");
			// one is alternate the other is next
			this.impl.processEndElement();

			// a second "identical" alternate link
			link = new GOMLinkImpl();
			this.impl.addLink(link);
			link.setRel("alternate");
			link.setHrefLang("http://www.apache.org");
			link.setType("text/html");
			try {
				this.impl.processEndElement();
				fail("missing elements");
			} catch (GDataParseException e) {
				// 
			}
			this.impl.setUpdated(new GOMUpdatedImpl());
		}

	}

	/*
	 * Test method for
	 * 'org.apache.lucene.gdata.gom.core.GOMFeedImpl.GOMFeedImpl()'
	 */
	public void testGOMFeedImpl() {
		GOMFeedImpl impl2 = new GOMFeedImpl();
		assertEquals(GOMFeed.LOCALNAME, impl2.getLocalName());
		assertEquals(GOMFeed.LOCALNAME, impl2.getQname().getLocalPart());
		assertEquals(GOMNamespace.ATOM_NS_URI, impl2.getQname()
				.getNamespaceURI());
	}

	/*
	 * Test method for
	 * 'org.apache.lucene.gdata.gom.core.GOMFeedImpl.writeAtomOutput(GOMWriter)'
	 */
	public void testWriteAtomOutput() throws XMLStreamException,
			FactoryConfigurationError {
		// write a whole feed and check if all elements are written
		this.impl.addAuthor(new GOMAuthorImpl());
		this.impl.addCategory(new GOMCategoryImpl());
		this.impl.addContributor(new GOMContributorImpl());
		this.impl.addLink(new GOMLinkImpl());
		this.impl.setGenerator(new GOMGeneratorImpl());
		this.impl.setIcon(new GOMIconImpl());
		this.impl.setId(new GOMIdImpl());
		this.impl.setLogo(new GOMLogoImpl());
		this.impl.setRights(new GOMRightsImpl());
		this.impl.setSubtitle(new GOMSubtitleImpl());
		this.impl.setTitle(new GOMTitleImpl());
		this.impl.setUpdated(new GOMUpdatedImpl());
		StringWriter stW = new StringWriter();
		GOMOutputWriter writer = new GOMStaxWriter(stW);
		this.impl.writeAtomOutput(writer);
		String string = stW.toString();
		assertTrue(string.startsWith("<" + GOMSource.LOCALNAME));
		assertTrue(string.contains("<" + GOMAuthor.LOCALNAME));
		assertTrue(string.contains("<" + GOMCategory.LOCALNAME));
		assertTrue(string.contains("<" + GOMContributor.LOCALNAME));
		assertTrue(string.contains("<" + GOMLink.LOCALNAME));
		assertTrue(string.contains("<" + GOMGenerator.LOCALNAME));
		assertTrue(string.contains("<" + GOMIcon.LOCALNAME));
		assertTrue(string.contains("<" + GOMId.LOCALNAME));
		assertTrue(string.contains("<" + GOMLogo.LOCALNAME));
		assertTrue(string.contains("<" + GOMRights.LOCALNAME));
		assertTrue(string.contains("<" + GOMSubtitle.LOCALNAME));
		assertTrue(string.contains("<" + GOMTitle.LOCALNAME));
		assertTrue(string.contains("<" + GOMUpdated.LOCALNAME));
	}

	/*
	 * Test method for
	 * 'org.apache.lucene.gdata.gom.core.GOMFeedImpl.writeRssOutput(GOMWriter)'
	 */
	public void testWriteRssOutput() throws XMLStreamException,
			FactoryConfigurationError {

		StringWriter stW = new StringWriter();
		GOMOutputWriter writer = new GOMStaxWriter(stW);
		this.impl.writeRssOutput(writer);
		assertEquals("", stW.toString());
	}

}
