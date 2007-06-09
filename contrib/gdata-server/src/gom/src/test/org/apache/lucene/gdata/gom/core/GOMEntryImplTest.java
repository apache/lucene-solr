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

import org.apache.lucene.gdata.gom.AtomMediaType;
import org.apache.lucene.gdata.gom.GOMAuthor;
import org.apache.lucene.gdata.gom.GOMCategory;
import org.apache.lucene.gdata.gom.GOMContent;
import org.apache.lucene.gdata.gom.GOMContributor;
import org.apache.lucene.gdata.gom.GOMEntry;
import org.apache.lucene.gdata.gom.GOMExtension;
import org.apache.lucene.gdata.gom.GOMGenerator;
import org.apache.lucene.gdata.gom.GOMIcon;
import org.apache.lucene.gdata.gom.GOMId;
import org.apache.lucene.gdata.gom.GOMLink;
import org.apache.lucene.gdata.gom.GOMLogo;
import org.apache.lucene.gdata.gom.GOMNamespace;
import org.apache.lucene.gdata.gom.GOMPublished;
import org.apache.lucene.gdata.gom.GOMRights;
import org.apache.lucene.gdata.gom.GOMSource;
import org.apache.lucene.gdata.gom.GOMSubtitle;
import org.apache.lucene.gdata.gom.GOMSummary;
import org.apache.lucene.gdata.gom.GOMTitle;
import org.apache.lucene.gdata.gom.GOMUpdated;
import org.apache.lucene.gdata.gom.core.GOMFeedImplTest.TestExtendsionFactory;
import org.apache.lucene.gdata.gom.core.GOMFeedImplTest.TestExtension;
import org.apache.lucene.gdata.gom.writer.GOMOutputWriter;
import org.apache.lucene.gdata.gom.writer.GOMStaxWriter;

import junit.framework.TestCase;

public class GOMEntryImplTest extends TestCase {
	private static final String TEST_LOCAL_NAME = GOMFeedImplTest.TEST_LOCAL_NAME;

	private GOMEntryImpl impl;

	protected void setUp() throws Exception {
		super.setUp();
		this.impl = new GOMEntryImpl();
	}

	public void testSetNamespace() {
		assertEquals(0, this.impl.getNamespaces().size());
		assertNotNull(this.impl.getDefaultNamespace());
		this.impl.addNamespace(GOMNamespace.ATOM_NAMESPACE);
		assertSame(GOMNamespace.ATOM_NAMESPACE, this.impl.getDefaultNamespace());
		this.impl.addNamespace(GOMNamespace.OPENSEARCH_NAMESPACE);
		assertEquals(1, this.impl.getNamespaces().size());
		assertSame(GOMNamespace.OPENSEARCH_NAMESPACE, this.impl.getNamespaces()
				.get(0));

		// detect defaul ns
		this.impl.addNamespace(new GOMNamespace(GOMNamespace.ATOM_NS_URI, ""));
		assertEquals(1, this.impl.getNamespaces().size());
	}

	/*
	 * Test method for
	 * 'org.apache.lucene.gdata.gom.core.GOMEntryImpl.getChildParser(QName)'
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

		{
			// atomSource?
			AtomParser parser = this.impl.getChildParser(new QName(
					GOMNamespace.ATOM_NS_URI, "source"));
			assertNotNull(parser);
			assertTrue(parser instanceof GOMSource);
			assertEquals(parser, this.impl.getSource());
			try {
				this.impl.getChildParser(new QName(GOMNamespace.ATOM_NS_URI,
						"source"));
				fail("duplicated element");
			} catch (GDataParseException e) {
				//
			}

		}

		{
			// atomSummary?
			AtomParser parser = this.impl.getChildParser(new QName(
					GOMNamespace.ATOM_NS_URI, "summary"));
			assertNotNull(parser);
			assertTrue(parser instanceof GOMSummary);
			assertEquals(parser, this.impl.getSummary());
			try {
				this.impl.getChildParser(new QName(GOMNamespace.ATOM_NS_URI,
						"summary"));
				fail("duplicated element");
			} catch (GDataParseException e) {
				//
			}

		}

		{
			// atomContent?
			AtomParser parser = this.impl.getChildParser(new QName(
					GOMNamespace.ATOM_NS_URI, "content"));
			assertNotNull(parser);
			assertTrue(parser instanceof GOMContent);
			assertEquals(parser, this.impl.getContent());
			try {
				this.impl.getChildParser(new QName(GOMNamespace.ATOM_NS_URI,
						"content"));
				fail("duplicated element");
			} catch (GDataParseException e) {
				//
			}

		}

		{
			// atomContent?
			AtomParser parser = this.impl.getChildParser(new QName(
					GOMNamespace.ATOM_NS_URI, "published"));
			assertNotNull(parser);
			assertTrue(parser instanceof GOMPublished);
			assertEquals(parser, this.impl.getPublished());
			try {
				this.impl.getChildParser(new QName(GOMNamespace.ATOM_NS_URI,
						"published"));
				fail("duplicated element");
			} catch (GDataParseException e) {
				//
			}

		}

	}

	/*
	 * Test method for
	 * 'org.apache.lucene.gdata.gom.core.GOMEntryImpl.processEndElement()'
	 */
	public void testProcessEndElement() {
		try {
			this.impl.processEndElement();
			fail("missing elements");
		} catch (GDataParseException e) {
			// 
		}

		// atom:entry elements MUST contain exactly one atom:id element.
		this.impl.setId(new GOMIdImpl());
		/*
		 * atom:entry elements that contain no child atom:content element MUST
		 * contain at least one atom:link element with a rel attribute value of
		 * "alternate".
		 */
		GOMLink link = new GOMLinkImpl();
		link.setRel("alternate");
		this.impl.addLink(link);
		/*
		 * atom:entry elements MUST contain exactly one atom:title element.
		 */
		this.impl.setTitle(new GOMTitleImpl());
		/*
		 * atom:entry elements MUST contain exactly one atom:updated element.
		 */
		this.impl.setUpdated(new GOMUpdatedImpl());

		{
			this.impl.setId(null);
			try {
				this.impl.processEndElement();
				fail("id is missing");
			} catch (GDataParseException e) {
				// 
			}
			this.impl.setId(new GOMIdImpl());
		}

		{
			this.impl.getLinks().clear();
			try {
				this.impl.processEndElement();
				fail("link alternate is missing");
			} catch (GDataParseException e) {
				// 
			}
			this.impl.setContent(new GOMContentImpl());
			this.impl.processEndElement();
			this.impl.setContent(null);
			this.impl.addLink(link);
		}

		{
			this.impl.setTitle(null);
			try {
				this.impl.processEndElement();
				fail("title is missing");
			} catch (GDataParseException e) {
				// 
			}
			this.impl.setTitle(new GOMTitleImpl());
		}
		{
			this.impl.setUpdated(null);
			try {
				this.impl.processEndElement();
				fail("Updated is missing");
			} catch (GDataParseException e) {
				// 
			}
			this.impl.setUpdated(new GOMUpdatedImpl());
		}

		/*
		 * atom:entry elements MUST NOT contain more than one atom:link element
		 * with a rel attribute value of "alternate" that has the same
		 * combination of type and hreflang attribute values.
		 */
		link.setType("test");
		link.setHrefLang("http://www.apache.org");
		this.impl.addLink(link);
		try {
			this.impl.processEndElement();
			fail("doulbe alternate link with same type and hreflang");

		} catch (GDataParseException e) {
			// 
		}
		this.impl.getLinks().remove(0);
		/*
		 * # atom:entry elements MUST contain an atom:summary element in either
		 * of the following cases:
		 * 
		 * the atom:entry contains an atom:content that has a "src" attribute
		 * (and is thus empty). the atom:entry contains content that is encoded
		 * in Base64; i.e., the "type" attribute of atom:content is a MIME media
		 * type [MIMEREG], but is not an XML media type [RFC3023], does not
		 * begin with "text/", and does not end with "/xml" or "+xml".
		 * 
		 * 
		 */
		GOMContent c = new GOMContentImpl();
		c.setSrc("");
		this.impl.setContent(c);
		try {
			this.impl.processEndElement();
			fail("no summary");
		} catch (GDataParseException e) {
			// 
		}
		c.setSrc(null);
		c.setAtomMediaType(AtomMediaType.BINARY);
		try {
			this.impl.processEndElement();
			fail("no summary");
		} catch (GDataParseException e) {
			// 
		}

	}

	/*
	 * Test method for
	 * 'org.apache.lucene.gdata.gom.core.GOMEntryImpl.GOMEntryImpl()'
	 */
	public void testGOMEntryImpl() {
		GOMEntryImpl impl2 = new GOMEntryImpl();
		assertNotNull(impl2.getQname());
		assertEquals(GOMEntry.LOCALNAME, impl.getQname().getLocalPart());
		assertEquals(GOMEntry.LOCALNAME, this.impl.getLocalName());
		assertEquals(GOMNamespace.ATOM_NS_URI, impl.getQname()
				.getNamespaceURI());
	}

	/*
	 * Test method for
	 * 'org.apache.lucene.gdata.gom.core.GOMEntryImpl.writeAtomOutput(GOMOutputWriter)'
	 */
	public void testWriteAtomOutput() throws XMLStreamException,
			FactoryConfigurationError {
		// write a whole feed and check if all elements are written
		this.impl.addAuthor(new GOMAuthorImpl());
		this.impl.addCategory(new GOMCategoryImpl());
		this.impl.addContributor(new GOMContributorImpl());
		this.impl.addLink(new GOMLinkImpl());
		this.impl.setContent(new GOMContentImpl());
		this.impl.setId(new GOMIdImpl());
		this.impl.setRights(new GOMRightsImpl());
		this.impl.setSummary(new GOMSummaryImpl());
		this.impl.setTitle(new GOMTitleImpl());
		this.impl.setUpdated(new GOMUpdatedImpl());
		this.impl.setSource(new GOMSourceImpl());
		this.impl.setPublished(new GOMPublishedImpl());
		this.impl.extensions.add(new GOMFeedImplTest.TestExtension());
		StringWriter stW = new StringWriter();
		GOMOutputWriter writer = new GOMStaxWriter(stW);
		this.impl.writeAtomOutput(writer);
		String string = stW.toString();
		assertTrue(string.contains("xmlns=\"http://www.w3.org/2005/Atom\""));
		assertTrue(string.startsWith("<" + GOMEntry.LOCALNAME));
		assertTrue(string.contains("<" + GOMAuthor.LOCALNAME));
		assertTrue(string.contains("<" + GOMCategory.LOCALNAME));
		assertTrue(string.contains("<" + GOMContributor.LOCALNAME));
		assertTrue(string.contains("<" + GOMLink.LOCALNAME));
		assertTrue(string.contains("<" + GOMId.LOCALNAME));
		assertTrue(string.contains("<" + GOMRights.LOCALNAME));
		assertTrue(string.contains("<" + GOMSummary.LOCALNAME));
		assertTrue(string.contains("<" + GOMContent.LOCALNAME));
		assertTrue(string.contains("<" + GOMTitle.LOCALNAME));
		assertTrue(string.contains("<" + GOMUpdated.LOCALNAME));
		assertTrue(string.contains("<" + GOMSource.LOCALNAME));
		assertTrue(string.contains("<" + GOMPublished.LOCALNAME));
		assertTrue(string.contains("<test"));
		assertTrue(string.endsWith("</entry>"));
	}

	/*
	 * Test method for
	 * 'org.apache.lucene.gdata.gom.core.GOMEntryImpl.writeRssOutput(GOMOutputWriter)'
	 */
	public void testWriteRssOutputGOMOutputWriter() throws XMLStreamException,
			FactoryConfigurationError {
		// write a whole feed and check if all elements are written
		this.impl.addAuthor(new GOMAuthorImpl());
		this.impl.addCategory(new GOMCategoryImpl());
		this.impl.addContributor(new GOMContributorImpl());
		GOMLink link = new GOMLinkImpl();
		link.setRel("enclosure");
		link.setHref("test");
		link.setType("testType");
		this.impl.addLink(link);
		this.impl.setContent(new GOMContentImpl());
		this.impl.setId(new GOMIdImpl());
		this.impl.setRights(new GOMRightsImpl());
		GOMSummaryImpl summ = new GOMSummaryImpl();
		summ.xmlLang = "de";
		this.impl.setSummary(summ);
		this.impl.setTitle(new GOMTitleImpl());
		this.impl.setUpdated(new GOMUpdatedImpl());
		this.impl.setSource(new GOMSourceImpl());
		this.impl.setPublished(new GOMPublishedImpl());
		this.impl.extensions.add(new GOMFeedImplTest.TestExtension());
		StringWriter stW = new StringWriter();
		GOMOutputWriter writer = new GOMStaxWriter(stW);
		this.impl.writeRssOutput(writer);
		String string = stW.toString();
		assertTrue(string
				.contains("xmlns:atom=\"http://www.w3.org/2005/Atom\""));
		assertTrue(string.startsWith("<" + GOMEntry.LOCALNAME_RSS));
		assertTrue(string.contains("<" + GOMId.LOCALNAME_RSS));
		assertTrue(string.contains("<pubDate"));
		assertTrue(string.contains("<atom:" + GOMUpdated.LOCALNAME));
		assertTrue(string.contains("<" + GOMId.LOCALNAME_RSS));
		assertTrue(string.contains("<language"));
		assertTrue(string.contains("<category domain=\""));
		assertTrue(string.contains("<title"));
		assertTrue(string.contains("<atom:summary"));
		assertTrue(string.contains("<description"));
		// a link element
		assertTrue(string.contains("<enclosure"));
		assertTrue(string.contains("<author"));
		assertTrue(string.contains("<atom:test"));
		assertTrue(string.endsWith("</item>"));

	}

	/*
	 * Test method for
	 * 'org.apache.lucene.gdata.gom.core.GOMEntryImpl.getExtensions()'
	 */
	public void testGetExtensions() {

		{
			List<GOMExtension> extensions = this.impl.getExtensions();
			assertNotNull(extensions);
			assertEquals(0, extensions.size());
		}
		QName name = new QName(TEST_LOCAL_NAME);
		this.impl.setExtensionFactory(new TestExtendsionFactory());

		AtomParser childParser = this.impl.getChildParser(name);
		assertTrue(childParser instanceof TestExtension);
		List<GOMExtension> extensions = this.impl.getExtensions();
		assertNotNull(extensions);
		assertEquals(1, extensions.size());
		assertSame(childParser, extensions.get(0));
	}

	/*
	 * Test method for
	 * 'org.apache.lucene.gdata.gom.core.GOMEntryImpl.setExtensionFactory(GOMExtensionFactory)'
	 */
	public void testSetExtensionFactory() {
		QName name = new QName(TEST_LOCAL_NAME);
		try {
			this.impl.getChildParser(name);
			fail("no child hander for this qname");
		} catch (GDataParseException e) {
			// 
		}

		this.impl.setExtensionFactory(new TestExtendsionFactory());

		AtomParser childParser = this.impl.getChildParser(name);
		assertTrue(childParser instanceof TestExtension);

	}

}
