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

import java.util.LinkedList;
import java.util.List;

import javax.xml.namespace.QName;
import javax.xml.stream.XMLStreamException;

import org.apache.lucene.gdata.gom.GOMAttribute;
import org.apache.lucene.gdata.gom.GOMAuthor;
import org.apache.lucene.gdata.gom.GOMCategory;
import org.apache.lucene.gdata.gom.GOMEntry;
import org.apache.lucene.gdata.gom.GOMExtension;
import org.apache.lucene.gdata.gom.GOMFeed;
import org.apache.lucene.gdata.gom.GOMLink;
import org.apache.lucene.gdata.gom.GOMNamespace;
import org.apache.lucene.gdata.gom.core.extension.GOMExtensionFactory;
import org.apache.lucene.gdata.gom.core.utils.GOMUtils;
import org.apache.lucene.gdata.gom.writer.GOMOutputWriter;

/**
 * atom:feed { atomCommonAttributes, (atomAuthor* & atomCategory* &
 * atomContributor* & atomGenerator? & atomIcon? & atomId & atomLink* &
 * atomLogo? & atomRights? & atomSubtitle? & atomTitle & atomUpdated &
 * extensionElement*), atomEntry* }
 * 
 * @author Simon Willnauer
 */
class GOMFeedImpl extends GOMSourceImpl implements GOMFeed {
	// TODO add totalResults OS namespace

	static final int DEFAULT_START_INDEX = 1;

	static final int DEFAULT_ITEMS_PER_PAGE = 25;

	private static final GOMAttribute RSS_VERSION_ATTRIBUTE = new GOMAttributeImpl(
			"version", "2.0");

	protected List<GOMEntry> entries = new LinkedList<GOMEntry>();

	protected List<GOMExtension> extensions = new LinkedList<GOMExtension>();

	protected List<GOMNamespace> namespaces = new LinkedList<GOMNamespace>();

	private SimpleGOMElementImpl startIndexElement;

	private SimpleGOMElementImpl itemsPerPageElement;

	private GOMExtensionFactory extensionFactory;

	private GOMNamespace defaultNamespace = GOMNamespace.ATOM_NAMESPACE;

	GOMFeedImpl() {
		this.localName = GOMFeed.LOCALNAME;
		this.qname = new QName(GOMNamespace.ATOM_NS_URI, this.localName);
		startIndexElement = new SimpleGOMElementImpl(
				SimpleGOMElementImpl.ELEMENT_OS_START_INDEX,
				GOMNamespace.OPENSEARCH_NAMESPACE);
		itemsPerPageElement = new SimpleGOMElementImpl(
				SimpleGOMElementImpl.ELEMENT_OS_ITEMS_PER_PAGE,
				GOMNamespace.OPENSEARCH_NAMESPACE);
		itemsPerPageElement.setTextValue(Integer
				.toString(DEFAULT_ITEMS_PER_PAGE));
		startIndexElement.setTextValue(Integer.toString(DEFAULT_START_INDEX));
	}

	/**
	 * @see org.apache.lucene.gdata.gom.GOMSource#addEntry(org.apache.lucene.gdata.gom.GOMEntry)
	 */
	public void addEntry(GOMEntry aEntry) {
		if (aEntry != null)
			this.entries.add(aEntry);

	}

	/**
	 * @see org.apache.lucene.gdata.gom.GOMFeed#getEntries()
	 */
	public List<GOMEntry> getEntries() {
		return this.entries;
	}

	/**
	 * @see org.apache.lucene.gdata.gom.GOMFeed#getItemsPerPage()
	 */
	public int getItemsPerPage() {
		return Integer.parseInt(this.itemsPerPageElement.getTextValue());
	}

	/**
	 * @see org.apache.lucene.gdata.gom.GOMFeed#getStartIndex()
	 */
	public int getStartIndex() {
		return Integer.parseInt(this.startIndexElement.getTextValue());
	}

	/**
	 * @see org.apache.lucene.gdata.gom.GOMFeed#setStartIndex(int)
	 */
	public void setStartIndex(int aIndex) {
		if (aIndex < 1)
			return;
		this.startIndexElement.textValue = Integer.toString(aIndex);
	}

	/**
	 * @see org.apache.lucene.gdata.gom.GOMFeed#setItemsPerPage(int)
	 */
	public void setItemsPerPage(int aInt) {
		if (aInt < 0)
			return;
		this.itemsPerPageElement.textValue = Integer.toString(aInt);
	}

	/**
	 * @see org.apache.lucene.gdata.gom.GOMFeed#addNamespace(org.apache.lucene.gdata.gom.GOMNamespace)
	 */
	public void addNamespace(GOMNamespace aNamespace) {
		if (aNamespace == null)
			return;
		// namespace overrides hash / equals
		if (this.namespaces.contains(aNamespace))
			return;
		if ("".equals(aNamespace.getNamespacePrefix())
				|| aNamespace.getNamespaceUri()
						.equals(GOMNamespace.ATOM_NS_URI))
			return;
		else
			this.namespaces.add(aNamespace);
	}

	/**
	 * @see org.apache.lucene.gdata.gom.GOMFeed#getDefaultNamespace()
	 */
	public GOMNamespace getDefaultNamespace() {
		return this.defaultNamespace;
	}

	/**
	 * @see org.apache.lucene.gdata.gom.GOMFeed#getNamespaces()
	 * 
	 */
	public List<GOMNamespace> getNamespaces() {
		return this.namespaces;
	}

	/**
	 * @see org.apache.lucene.gdata.gom.core.AtomParser#getChildParser(javax.xml.namespace.QName)
	 */
	public AtomParser getChildParser(QName aName) {
		if (aName.getNamespaceURI().equals(GOMNamespace.OPENSEARCH_NS_URI)) {
			if (aName.getLocalPart().equals(
					SimpleGOMElementImpl.ELEMENT_OS_ITEMS_PER_PAGE)) {

				this.itemsPerPageElement = new SimpleGOMElementImpl(
						SimpleGOMElementImpl.ELEMENT_OS_ITEMS_PER_PAGE,
						GOMNamespace.OPENSEARCH_NAMESPACE);
				this.itemsPerPageElement
						.setValidator(new PositiveIntegerValidator(
								SimpleGOMElementImpl.ELEMENT_OS_ITEMS_PER_PAGE));
				return this.itemsPerPageElement;
			}
			if (aName.getLocalPart().equals(
					SimpleGOMElementImpl.ELEMENT_OS_START_INDEX)) {
				this.startIndexElement = new SimpleGOMElementImpl(
						SimpleGOMElementImpl.ELEMENT_OS_START_INDEX,
						GOMNamespace.OPENSEARCH_NAMESPACE);
				this.startIndexElement
						.setValidator(new PositiveIntegerValidator(
								SimpleGOMElementImpl.ELEMENT_OS_START_INDEX));
				return this.startIndexElement;
			}

		}
		if (aName.getNamespaceURI().equals(GOMNamespace.ATOM_NS_URI)
				&& aName.getLocalPart().equals(GOMEntry.LOCALNAME)) {
			GOMEntry entry = new GOMEntryImpl();
			this.entries.add(entry);
			return entry;

		}
		if (this.extensionFactory != null) {
			GOMExtension extension = this.extensionFactory
					.canHandleExtensionElement(aName);
			if (extension != null) {
				this.extensions.add(extension);
				return extension;
			}
		}
		return super.getChildParser(aName);

	}

	/**
	 * @see org.apache.lucene.gdata.gom.GOMElement#writeAtomOutput(org.apache.lucene.gdata.gom.writer.GOMStaxWriter)
	 */
	public void writeAtomOutput(GOMOutputWriter aStreamWriter)
			throws XMLStreamException {
		aStreamWriter.writeStartElement(this.localName,
				this.extensionAttributes);
		if (this.defaultNamespace != null)
			aStreamWriter.writeDefaultNamespace(this.defaultNamespace);
		for (GOMNamespace namespace : this.namespaces) {
			aStreamWriter.writeNamespace(namespace);
		}
		List<GOMAttribute> xmlNamespaceAttributes = getXmlNamespaceAttributes();
		for (GOMAttribute attribute : xmlNamespaceAttributes) {
			aStreamWriter.writeAttribute(attribute);
		}
		writeInnerAtomOutput(aStreamWriter);
		if (this.itemsPerPageElement != null)
			this.itemsPerPageElement.writeAtomOutput(aStreamWriter);
		if (this.startIndexElement != null)
			this.startIndexElement.writeAtomOutput(aStreamWriter);
		for (GOMExtension extension : this.extensions) {
			extension.writeAtomOutput(aStreamWriter);
		}
		for (GOMEntry entry : this.entries) {
			entry.writeAtomOutput(aStreamWriter);
		}

		aStreamWriter.writeEndElement();

	}

	/**
	 * @see org.apache.lucene.gdata.gom.GOMElement#writeRssOutput(org.apache.lucene.gdata.gom.writer.GOMStaxWriter)
	 */
	public void writeRssOutput(GOMOutputWriter aStreamWriter)
			throws XMLStreamException {
		aStreamWriter
				.writeStartElement(LOCALNAME_RSS, this.extensionAttributes);
		List<GOMAttribute> xmlNamespaceAttributes = getXmlNamespaceAttributes();
		for (GOMAttribute attribute : xmlNamespaceAttributes) {
			aStreamWriter.writeAttribute(attribute);
		}
		if (this.defaultNamespace != null)
			aStreamWriter.writeNamespace(this.defaultNamespace);
		for (GOMNamespace namespace : this.namespaces) {
			aStreamWriter.writeNamespace(namespace);
		}
		aStreamWriter.writeAttribute(RSS_VERSION_ATTRIBUTE);

		aStreamWriter.writeStartElement(RSS_CHANNEL_ELEMENT_NAME);

		if (this.id != null)
			this.id.writeRssOutput(aStreamWriter);
		if (this.title != null)
			this.title.writeRssOutput(aStreamWriter);
		if (this.subtitle != null)
			this.subtitle.writeRssOutput(aStreamWriter);
		if (this.rights != null)
			this.rights.writeRssOutput(aStreamWriter);
		for (GOMAuthor authors : this.authors) {
			authors.writeRssOutput(aStreamWriter, "managingEditor");
		}
		for (GOMCategory category : this.categories) {
			category.writeRssOutput(aStreamWriter);
		}
		for (GOMLink link : this.links) {
			link.writeRssOutput(aStreamWriter);
		}
		if (this.updated != null) {
			// udated.getDate can not be null
			aStreamWriter.writeSimpleXMLElement("lastBuildDate", GOMUtils
					.buildRfc822Date(this.updated.getDate().getTime()), null);
		}

		if (this.logo != null || this.icon != null) {
			aStreamWriter.writeStartElement("image");
			if (this.logo != null)
				this.logo.writeRssOutput(aStreamWriter);
			else
				this.icon.writeRssOutput(aStreamWriter);
			aStreamWriter.writeEndElement();

		}

		if (this.generator != null)
			this.generator.writeRssOutput(aStreamWriter);
		if (this.itemsPerPageElement != null)
			this.itemsPerPageElement.writeRssOutput(aStreamWriter);
		if (this.startIndexElement != null)
			this.startIndexElement.writeRssOutput(aStreamWriter);
		for (GOMExtension extension : this.extensions) {
			extension.writeRssOutput(aStreamWriter);
		}
		for (GOMExtension extension : this.extensions) {
			extension.writeRssOutput(aStreamWriter);
		}
		for (GOMEntry entry : this.entries) {
			entry.writeRssOutput(aStreamWriter);
		}
		// channel
		aStreamWriter.writeEndElement();
		// rss
		aStreamWriter.writeEndElement();

	}

	static class PositiveIntegerValidator extends
			SimpleGOMElementImpl.SimpleValidator {

		protected PositiveIntegerValidator(String aLocalName) {
			super(aLocalName);

		}

		/**
		 * @see org.apache.lucene.gdata.gom.core.SimpleGOMElementImpl.SimpleValidator#validate(java.lang.String)
		 */
		@Override
		protected void validate(String aTextValue) {
			super.validate(aTextValue);
			try {
				int i = Integer.parseInt(aTextValue);
				if (i < 0)
					throw new GDataParseException(String.format(
							AtomParser.INVALID_ELEMENT_VALUE, this.localName,
							"positive integer value"));
			} catch (NumberFormatException e) {
				throw new GDataParseException(String.format(
						AtomParser.INVALID_ELEMENT_VALUE, this.localName,
						"positive integer value"));
			}

		}

	}

	/**
	 * @see org.apache.lucene.gdata.gom.GOMFeed#getExtensions()
	 */
	public List<GOMExtension> getExtensions() {
		return this.extensions;
	}

	/**
	 * @see org.apache.lucene.gdata.gom.GOMFeed#setExtensionFactory(org.apache.lucene.gdata.gom.core.extension.GOMExtensionFactory)
	 */
	public void setExtensionFactory(GOMExtensionFactory aFactory) {
		if (extensionFactory != null) {
			List<GOMNamespace> namespaces2 = extensionFactory.getNamespaces();
			if (namespaces2 != null)
				for (GOMNamespace namespace : namespaces2) {
					this.addNamespace(namespace);
				}

		}

		this.extensionFactory = aFactory;

	}

	/**
	 * @see org.apache.lucene.gdata.gom.GOMFeed#addLink(org.apache.lucene.gdata.gom.GOMLink)
	 */
	public void addLink(GOMLink aLink) {
		if (aLink == null)
			return;
		this.links.add(aLink);

	}

}
