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

import org.apache.lucene.gdata.gom.AtomMediaType;
import org.apache.lucene.gdata.gom.GOMAuthor;
import org.apache.lucene.gdata.gom.GOMCategory;
import org.apache.lucene.gdata.gom.GOMContent;
import org.apache.lucene.gdata.gom.GOMContributor;
import org.apache.lucene.gdata.gom.GOMEntry;
import org.apache.lucene.gdata.gom.GOMExtension;
import org.apache.lucene.gdata.gom.GOMId;
import org.apache.lucene.gdata.gom.GOMLink;
import org.apache.lucene.gdata.gom.GOMNamespace;
import org.apache.lucene.gdata.gom.GOMPublished;
import org.apache.lucene.gdata.gom.GOMRights;
import org.apache.lucene.gdata.gom.GOMSource;
import org.apache.lucene.gdata.gom.GOMSummary;
import org.apache.lucene.gdata.gom.GOMTitle;
import org.apache.lucene.gdata.gom.GOMUpdated;
import org.apache.lucene.gdata.gom.core.extension.GOMExtensionFactory;
import org.apache.lucene.gdata.gom.core.utils.AtomParserUtils;
import org.apache.lucene.gdata.gom.writer.GOMOutputWriter;

/**
 * 
 * The default implementation of {@link org.apache.lucene.gdata.gom.GOMEntry}
 * 
 * <pre>
 *         atomEntry =
 *         element atom:entry {
 *         atomCommonAttributes,
 *         (	atomAuthor*
 *         	&amp; atomCategory*
 *         	&amp; atomContent?
 *         	&amp; atomContributor*
 *         	&amp; atomId
 *         	&amp; atomLink*
 *         	&amp; atomPublished?
 *       	 	&amp; atomRights?
 *         	&amp; atomSource?
 *         	&amp; atomSummary?
 *         	&amp; atomTitle
 *         	&amp; atomUpdated
 *         	&amp; extensionElement*)
 *         }
 * </pre>
 * 
 * @author Simon Willnauer
 * 
 */
public class GOMEntryImpl extends AbstractGOMElement implements GOMEntry {

	protected List<GOMNamespace> namespaces = new LinkedList<GOMNamespace>();

	protected List<GOMExtension> extensions = new LinkedList<GOMExtension>();

	private List<GOMAuthor> authors = new LinkedList<GOMAuthor>();

	private List<GOMCategory> categories = new LinkedList<GOMCategory>();

	private List<GOMContributor> contributors = new LinkedList<GOMContributor>();

	private GOMId id;

	private List<GOMLink> links = new LinkedList<GOMLink>();

	private GOMPublished published;

	private GOMRights rights;

	private GOMSource source;

	private GOMSummary summary;

	private GOMTitle title;

	private GOMUpdated updated;

	private GOMExtensionFactory extensionFactory;

	private GOMContent content;

	private final GOMNamespace defaultNamespace = GOMNamespace.ATOM_NAMESPACE;

	/**
	 * 
	 */
	public GOMEntryImpl() {
		super();
		this.localName = GOMEntry.LOCALNAME;
		this.qname = new QName(GOMNamespace.ATOM_NS_URI, this.localName);

	}

	/**
	 * @see org.apache.lucene.gdata.gom.GOMEntry#addAuthor(org.apache.lucene.gdata.gom.GOMAuthor)
	 */
	public void addAuthor(GOMAuthor aAuthor) {
		if (aAuthor != null)
			this.authors.add(aAuthor);

	}

	/**
	 * @see org.apache.lucene.gdata.gom.GOMEntry#addCategory(org.apache.lucene.gdata.gom.GOMCategory)
	 */
	public void addCategory(GOMCategory aCategory) {
		if (aCategory != null)
			this.categories.add(aCategory);

	}

	/**
	 * @see org.apache.lucene.gdata.gom.GOMEntry#addContributor(org.apache.lucene.gdata.gom.GOMContributor)
	 */
	public void addContributor(GOMContributor aContributor) {
		if (aContributor != null)
			this.contributors.add(aContributor);

	}

	/**
	 * @see org.apache.lucene.gdata.gom.GOMEntry#addLink(org.apache.lucene.gdata.gom.GOMLink)
	 */
	public void addLink(GOMLink aLink) {
		if (aLink != null)
			this.links.add(aLink);

	}

	/**
	 * @see org.apache.lucene.gdata.gom.GOMEntry#getAuthors()
	 */
	public List<GOMAuthor> getAuthors() {
		return this.authors;
	}

	/**
	 * @see org.apache.lucene.gdata.gom.GOMEntry#getCategories()
	 */
	public List<GOMCategory> getCategories() {
		return this.categories;

	}

	/**
	 * @see org.apache.lucene.gdata.gom.GOMEntry#getContributor()
	 */
	public List<GOMContributor> getContributor() {
		return this.contributors;
	}

	/**
	 * @see org.apache.lucene.gdata.gom.GOMEntry#getId()
	 */
	public GOMId getId() {
		return this.id;
	}

	/**
	 * @see org.apache.lucene.gdata.gom.GOMEntry#getLinks()
	 */
	public List<GOMLink> getLinks() {
		return this.links;
	}

	/**
	 * @see org.apache.lucene.gdata.gom.GOMEntry#getPublished()
	 */
	public GOMPublished getPublished() {
		return this.published;
	}

	/**
	 * @see org.apache.lucene.gdata.gom.GOMEntry#getRights()
	 */
	public GOMRights getRights() {
		return this.rights;
	}

	/**
	 * @see org.apache.lucene.gdata.gom.GOMEntry#getSource()
	 */
	public GOMSource getSource() {
		return this.source;
	}

	/**
	 * @see org.apache.lucene.gdata.gom.GOMEntry#getSummary()
	 */
	/**
	 * @see org.apache.lucene.gdata.gom.GOMEntry#getSummary()
	 */
	public GOMSummary getSummary() {
		return this.summary;
	}

	/**
	 * @see org.apache.lucene.gdata.gom.GOMEntry#getTitle()
	 */
	public GOMTitle getTitle() {
		return this.title;
	}

	/**
	 * @see org.apache.lucene.gdata.gom.GOMEntry#getUpdated()
	 */
	public GOMUpdated getUpdated() {
		return this.updated;
	}

	/**
	 * @see org.apache.lucene.gdata.gom.GOMEntry#setId(org.apache.lucene.gdata.gom.GOMId)
	 */
	public void setId(GOMId aId) {
		this.id = aId;
	}

	/**
	 * @see org.apache.lucene.gdata.gom.GOMEntry#setPublished(org.apache.lucene.gdata.gom.GOMPublished)
	 */
	public void setPublished(GOMPublished aPublished) {
		this.published = aPublished;
	}

	/**
	 * @see org.apache.lucene.gdata.gom.GOMEntry#setRights(org.apache.lucene.gdata.gom.GOMRights)
	 */
	public void setRights(GOMRights aRights) {
		this.rights = aRights;
	}

	/**
	 * @see org.apache.lucene.gdata.gom.GOMEntry#setSource(org.apache.lucene.gdata.gom.GOMSource)
	 */
	public void setSource(GOMSource aSource) {
		this.source = aSource;
	}

	/**
	 * @see org.apache.lucene.gdata.gom.GOMEntry#setSummary(org.apache.lucene.gdata.gom.GOMSummary)
	 */
	public void setSummary(GOMSummary aSummary) {
		this.summary = aSummary;
	}

	/**
	 * @see org.apache.lucene.gdata.gom.GOMEntry#setTitle(org.apache.lucene.gdata.gom.GOMTitle)
	 */
	public void setTitle(GOMTitle aTitle) {
		this.title = aTitle;

	}

	/**
	 * @see org.apache.lucene.gdata.gom.GOMEntry#setUpdated(org.apache.lucene.gdata.gom.GOMUpdated)
	 */
	public void setUpdated(GOMUpdated aUpdated) {
		this.updated = aUpdated;
	}

	/**
	 * @see org.apache.lucene.gdata.gom.GOMEntry#addNamespace(org.apache.lucene.gdata.gom.GOMNamespace)
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
	 * @see org.apache.lucene.gdata.gom.GOMEntry#getNamespaces()
	 */
	public List<GOMNamespace> getNamespaces() {
		return this.namespaces;
	}

	/**
	 * @see org.apache.lucene.gdata.gom.GOMEntry#getDefaultNamespace()
	 */
	public GOMNamespace getDefaultNamespace() {

		return this.defaultNamespace;
	}

	/**
	 * @see org.apache.lucene.gdata.gom.GOMElement#writeAtomOutput(org.apache.lucene.gdata.gom.writer.GOMStaxWriter)
	 */
	public void writeAtomOutput(GOMOutputWriter aStreamWriter)
			throws XMLStreamException {
		aStreamWriter
				.writeStartElement(this.qname, getXmlNamespaceAttributes());
		if (this.defaultNamespace != null)
			aStreamWriter.writeDefaultNamespace(this.defaultNamespace);
		for (GOMNamespace namespace : this.namespaces) {
			aStreamWriter.writeNamespace(namespace);
		}
		if (this.id != null)
			this.id.writeAtomOutput(aStreamWriter);
		if (this.published != null)
			this.published.writeAtomOutput(aStreamWriter);
		if (this.updated != null)
			this.updated.writeAtomOutput(aStreamWriter);
		for (GOMCategory category : this.categories) {
			category.writeAtomOutput(aStreamWriter);
		}
		if (this.title != null)
			this.title.writeAtomOutput(aStreamWriter);
		if (this.summary != null)
			this.summary.writeAtomOutput(aStreamWriter);
		if (this.content != null)
			this.content.writeAtomOutput(aStreamWriter);
		for (GOMLink link : this.links) {
			link.writeAtomOutput(aStreamWriter);
		}
		for (GOMAuthor autor : this.authors) {
			autor.writeAtomOutput(aStreamWriter);
		}
		for (GOMContributor contributor : this.contributors) {
			contributor.writeAtomOutput(aStreamWriter);
		}
		if (this.rights != null) {
			this.rights.writeAtomOutput(aStreamWriter);
		}
		if (this.source != null) {
			this.source.writeAtomOutput(aStreamWriter);
		}

		for (GOMExtension extension : this.extensions) {
			extension.writeAtomOutput(aStreamWriter);
		}
		aStreamWriter.writeEndElement();

	}

	/**
	 * @see org.apache.lucene.gdata.gom.GOMElement#writeRssOutput(org.apache.lucene.gdata.gom.writer.GOMStaxWriter)
	 */
	public void writeRssOutput(GOMOutputWriter aStreamWriter)
			throws XMLStreamException {
		aStreamWriter.writeStartElement(GOMEntry.LOCALNAME_RSS,
				getXmlNamespaceAttributes());
		if (this.defaultNamespace != null)
			aStreamWriter.writeNamespace(this.defaultNamespace);
		for (GOMNamespace namespace : this.namespaces) {
			aStreamWriter.writeNamespace(namespace);
		}
		if (this.id != null)
			this.id.writeRssOutput(aStreamWriter, GOMId.LOCALNAME_RSS);
		String xmlLang = null;

		if (this.content != null) {
			xmlLang = this.content.getXmlLang();
		}
		if (xmlLang == null && this.summary != null) {
			xmlLang = this.summary.getXmlLang();
		}

		if (xmlLang == null && this.title != null) {
			xmlLang = this.title.getXmlLang();
		}

		if (xmlLang != null) {
			aStreamWriter.writeSimpleXMLElement("language", xmlLang, null);
		}
		if (this.published != null) {
			this.published.writeRssOutput(aStreamWriter);
		}
		if (this.updated != null)
			this.updated.writeRssOutput(aStreamWriter);
		for (GOMCategory category : this.categories) {
			category.writeRssOutput(aStreamWriter);
		}
		if (this.title != null)
			this.title.writeRssOutput(aStreamWriter);
		if (this.summary != null)
			this.summary.writeRssOutput(aStreamWriter);
		if (this.content != null)
			this.content.writeRssOutput(aStreamWriter);
		for (GOMLink link : this.links) {
			link.writeRssOutput(aStreamWriter);
		}
		for (GOMAuthor author : this.authors) {
			author.writeRssOutput(aStreamWriter);
		}

		for (GOMContributor contributors : this.contributors) {
			contributors.writeRssOutput(aStreamWriter);
		}

		for (GOMExtension extension : this.extensions) {
			extension.writeRssOutput(aStreamWriter);
		}
		aStreamWriter.writeEndElement();
	}

	/**
	 * @see org.apache.lucene.gdata.gom.core.AbstractGOMElement#getChildParser(javax.xml.namespace.QName)
	 */
	@Override
	public AtomParser getChildParser(QName aName) {
		if (aName.getNamespaceURI().equals(GOMNamespace.ATOM_NS_URI)) {
			if (aName.getLocalPart().equals(GOMId.LOCALNAME)) {
				// atom:feed elements MUST contain exactly one atom:id element.
				if (this.id != null)
					throw new GDataParseException(String.format(
							AtomParser.DUPLICATE_ELEMENT, GOMId.LOCALNAME));
				this.id = new GOMIdImpl();
				return this.id;
			}
			if (aName.getLocalPart().equals(GOMTitle.LOCALNAME)) {
				// atom:entry elements MUST contain exactly one atom:title
				// element.
				if (this.title != null)
					throw new GDataParseException(String.format(
							AtomParser.DUPLICATE_ELEMENT, GOMTitle.LOCALNAME));
				this.title = new GOMTitleImpl();
				return this.title;
			}
			if (aName.getLocalPart().equals(GOMAuthor.LOCALNAME)) {
				GOMAuthor author = new GOMAuthorImpl();
				this.authors.add(author);
				return author;
			}
			if (aName.getLocalPart().equals(GOMCategory.LOCALNAME)) {
				GOMCategory category = new GOMCategoryImpl();
				this.categories.add(category);
				return category;
			}
			if (aName.getLocalPart().equals(GOMContributor.LOCALNAME)) {
				GOMContributorImpl impl = new GOMContributorImpl();
				this.contributors.add(impl);
				return impl;
			}
			if (aName.getLocalPart().equals(GOMLink.LOCALNAME)) {
				GOMLinkImpl impl = new GOMLinkImpl();
				this.links.add(impl);
				return impl;
			}

			if (aName.getLocalPart().equals(GOMUpdated.LOCALNAME)) {
				if (this.updated != null)
					throw new GDataParseException(String.format(
							AtomParser.DUPLICATE_ELEMENT,
							GOMUpdated.LOCALNAME));
				GOMUpdated updatedImpl = new GOMUpdatedImpl();
				this.updated = updatedImpl;
				return this.updated;

			}
			if (aName.getLocalPart().equals(GOMRights.LOCALNAME)) {
				if (this.rights != null)
					throw new GDataParseException(String
							.format(AtomParser.DUPLICATE_ELEMENT,
									GOMRights.LOCALNAME));

				this.rights = new GOMRightsImpl();
				return this.rights;

			}
			if (aName.getLocalPart().equals(GOMSource.LOCALNAME)) {
				if (this.source != null)
					throw new GDataParseException(String
							.format(AtomParser.DUPLICATE_ELEMENT,
									GOMSource.LOCALNAME));
				this.source = new GOMSourceImpl();

				return this.source;

			}
			if (aName.getLocalPart().equals(GOMSummary.LOCALNAME)) {
				if (this.summary != null)
					throw new GDataParseException(String.format(
							AtomParser.DUPLICATE_ELEMENT,
							GOMSummary.LOCALNAME));

				this.summary = new GOMSummaryImpl();
				return this.summary;

			}
			if (aName.getLocalPart().equals(GOMPublished.LOCALNAME)) {
				if (this.published != null)
					throw new GDataParseException(String.format(
							AtomParser.DUPLICATE_ELEMENT,
							GOMPublished.LOCALNAME));

				this.published = new GOMPublishedImpl();
				return this.published;

			}
			if (aName.getLocalPart().endsWith(GOMContent.LOCALNAME)) {
				if (this.content != null)
					throw new GDataParseException(String.format(
							AtomParser.DUPLICATE_ELEMENT,
							GOMContent.LOCALNAME));
				this.content = new GOMContentImpl();
				return this.content;

			}

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
	 * @see org.apache.lucene.gdata.gom.core.AbstractGOMElement#processEndElement()
	 */
	@Override
	public void processEndElement() {

		/*
		 * atom:entry elements MUST contain an atom:summary element in either of
		 * the following cases:
		 * 
		 * the atom:entry contains an atom:content that has a "src" attribute
		 * (and is thus empty). the atom:entry contains content that is encoded
		 * in Base64; i.e., the "type" attribute of atom:content is a MIME media
		 * type [MIMEREG], but is not an XML media type [RFC3023], does not
		 * begin with "text/", and does not end with "/xml" or "+xml".
		 * 
		 * 
		 */
		if (this.summary == null && this.content != null) {

			if (this.content.getAtomMediaType() == AtomMediaType.BINARY
					|| "".equals(this.content.getSrc())) {
				throw new GDataParseException(String.format(
						MISSING_ELEMENT_CHILD, this.localName,
						GOMSummary.LOCALNAME));
			}
		}

		/*
		 * atom:entry elements MUST contain exactly one atom:id element.
		 */
		if (this.id == null)
			throw new GDataParseException(String.format(
					MISSING_ELEMENT_CHILD, this.localName, GOMId.LOCALNAME));
		/*
		 * atom:entry elements MUST contain exactly one atom:title element.
		 */
		if (this.title == null)
			throw new GDataParseException(String
					.format(MISSING_ELEMENT_CHILD, this.localName,
							GOMTitle.LOCALNAME));
		/*
		 * atom:entry elements MUST contain exactly one atom:updated element.
		 */
		if (this.updated == null)
			throw new GDataParseException(String.format(
					MISSING_ELEMENT_CHILD, this.localName,
					GOMUpdated.LOCALNAME));

		/*
		 * atom:entry elements MUST NOT contain more than one atom:link element
		 * with a rel attribute value of "alternate" that has the same
		 * combination of type and hreflang attribute values.
		 */
		List<GOMLink> alternateLinks = new LinkedList<GOMLink>();
		for (GOMLink link : this.links) {
			/*
			 * atom:link elements MAY have a "rel" attribute that indicates the
			 * link relation type. If the "rel" attribute is not present, the
			 * link element MUST be interpreted as if the link relation type is
			 * "alternate".
			 */
			if (link.getRel() == null
					|| link.getRel().equalsIgnoreCase("alternate"))
				alternateLinks.add(link);
		}

		/*
		 * atom:entry elements MUST NOT contain more than one atom:link element
		 * with a rel attribute value of "alternate" that has the same
		 * combination of type and hreflang attribute values.
		 */
		if (alternateLinks.size() > 1) {
			for (GOMLink link : alternateLinks) {
				for (GOMLink link2 : alternateLinks) {
					if (AtomParserUtils.compareAlternateLinks(link, link2))
						throw new GDataParseException(
								String
										.format(DUPLICATE_ELEMENT,
												"link with rel=\"alternate\" and same href and type attributes"));

				}
			}
		} else if (this.content == null && alternateLinks.size() == 0) {
			throw new GDataParseException(
					"Element Entry must contain a element link with attribute alternate if no content element is set");
		}
	}

	/**
	 * @see org.apache.lucene.gdata.gom.GOMExtensible#getExtensions()
	 */
	public List<GOMExtension> getExtensions() {
		return this.extensions;
	}

	/**
	 * @see org.apache.lucene.gdata.gom.GOMExtensible#setExtensionFactory(org.apache.lucene.gdata.gom.core.extension.GOMExtensionFactory)
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
	 * @see org.apache.lucene.gdata.gom.GOMEntry#getContent()
	 */
	public GOMContent getContent() {
		return this.content;
	}

	/**
	 * @see org.apache.lucene.gdata.gom.GOMEntry#setContent(org.apache.lucene.gdata.gom.GOMContent)
	 */
	public void setContent(GOMContent aContent) {
		this.content = aContent;

	}

}
