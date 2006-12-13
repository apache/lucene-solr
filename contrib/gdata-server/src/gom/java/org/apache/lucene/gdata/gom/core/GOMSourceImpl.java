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
import org.apache.lucene.gdata.gom.GOMContributor;
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
import org.apache.lucene.gdata.gom.core.utils.AtomParserUtils;
import org.apache.lucene.gdata.gom.writer.GOMOutputWriter;

/**
 * @author Simon Willnauer
 * 
 */
public class GOMSourceImpl extends AbstractGOMElement implements GOMSource {

	protected List<GOMAuthor> authors = new LinkedList<GOMAuthor>();

	protected List<GOMCategory> categories = new LinkedList<GOMCategory>();

	protected List<GOMLink> links = new LinkedList<GOMLink>();

	protected List<GOMContributor> contributors = new LinkedList<GOMContributor>();

	protected GOMGenerator generator;

	protected GOMId id;

	protected GOMLogo logo;

	protected GOMRights rights;

	protected GOMSubtitle subtitle;

	protected GOMTitle title;

	protected GOMUpdated updated;

	protected GOMIcon icon;

	GOMSourceImpl() {
		this.localName = LOCALNAME;
		this.qname = new QName(GOMNamespace.ATOM_NS_URI, this.localName);
	}

	/**
	 * @see org.apache.lucene.gdata.gom.core.AbstractGOMElement#getLocalName()
	 */
	@Override
	public String getLocalName() {
		return this.localName;
	}

	/**
	 * @see org.apache.lucene.gdata.gom.GOMSource#addAuthor(org.apache.lucene.gdata.gom.GOMAuthor)
	 */
	public void addAuthor(GOMAuthor aAuthor) {
		if (aAuthor != null)
			this.authors.add(aAuthor);

	}

	/**
	 * @see org.apache.lucene.gdata.gom.GOMSource#addCategory(org.apache.lucene.gdata.gom.GOMCategory)
	 */
	public void addCategory(GOMCategory aCategory) {
		if (aCategory != null)
			this.categories.add(aCategory);

	}

	/**
	 * @see org.apache.lucene.gdata.gom.GOMSource#addContributor(org.apache.lucene.gdata.gom.GOMContributor)
	 */
	public void addContributor(GOMContributor aContributor) {
		if (aContributor != null)
			this.contributors.add(aContributor);

	}

	/**
	 * @see org.apache.lucene.gdata.gom.GOMSource#getAuthors()
	 * 
	 */
	public List<GOMAuthor> getAuthors() {
		return this.authors;
	}

	/**
	 * @see org.apache.lucene.gdata.gom.GOMSource#getCategories()
	 * 
	 */
	public List<GOMCategory> getCategories() {
		return this.categories;
	}

	/**
	 * @see org.apache.lucene.gdata.gom.GOMSource#getContributor()
	 */
	public List<GOMContributor> getContributor() {
		return this.contributors;
	}

	/**
	 * @see org.apache.lucene.gdata.gom.GOMSource#getGenerator()
	 * 
	 */
	public GOMGenerator getGenerator() {
		return this.generator;
	}

	/**
	 * @see org.apache.lucene.gdata.gom.GOMSource#getId()
	 * 
	 */
	public GOMId getId() {
		return this.id;
	}

	/**
	 * @see org.apache.lucene.gdata.gom.GOMSource#setGenerator(org.apache.lucene.gdata.gom.GOMGenerator)
	 * 
	 */
	public void setGenerator(GOMGenerator aGenerator) {
		this.generator = aGenerator;

	}

	/**
	 * @see org.apache.lucene.gdata.gom.GOMSource#setIcon(org.apache.lucene.gdata.gom.GOMIcon)
	 * 
	 */
	public void setIcon(GOMIcon aIcon) {
		this.icon = aIcon;

	}

	/**
	 * @see org.apache.lucene.gdata.gom.GOMSource#setId(org.apache.lucene.gdata.gom.GOMId)
	 * 
	 */
	public void setId(GOMId aId) {
		this.id = aId;

	}

	/**
	 * @return the logo
	 * 
	 */
	public GOMLogo getLogo() {
		return this.logo;
	}

	/**
	 * @param aLogo
	 *            the logo to set
	 * 
	 */
	public void setLogo(GOMLogo aLogo) {
		this.logo = aLogo;
	}

	/**
	 * @return the rights
	 * 
	 */
	public GOMRights getRights() {
		return this.rights;
	}

	/**
	 * @param aRights
	 *            the rights to set
	 * 
	 */
	public void setRights(GOMRights aRights) {
		rights = aRights;
	}

	/**
	 * @return the subtitle
	 * 
	 */
	public GOMSubtitle getSubtitle() {
		return this.subtitle;
	}

	/**
	 * @param aSubtitle
	 *            the subtitle to set
	 * 
	 */
	public void setSubtitle(GOMSubtitle aSubtitle) {
		this.subtitle = aSubtitle;
	}

	/**
	 * @return the title
	 * 
	 */
	public GOMTitle getTitle() {
		return this.title;
	}

	/**
	 * @param aTitle
	 *            the title to set
	 * 
	 */
	public void setTitle(GOMTitle aTitle) {
		this.title = aTitle;
	}

	/**
	 * @return the updated
	 * 
	 */
	public GOMUpdated getUpdated() {
		return this.updated;
	}

	/**
	 * @param aUpdated
	 *            the updated to set
	 * 
	 */
	public void setUpdated(GOMUpdated aUpdated) {
		this.updated = aUpdated;
	}

	/**
	 * @return the icon
	 * 
	 */
	public GOMIcon getIcon() {
		return this.icon;
	}

	/**
	 * @return the links
	 * 
	 */
	public List<GOMLink> getLinks() {
		return this.links;
	}

	/**
	 * @see org.apache.lucene.gdata.gom.GOMSource#addLink(org.apache.lucene.gdata.gom.GOMLink)
	 */
	public void addLink(GOMLink aLink) {
		if (aLink == null)
			return;
		this.links.add(aLink);

	}

	/**
	 * @see org.apache.lucene.gdata.gom.core.AtomParser#processElementValue(java.lang.String)
	 */
	public void processElementValue(String aValue) {
		throw new GDataParseException(String.format(
				AtomParser.UNEXPECTED_ELEMENT_VALUE, this.localName));
	}

	/**
	 * @see org.apache.lucene.gdata.gom.core.AtomParser#processEndElement()
	 */
	public void processEndElement() {
		/*
		 * atom:feed elements MUST contain exactly one atom:id element.
		 */
		if (this.id == null)
			throw new GDataParseException(String.format(
					MISSING_ELEMENT_CHILD, this.localName, GOMId.LOCALNAME));
		/*
		 * atom:feed elements MUST contain exactly one atom:title element.
		 */
		if (this.title == null)
			throw new GDataParseException(String
					.format(MISSING_ELEMENT_CHILD, this.localName,
							GOMTitle.LOCALNAME));
		/*
		 * atom:feed elements MUST contain exactly one atom:updated element.
		 */
		if (this.updated == null)
			throw new GDataParseException(String.format(
					MISSING_ELEMENT_CHILD, this.localName,
					GOMUpdated.LOCALNAME));
		/*
		 * atom:feed elements MUST contain one or more atom:author elements,
		 * unless all of the
		 */
		if (this.authors.size() < 1)
			throw new GDataParseException(String.format(
					MISSING_ELEMENT_CHILD, this.localName,
					GOMAuthor.LOCALNAME));

		/*
		 * atom:feed elements MUST NOT contain more than one atom:link element
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
		 * atom:feed elements MUST NOT contain more than one atom:link element
		 * with a rel attribute value of "alternate" that has the same
		 * combination of type and hreflang attribute values.
		 */
		if (alternateLinks.size() > 1) {
			for (GOMLink link : alternateLinks) {
				for (GOMLink link2 : alternateLinks) {
					if (link != link2)
						if (AtomParserUtils.compareAlternateLinks(link, link2))
							throw new GDataParseException(
									String
											.format(DUPLICATE_ELEMENT,
													"link with rel=\"alternate\" and same href and type attributes"));

				}
			}
		}

	}

	/**
	 * @see org.apache.lucene.gdata.gom.core.AtomParser#getChildParser(javax.xml.namespace.QName)
	 */
	public AtomParser getChildParser(QName aName) {
		if (aName.getNamespaceURI().equals(GOMNamespace.ATOM_NS_URI)) {
			if (aName.getLocalPart().equals(GOMId.LOCALNAME)) {
				// atom:feed / atom:source elements MUST contain exactly one
				// atom:id element.
				if (this.id != null)
					throw new GDataParseException(String.format(
							AtomParser.DUPLICATE_ELEMENT, GOMId.LOCALNAME));
				this.id = new GOMIdImpl();
				return this.id;
			}
			if (aName.getLocalPart().equals(GOMTitle.LOCALNAME)) {
				// atom:feed / atom:source elements MUST contain exactly one
				// atom:title
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
			if (aName.getLocalPart().equals(GOMSubtitle.LOCALNAME)) {
				GOMSubtitleImpl impl = new GOMSubtitleImpl();
				/*
				 * atom:feed elements MUST NOT contain more than one
				 * atom:subtitle element.
				 */
				if (this.subtitle != null)
					throw new GDataParseException(String.format(
							AtomParser.DUPLICATE_ELEMENT,
							GOMSubtitle.LOCALNAME));
				this.subtitle = impl;
				return this.subtitle;
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
			if (aName.getLocalPart().equals(GOMLogo.LOCALNAME)) {
				if (this.logo != null)
					throw new GDataParseException(String.format(
							AtomParser.DUPLICATE_ELEMENT, GOMLogo.LOCALNAME));

				this.logo = new GOMLogoImpl();
				return this.logo;

			}
			if (aName.getLocalPart().equals(GOMIcon.LOCALNAME)) {
				if (this.icon != null)
					throw new GDataParseException(String.format(
							AtomParser.DUPLICATE_ELEMENT, GOMIcon.LOCALNAME));

				this.icon = new GOMIconImpl();
				return this.icon;

			}
			if (aName.getLocalPart().equals(GOMGenerator.LOCALNAME)) {
				if (this.generator != null)
					throw new GDataParseException(String.format(
							AtomParser.DUPLICATE_ELEMENT,
							GOMGenerator.LOCALNAME));

				this.generator = new GOMGeneratorImpl();
				return this.generator;

			}
			if (aName.getLocalPart().equals(GOMRights.LOCALNAME)) {
				if (this.rights != null)
					throw new GDataParseException(String
							.format(AtomParser.DUPLICATE_ELEMENT,
									GOMRights.LOCALNAME));

				this.rights = new GOMRightsImpl();
				return this.rights;

			}

		}
		throw new GDataParseException(String.format(
				AtomParser.URECOGNIZED_ELEMENT_CHILD, this.localName, aName
						.getLocalPart()));

	}

	/**
	 * @see org.apache.lucene.gdata.gom.GOMElement#writeAtomOutput(org.apache.lucene.gdata.gom.writer.GOMStaxWriter)
	 */
	public void writeAtomOutput(GOMOutputWriter aStreamWriter)
			throws XMLStreamException {
		aStreamWriter.writeStartElement(this.localName,
				this.extensionAttributes);
		List<GOMAttribute> xmlNamespaceAttributes = getXmlNamespaceAttributes();
		for (GOMAttribute attribute : xmlNamespaceAttributes) {
			aStreamWriter.writeAttribute(attribute);
		}
		writeInnerAtomOutput(aStreamWriter);
		aStreamWriter.writeEndElement();

	}

	/**
	 * @param aStreamWriter
	 * @throws XMLStreamException
	 */
	protected void writeInnerAtomOutput(GOMOutputWriter aStreamWriter)
			throws XMLStreamException {
		if (this.id != null)
			this.id.writeAtomOutput(aStreamWriter);
		if (this.title != null)
			this.title.writeAtomOutput(aStreamWriter);
		if (this.subtitle != null)
			this.subtitle.writeAtomOutput(aStreamWriter);
		for (GOMAuthor authors : this.authors) {
			authors.writeAtomOutput(aStreamWriter);
		}
		for (GOMCategory category : this.categories) {
			category.writeAtomOutput(aStreamWriter);
		}
		for (GOMContributor contributor : this.contributors) {
			contributor.writeAtomOutput(aStreamWriter);
		}
		for (GOMLink link : this.links) {
			link.writeAtomOutput(aStreamWriter);
		}
		if (this.rights != null)
			this.rights.writeAtomOutput(aStreamWriter);
		if (this.updated != null)
			this.updated.writeAtomOutput(aStreamWriter);
		if (this.logo != null)
			this.logo.writeAtomOutput(aStreamWriter);
		if (this.icon != null)
			this.icon.writeAtomOutput(aStreamWriter);
		if (this.generator != null)
			this.generator.writeAtomOutput(aStreamWriter);
	}

	/**
	 * @see org.apache.lucene.gdata.gom.GOMElement#writeRssOutput(org.apache.lucene.gdata.gom.writer.GOMStaxWriter)
	 */
	public void writeRssOutput(GOMOutputWriter aStreamWriter)
			throws XMLStreamException {
		// no rss output

	}

}
