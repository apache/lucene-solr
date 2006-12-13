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
package org.apache.lucene.gdata.gom;

import java.util.List;

/**
 * 
 * <p>
 * The GOMEntry class represents a "atom:entry" element in the GData Object
 * Model.
 * </p>
 * <p>
 * The "atom:entry" element represents an individual entry, acting as a
 * container for metadata and data associated with the entry. This element can
 * appear as a child of the atom:feed element, or it can appear as the document
 * (i.e., top-level) element of a stand-alone Atom Entry Document.
 * </p>
 * <p>
 * RelaxNG Schema:
 * </p>
 * 
 * <pre>
 *     atomEntry =
 *     element atom:entry {
 *     atomCommonAttributes,
 *     (	atomAuthor*
 *     	&amp; atomCategory*
 *     	&amp; atomContent?
 *     	&amp; atomContributor*
 *     	&amp; atomId
 *     	&amp; atomLink*
 *     	&amp; atomPublished?
 *   	 	&amp; atomRights?
 *     	&amp; atomSource?
 *     	&amp; atomSummary?
 *     	&amp; atomTitle
 *     	&amp; atomUpdated
 *     	&amp; extensionElement*)
 *     }
 * </pre>
 * 
 * @author Simon Willnauer
 * 
 */
public interface GOMEntry extends GOMXmlEntity, GOMElement, GOMExtensible {
	/**
	 * Atom 1.0 local name for the xml element
	 */
	public static final String LOCALNAME = "entry";

	/**
	 * RSS 2.0 local name for the xml element
	 */
	public static final String LOCALNAME_RSS = "item";

	/**
	 * @param aAuthor -
	 *            a author to add
	 * @see GOMAuthor
	 */
	public abstract void addAuthor(GOMAuthor aAuthor);

	/**
	 * @param aCategory -
	 *            a category to add
	 * @see GOMCategory
	 */
	public abstract void addCategory(GOMCategory aCategory);

	/**
	 * @param aContributor -
	 *            a contributor to add
	 * @see GOMContributor
	 */
	public abstract void addContributor(GOMContributor aContributor);

	/**
	 * @param aLink -
	 *            a link to add
	 * @see GOMLink
	 */
	public abstract void addLink(GOMLink aLink);

	/**
	 * @return - the entry author
	 * @see GOMAuthor
	 */
	public abstract List<GOMAuthor> getAuthors();

	/**
	 * 
	 * This method returns all categories and will never return<code>null</code>
	 * 
	 * @return - a list of categories
	 * @see GOMCategory
	 */
	public abstract List<GOMCategory> getCategories();

	/**
	 * 
	 * This method returns all contributors and will never return<code>null</code>
	 * 
	 * @return - a list of contributors
	 * @see GOMContributor
	 */
	public abstract List<GOMContributor> getContributor();

	/**
	 * @return - the feed id
	 * @see GOMId
	 */
	public abstract GOMId getId();

	/**
	 * @param aId -
	 *            the entry id
	 * @see GOMId
	 */
	public abstract void setId(GOMId aId);

	/**
	 * @return - the entry rights
	 * @see GOMRights
	 */
	public abstract GOMRights getRights();

	/**
	 * @param aRights -
	 *            the GOMRights to set
	 * @see GOMRights
	 */
	public abstract void setRights(GOMRights aRights);

	/**
	 * @return - the entries title
	 * @see GOMTitle
	 */
	public abstract GOMTitle getTitle();

	/**
	 * @param aTitle -
	 *            the title to set
	 * @see GOMTitle
	 */
	public abstract void setTitle(GOMTitle aTitle);

	/**
	 * @return - the last updated element
	 * @see GOMUpdated
	 */
	public abstract GOMUpdated getUpdated();

	/**
	 * @param aUpdated -
	 *            the updated element to set
	 * @see GOMUpdated
	 */
	public abstract void setUpdated(GOMUpdated aUpdated);

	/**
	 * 
	 * This method returns all links and will never return<code>null</code>
	 * 
	 * @return - a list of links
	 * @see GOMLink
	 */
	public abstract List<GOMLink> getLinks();

	/**
	 * @param aSummary -
	 *            a summary to set
	 * @see GOMSummary
	 */
	public abstract void setSummary(GOMSummary aSummary);

	/**
	 * @return - the summary
	 * @see GOMSummary
	 */
	public abstract GOMSummary getSummary();

	/**
	 * @param aSource -
	 *            the source to set
	 * @see GOMSource
	 */
	public abstract void setSource(GOMSource aSource);

	/**
	 * @return - the entry source
	 * @see GOMSource
	 */
	public abstract GOMSource getSource();

	/**
	 * @param aPublished -
	 *            the published element to set
	 * @see GOMPublished
	 */
	public abstract void setPublished(GOMPublished aPublished);

	/**
	 * @return - the published element
	 * @see GOMPublished
	 */
	public abstract GOMPublished getPublished();

	/**
	 * @return - the content element
	 * @see GOMContent
	 */
	public abstract GOMContent getContent();

	/**
	 * @param content -
	 *            the content to set
	 * @see GOMContent
	 */
	public abstract void setContent(GOMContent content);

	/**
	 * @param aNamespace -
	 *            a Namespace to add
	 * @see GOMNamespace
	 */
	public abstract void addNamespace(GOMNamespace aNamespace);

	/**
	 * @return - list of all namespaces - will never be null
	 * @see GOMNamespace
	 */
	public abstract List<GOMNamespace> getNamespaces();

	/**
	 * @return - the default namespace
	 * @see GOMNamespace
	 */
	public abstract GOMNamespace getDefaultNamespace();

}
