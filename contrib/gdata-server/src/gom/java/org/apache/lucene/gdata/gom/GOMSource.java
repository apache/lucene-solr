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
 * If an atom:entry is copied from one feed into another feed, then the source
 * atom:feed's metadata (all child elements of atom:feed other than the
 * atom:entry elements) MAY be preserved within the copied entry by adding an
 * atom:source child element, if it is not already present in the entry, and
 * including some or all of the source feed's Metadata elements as the
 * atom:source element's children. Such metadata SHOULD be preserved if the
 * source atom:feed contains any of the child elements atom:author,
 * atom:contributor, atom:rights, or atom:category and those child elements are
 * not present in the source atom:entry.
 * 
 * <pre>
 *    atomSource =
 *    element atom:source {
 *    atomCommonAttributes,
 *    (atomAuthor*
 *    &amp; atomCategory*
 *    &amp; atomContributor*
 *    &amp; atomGenerator?
 *    &amp; atomIcon?
 *    &amp; atomId?
 *    &amp; atomLink*
 *    &amp; atomLogo?
 *    &amp; atomRights?
 *    &amp; atomSubtitle?
 *    &amp; atomTitle?
 *    &amp; atomUpdated?
 *    &amp; extensionElement*)
 *    }
 * </pre>
 * 
 * @author Simon Willnauer
 * 
 */
public interface GOMSource extends GOMXmlEntity, GOMElement {
	public static final String LOCALNAME = "source";

	/**
	 * @param aAuthor
	 */
	public void addAuthor(GOMAuthor aAuthor);

	/**
	 * @param aCategory
	 */
	public void addCategory(GOMCategory aCategory);

	/**
	 * @param aContributor
	 */
	public void addContributor(GOMContributor aContributor);

	/**
	 * @param aLink
	 */
	public void addLink(GOMLink aLink);

	public List<GOMAuthor> getAuthors();

	public List<GOMCategory> getCategories();

	public List<GOMContributor> getContributor();

	public GOMGenerator getGenerator();

	public GOMId getId();

	public void setGenerator(GOMGenerator aGenerator);

	public void setIcon(GOMIcon aIcon);

	public void setId(GOMId aId);

	public GOMLogo getLogo();

	public void setLogo(GOMLogo aLogo);

	public GOMRights getRights();

	public void setRights(GOMRights aRights);

	public GOMSubtitle getSubtitle();

	public void setSubtitle(GOMSubtitle aSubtitle);

	public GOMTitle getTitle();

	public void setTitle(GOMTitle aTitle);

	public GOMUpdated getUpdated();

	public void setUpdated(GOMUpdated aUpdated);

	public GOMIcon getIcon();

	public List<GOMLink> getLinks();
	
	//TODO needs extension elements

}
