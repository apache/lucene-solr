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
 * Class representing the "atom:feed" element. The "atom:feed" element is the
 * document (i.e., top-level) element of an Atom Feed Document, acting as a
 * container for metadata and data associated with the feed. Its element
 * children consist of metadata elements followed by zero or more atom:entry
 * child elements.
 * 
 * <pre>
 *        atom:feed {
 *        	atomCommonAttributes, 
 *         	(atomAuthor* &amp; atomCategory* &amp;
 *        	atomContributor* &amp;
 *         	atomGenerator? &amp; atomIcon? &amp;
 *         	atomId &amp; 
 *         	atomLink* &amp;
 *        	atomLogo? &amp;
 *        	atomRights? &amp;
 *        	atomSubtitle? &amp;
 *        	atomTitle &amp; 
 *        	atomUpdated &amp;
 *        	extensionElement*),
 *        	 atomEntry* }
 * </pre>
 * 
 * 
 * @author Simon Willnauer
 * @see org.apache.lucene.gdata.gom.GOMExtensible
 * @see org.apache.lucene.gdata.gom.GOMExtension
 * @see org.apache.lucene.gdata.gom.GOMDocument
 */
public interface GOMFeed extends GOMSource, GOMExtensible {
	/**
	 * Atom local name for the xml element
	 */
	public static final String LOCALNAME = "feed";

	/**
	 * RSS local name for the xml element
	 */
	public static final String LOCALNAME_RSS = "rss";

	/**
	 * RSS channel localname as Rss starts with
	 * 
	 * <pre>
	 *     &lt;rss&gt;&lt;channel&gt;
	 * </pre>
	 */
	public static final String RSS_CHANNEL_ELEMENT_NAME = "channel";

	/**
	 * this class can contain namespaces which will be rendered into the start
	 * element.
	 * 
	 * <pre>
	 *     &lt;feed xmlns:myNs=&quot;someNamespace&quot;&gt;&lt;/feed&gt;
	 * </pre>
	 * 
	 * @param aNamespace -
	 *            a namespace to add
	 */
	public void addNamespace(GOMNamespace aNamespace);

	/**
	 * @return - all declared namespaces, excluding the default namespace, this
	 *         method will never return <code>null</code>.
	 * @see GOMFeed#getDefaultNamespace()
	 */
	public List<GOMNamespace> getNamespaces();

	/**
	 * 
	 * @return - a list of added entries, this method will never return
	 *         <code>null</code>.
	 */
	public List<GOMEntry> getEntries();

	/**
	 * @return - the OpenSearch namespace element <i>itemsPerPage</i> text
	 *         value.
	 */
	public int getItemsPerPage();

	/**
	 * @return - the OpenSearch namespace element <i>startIndex</i> text value.
	 */
	public int getStartIndex();

	/**
	 * @param aIndex -
	 *            the OpenSearch namespace element <i>startIndex</i> text value
	 *            as an integer.
	 */
	public void setStartIndex(int aIndex);

	/**
	 * @param aInt -
	 *            the OpenSearch namespace element <i>itemsPerPage</i> text
	 *            value as an integer.
	 */
	public void setItemsPerPage(int aInt);

	/**
	 * 
	 * @return the default namespace - this will always be
	 *         {@link GOMNamespace#ATOM_NAMESPACE}
	 */
	public GOMNamespace getDefaultNamespace();

}
