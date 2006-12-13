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

/**
 * The "atom:link" element defines a reference from an entry or feed to a Web
 * resource. This specification assigns no meaning to the content (if any) of
 * this element.
 * 
 * <pre>
 *  atomLink =
 *  element atom:link {
 *  atomCommonAttributes,
 *  attribute href { atomUri },
 *  attribute rel { atomNCName | atomUri }?,
 *  attribute type { atomMediaType }?,
 *  attribute hreflang { atomLanguageTag }?,
 *  attribute title { text }?,
 *  attribute length { text }?,
 *  undefinedContent
 *  }
 * </pre>
 * 
 * @author Simon Willnauer
 * 
 */
public interface GOMLink extends GOMElement {
	/**
	 * Atom local name for the xml element
	 */
	public static final String LOCALNAME = "link";

	/**
	 * @return - the href attribute value of the element link
	 */
	public String getHref();

	/**
	 * @param aHref -
	 *            the href attribute value of the element link to set.
	 */
	public void setHref(String aHref);

	/**
	 * @return the hreflang attribute value of the element link
	 */
	public String getHrefLang();

	/**
	 * @param aHrefLang -
	 *            the hreflang attribute value of the element link to set.
	 */
	public void setHrefLang(String aHrefLang);

	/**
	 * @return - the length attribute value of the element link.
	 */
	public Integer getLength();

	/**
	 * @param aLength -
	 *            the length attribute value of the element link to set.
	 */
	public void setLength(Integer aLength);

	/**
	 * @return - the rel attribute value of the element link.
	 */
	public String getRel();

	/**
	 * @param aRel -
	 *            the rel attribute value of the element link to set
	 */
	public void setRel(String aRel);

	/**
	 * @return - the title attribute value of the element link.
	 */
	public String getTitle();

	/**
	 * @param aTitle -
	 *            the title attribute value of the element link to set
	 */
	public void setTitle(String aTitle);

	/**
	 * @return - the type attribute value of the element link.
	 */
	public String getType();

	/**
	 * @param aType -
	 *            the type attribute value of the element link.
	 */
	public void setType(String aType);

}
