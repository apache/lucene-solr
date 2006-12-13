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
 * 
 * GOMCategory type<br>
 * <p>
 * The <b>"category"</b> element conveys information about a category
 * associated with an entry or feed. This specification assigns no meaning to
 * the content (if any) of this element.
 * </p>
 * <p>
 * RelaxNG Schema:
 * </p>
 * 
 * <pre>
 *      atomCategory =
 *      element atom:category {
 *      	atomCommonAttributes,
 *      	attribute term { text },
 *      	attribute scheme { atomUri }?,
 *      	attribute label { text }?,
 *      	undefinedContent
 *      }
 * </pre>
 * 
 * 
 * @author Simon Willnauer
 * 
 */
public interface GOMCategory extends GOMElement {
	/**
	 * Atom local name for the xml element
	 */
	public static final String LOCALNAME = "category";

	/**
	 * Attribute name (attribute term { text })
	 */
	public static final String TERM_ATTRIBUTE = "term";

	/**
	 * Attribute name (attribute label { text })
	 */
	public static final String LABLE_ATTRIBUTE = "label";

	/**
	 * Attribute name (attribute scheme { atomUri })
	 */
	public static final String SCHEME_ATTRIBUTE = "scheme";

	/**
	 * @param aTerm -
	 *            the attribute term { text }
	 */
	public abstract void setTerm(String aTerm);

	/**
	 * @param aLabel -
	 *            the attribute lable { text }
	 */
	public abstract void setLabel(String aLabel);

	/**
	 * @param aScheme -
	 *            the attribute scheme { atomUri }
	 */
	public abstract void setScheme(String aScheme);

	/**
	 * @return the attribute term { text }
	 */
	public abstract String getTerm();

	/**
	 * @return the attribute scheme { atomUri }
	 */
	public abstract String getScheme();

	/**
	 * @return the attribute lable { text }
	 */
	public abstract String getLabel();

}
