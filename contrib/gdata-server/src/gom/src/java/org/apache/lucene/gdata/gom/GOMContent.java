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
 * <p>
 * GOMContent represents the atom:content element.
 * </p>
 * The "atom:content" element either contains or links to the content of the
 * entry. The content of atom:content is Language-Sensitive.
 * 
 * <pre>
 *   atomInlineTextContent =
 *   element atom:content {
 *   atomCommonAttributes,
 *   attribute type { &quot;text&quot; | &quot;html&quot; }?,
 *   (text)*
 *   }
 *  
 *   atomInlineXHTMLContent =
 *   element atom:content {
 *   atomCommonAttributes,
 *   attribute type { &quot;xhtml&quot; },
 *   xhtmlDiv
 *   }
 *  
 *   atomInlineOtherContent =
 *   element atom:content {
 *   atomCommonAttributes,
 *   attribute type { atomMediaType }?,
 *   (text|anyElement)*
 *   }
 *  
 *  
 *   atomOutOfLineContent =
 *   element atom:content {
 *   atomCommonAttributes,
 *   attribute type { atomMediaType }?,
 *   attribute src { atomUri },
 *   empty
 *   }
 *  
 *   atomContent = atomInlineTextContent
 *   | atomInlineXHTMLContent
 *  
 *   | atomInlineOtherContent
 *   | atomOutOfLineContent
 * </pre>
 * 
 * @author Simon Willnauer
 * @see org.apache.lucene.gdata.gom.GOMTextConstruct
 *
 * 
 */
public interface GOMContent extends GOMTextConstruct {
	/**
	 * Atom local name for the xml element
	 */
	public static final String LOCALNAME = "content";
	/**
	 * RSS local name for the xml element
	 */
	public static final String LOCAL_NAME_RSS = "description";
	/**
	 * The src attribute value
	 * @return - the value of the src attribute
	 */
	public abstract String getSrc();
	/**
	 * The src attribute value
	 * @param aSrc - the src attribute value to set
	 */
	public abstract void setSrc(String aSrc);
	/**
	 * The contents abstract media type
	 * @param aMediaType - 
	 */
	public abstract void setAtomMediaType(AtomMediaType aMediaType);

	/**
	 * @return - the atom media type of the content element
	 * @see AtomMediaType
	 */
	public abstract AtomMediaType getAtomMediaType();
}
