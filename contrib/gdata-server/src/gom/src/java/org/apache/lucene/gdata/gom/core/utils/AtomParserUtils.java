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
package org.apache.lucene.gdata.gom.core.utils;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.regex.Pattern;

import org.apache.lucene.gdata.gom.AtomMediaType;
import org.apache.lucene.gdata.gom.GOMLink;

/**
 * @author Simon Willnauer
 * 
 */
public class AtomParserUtils {
	private static final Pattern ATOM_MEDIA_TYPE_PATTERN = Pattern
			.compile(".+/.+");

	/**
	 * Replaces all xml character with the corresponding entity.
	 * 
	 * <ul>
	 * <li>&lt;!ENTITY lt &quot;&amp;#38;#60;&quot;&gt;</li>
	 * <li>&lt;!ENTITY gt &quot;&amp;#62;&quot;&gt;</li>
	 * <li>&lt;!ENTITY amp &quot;&amp;#38;#38;&quot;&gt;</li>
	 * <li>&lt;!ENTITY apos &quot;&amp;#39;&quot;&gt;</li>
	 * <li>&lt;!ENTITY quot &quot;&amp;#34;&quot;&gt;</li>
	 * </ul>
	 * 
	 * see <a
	 * href="http://www.w3.org/TR/2006/REC-xml-20060816/#intern-replacement">W3C
	 * specification</a>
	 * 
	 * @param aString -
	 *            a string may container xml characters like '<'
	 * @return the input string with escaped xml characters
	 */
	public static String escapeXMLCharacter(String aString) {
		StringBuilder builder = new StringBuilder();
		char[] cs = aString.toCharArray();
		for (int i = 0; i < cs.length; i++) {
			switch (cs[i]) {
			case '<':
				builder.append("&lt;");
				break;
			case '>':
				builder.append("&gt;");
				break;
			case '"':
				builder.append("&quot;");
				break;
			case '\'':
				builder.append("&apos;");
				break;
			case '&':
				builder.append("&amp;");
				break;
			case '\0':
				// this breaks some xml serializer like soap serializer -->
				// remove it
				break;
			default:
				builder.append(cs[i]);
			}
		}

		return builder.toString();

	}

	/**
	 * @param aMediaType
	 * @return
	 */
	public static boolean isAtomMediaType(String aMediaType) {
		return (aMediaType == null || aMediaType.length() < 3) ? false
				: ATOM_MEDIA_TYPE_PATTERN.matcher(aMediaType).matches();
	}

	/**
	 * @param aMediaType
	 * @return
	 */
	public static AtomMediaType getAtomMediaType(String aMediaType) {
		if (aMediaType == null || !isAtomMediaType(aMediaType))
			throw new IllegalArgumentException(
					"aMediaType must be a media type and  not be null ");
		if (aMediaType.endsWith("+xml") || aMediaType.endsWith("/xml"))
			return AtomMediaType.XML;
		if (aMediaType.startsWith("text/"))
			return AtomMediaType.TEXT;
		return AtomMediaType.BINARY;
	}

	/**
	 * @param xmlBase
	 * @param atomUri
	 * @return
	 * @throws URISyntaxException
	 */
	public static String getAbsolutAtomURI(String xmlBase, String atomUri)
			throws URISyntaxException {
		if (atomUri == null)
			throw new IllegalArgumentException("atomUri must not be null");
		if (atomUri.startsWith("www."))
			atomUri = "http://" + atomUri;
		URI aUri = new URI(atomUri);

		if (xmlBase == null || xmlBase.length() == 0) {
			if (!aUri.isAbsolute()) {
				throw new URISyntaxException(atomUri,
						" -- no xml:base specified atom uri must be an absolute url");
			}
		}

		return atomUri;
	}

	/**
	 * Compares two links with rel attribute "alternate" Checks if href and type
	 * are equal
	 * 
	 * @param left -
	 *            left link to compare
	 * @param right -
	 *            right link to compare
	 * @return <code>true</code> if and only if href and type are equal,
	 *         otherwise <code>false</code>
	 */
	public static boolean compareAlternateLinks(GOMLink left, GOMLink right) {
		if ((left.getType() == null) ^ right.getType() == null
				|| (left.getType() == null && right.getType() == null)) {
			return false;
		} else {
			if (!left.getType().equalsIgnoreCase(right.getType()))
				return false;
		}

		if (((left.getHrefLang() == null) ^ right.getHrefLang() == null)
				|| (left.getHrefLang() == null && right.getHrefLang() == null)) {
			return false;
		} else {
			if (!left.getHrefLang().equalsIgnoreCase(right.getHrefLang()))
				return false;
		}
		return true;

	}

	public static void main(String[] args) {
		// String s = new String(
		// "<!ENTITY lt \"&#38;#60;\"><!ENTITY gt \"&#62;\"><!ENTITY amp
		// \"&#38;#38;\"><!ENTITY apos \"&#39;\"><!ENTITY quot \"&#34;\">");
		// System.out.println(escapeXMLCharacter(s));
		//		
		System.out.println(isAtomMediaType("t/h"));
	}

}
