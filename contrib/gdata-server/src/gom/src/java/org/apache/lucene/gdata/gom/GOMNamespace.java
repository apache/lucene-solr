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
 * A simple domain object to represent a xml namespace.
 * 
 * @author Simon Willnauer
 * 
 */
public final class GOMNamespace {

	/**
	 * XML namespace uri
	 */
	public static final String XML_NS_URI = "http://www.w3.org/XML/1998/namespace";

	/**
	 * XML namespace prefix
	 */
	public static final String XML_NS_PREFIX = "xml";

	/**
	 * Amazon "opensearch" namespace prefix
	 */
	public static final String OPENSEARCH_NS_PREFIX = "openSearch";

	/**
	 * Amazon "opensearch" namespace uri
	 */
	public static final String OPENSEARCH_NS_URI = "http://a9.com/-/spec/opensearchrss/1.0/";

	/**
	 * ATOM namespace uri
	 */
	public static final String ATOM_NS_URI = "http://www.w3.org/2005/Atom";

	/**
	 * ATOM namespace prefix
	 */
	public static final String ATOM_NS_PREFIX = "atom";

	/**
	 * ATOM namespace
	 */
	public static final GOMNamespace ATOM_NAMESPACE = new GOMNamespace(
			ATOM_NS_URI, ATOM_NS_PREFIX);

	/**
	 * Amazon "opensearch" namespace
	 */
	public static final GOMNamespace OPENSEARCH_NAMESPACE = new GOMNamespace(
			OPENSEARCH_NS_URI, OPENSEARCH_NS_PREFIX);

	private final String namespaceUri;

	private final String namespacePrefix;

	/**
	 * Class constructor for GOMNamespace
	 * 
	 * @param aNamespaceUri -
	 *            the namespace uri (must not be null)
	 * @param aNamespacePrefix -
	 *            the namespace prefix (if null an empty string will be
	 *            assigned)
	 * 
	 */
	public GOMNamespace(final String aNamespaceUri,
			final String aNamespacePrefix) {
		if (aNamespaceUri == null)
			throw new IllegalArgumentException("uri must not be null");
		this.namespacePrefix = aNamespacePrefix == null ? "" : aNamespacePrefix;
		this.namespaceUri = aNamespaceUri;
	}

	/**
	 * @return Returns the namespacePrefix.
	 */
	public String getNamespacePrefix() {
		return this.namespacePrefix;
	}

	/**
	 * @return Returns the namespaceUri.
	 */
	public String getNamespaceUri() {
		return this.namespaceUri;
	}

	/**
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object arg0) {
		if (arg0 == null)
			return false;
		if (arg0 == this)
			return true;
		if (arg0 instanceof GOMNamespace) {
			GOMNamespace other = (GOMNamespace) arg0;
			return this.namespacePrefix.equals(other.getNamespacePrefix())
					&& this.namespaceUri.equals(other.getNamespaceUri());
		}
		return false;
	}

	/**
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		/*
		 * The multiplier 37 was chosen because it is an odd prime. If it was
		 * even and the multiplication overflowed, information would be lost
		 * because multiplication by two is equivalent to shifting The value 17
		 * is arbitrary. see
		 * http://java.sun.com/developer/Books/effectivejava/Chapter3.pdf
		 */
		int hash = 17;
		hash = 37 * hash + this.namespacePrefix.hashCode();
		hash = 37 * hash + this.namespaceUri.hashCode();
		return hash;
	}

	/**
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder(this.getClass().getName());
		builder.append(" uri: ").append(this.namespaceUri);
		builder.append(" prefix: ").append(this.namespacePrefix);
		return builder.toString();
	}

}
