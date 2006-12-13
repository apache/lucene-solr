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

import javax.xml.namespace.QName;

import junit.framework.TestCase;

/**
 * @author Simon Willnauer
 * 
 */
public class GOMAttributeImplTest extends TestCase {
	protected GOMAttributeImpl gomAttribute;

	protected void setUp() throws Exception {
		gomAttribute = new GOMAttributeImpl("test", "test");
	}

	/*
	 * Test method for
	 * 'org.apache.lucene.gdata.gom.core.GOMAttributeImpl.GOMAttributeImpl(String,
	 * String)'
	 */
	public void testGOMAttributeImplStringString() {
		GOMAttributeImpl impl = new GOMAttributeImpl("test", "test");
		assertTrue(impl.hasDefaultNamespace());
		assertTrue(impl.getLocalName().equals(impl.getTextValue()));
		assertEquals("test", impl.getLocalName());

	}

	/*
	 * Test method for
	 * 'org.apache.lucene.gdata.gom.core.GOMAttributeImpl.GOMAttributeImpl(String,
	 * String, String, String)'
	 */
	public void testGOMAttributeImplStringStringStringString() {
		GOMAttributeImpl impl = new GOMAttributeImpl("www.apache.org", "ap",
				"test", "test");
		assertFalse(impl.hasDefaultNamespace());
		assertTrue(impl.getLocalName().equals(impl.getTextValue()));
		assertEquals("test", impl.getLocalName());
		assertEquals("www.apache.org", impl.getQname().getNamespaceURI());
		assertEquals("ap", impl.getQname().getPrefix());
	}

	/*
	 * Test method for
	 * 'org.apache.lucene.gdata.gom.core.GOMAttributeImpl.getQname()'
	 */
	public void testGetQname() {
		QName qname = gomAttribute.getQname();
		assertSame(qname, gomAttribute.getQname());
		assertTrue(gomAttribute.hasDefaultNamespace());
		gomAttribute.setNamespaceUri("something else");
		assertNotSame(qname, gomAttribute.getQname());
		assertFalse(gomAttribute.hasDefaultNamespace());
	}

}
