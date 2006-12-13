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

import junit.framework.TestCase;

/**
 * Testcase for GOMNamespace
 * 
 * @author Simon Willnauer
 * 
 */
public class GOMNamespaceTest extends TestCase {

	/*
	 * Test method for
	 * 'org.apache.lucene.gdata.gom.GOMNamespace.GOMNamespace(String, String)'
	 */
	public void testGOMNamespace() {
		try {
			GOMNamespace namespace = new GOMNamespace(null, "a");
			fail("uri is null");
		} catch (IllegalArgumentException e) {
			//
		}
		try {
			new GOMNamespace("a", null);
		} catch (Exception e) {
			fail("unexp. exc");
		}

		GOMNamespace namespace = new GOMNamespace(GOMNamespace.ATOM_NS_URI,
				GOMNamespace.ATOM_NS_PREFIX);
		assertEquals(GOMNamespace.ATOM_NS_PREFIX, namespace
				.getNamespacePrefix());
		assertEquals(GOMNamespace.ATOM_NS_URI, namespace.getNamespaceUri());
		//		

	}

}
