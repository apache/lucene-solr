/**
 * contributor license agreements.  See the NOTICE file distributed with
 * Licensed to the Apache Software Foundation (ASF) under one or more
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
 * The "atom:contributor" element is a Person construct that indicates a person
 * or other entity who contributed to the entry or feed.
 * 
 * <pre>
 * atomContributor = element atom:contributor { atomPersonConstruct }
 * </pre>
 * 
 * @author Simon Willnauer
 * 
 */
public interface GOMContributor extends GOMPerson {
	/**
	 * Atom 1.0 local name for the xml element
	 */
	public static final String LOCALNAME = "contributor";
}
