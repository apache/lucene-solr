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
 * The "atom:updated" element is a Date construct indicating the most recent
 * instant in time when an entry or feed was modified in a way the publisher
 * considers significant. Therefore, not all modifications necessarily result in
 * a changed atom:updated value.
 * 
 * <pre>
 *  atomUpdated = element atom:updated { atomDateConstruct}
 * </pre>
 * 
 * @author Simon Willnauer
 * @see org.apache.lucene.gdata.gom.GOMDateConstruct
 */
public interface GOMUpdated extends GOMDateConstruct {
	/**
	 * Atom local name for the xml element
	 */
	public static final String LOCALNAME = "updated";
}
