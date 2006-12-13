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

import org.apache.lucene.gdata.gom.core.extension.GOMExtensionFactory;

/**
 * <p>
 * The Gdata Object Model describes an abstract object model for the gdata
 * protocol. GData is supposed to be very flexible and extensible. Users should
 * be able to extend {@link org.apache.lucene.gdata.gom.GOMFeed} and
 * {@link org.apache.lucene.gdata.gom.GOMEntry} elements to create extensions
 * and custom classes for their own model.
 * </p>
 * 
 * <p>
 * This interface describes the extensible GOM entities.
 * </p>
 * 
 * @author Simon Willnauer
 * @see org.apache.lucene.gdata.gom.GOMFeed
 * @see org.apache.lucene.gdata.gom.GOMEntry
 * 
 */
public interface GOMExtensible {
	//TODO add setter!
	//TODO add how to
	/**
	 * @return - a list of all extensions specified to the extended element
	 */
	public List<GOMExtension> getExtensions();

	/**
	 * 
	 * @param factory - the extension factory to set
	 */
	public void setExtensionFactory(GOMExtensionFactory factory);

}
