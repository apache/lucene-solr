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

import javax.xml.namespace.QName;

/**
 * GOMXmlEntity is a abstract base interface for all Gdata Object Model
 * Interfaces to be implemented by any class which is a part of the GOM. This
 * interface defines a basic interface for xml attributes and elements
 * 
 * @author Simon Willnauer
 * 
 */
public abstract interface GOMXmlEntity {

	/**
	 * @return - the entities QName
	 * @see QName
	 * 
	 */
	public abstract QName getQname();

	/**
	 * @param aString - the namespace uri to set
	 */
	public abstract void setNamespaceUri(String aString);

	/**
	 * @param aString - the namespace prefix to set
	 */
	public abstract void setNamespacePrefix(String aString);

	/**
	 * @param aLocalName - the localname of the entitiy
	 */
	public abstract void setLocalName(String aLocalName);

	/**
	 * @return - the local name of the entitiy
	 */
	public abstract String getLocalName();

	/**
	 * @return - the text value of the entity
	 */
	public abstract String getTextValue();

	/**
	 * @param aTextValue - the text value of the entity
	 */
	public abstract void setTextValue(String aTextValue);

}
