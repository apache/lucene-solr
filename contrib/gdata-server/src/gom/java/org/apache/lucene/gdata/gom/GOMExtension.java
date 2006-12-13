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
 * The GData Object Model is like the Google Data Api highly extensible and
 * offers a lot of base classes to extend as a {@link GOMExtension}. All
 * extensions returned by
 * {@link org.apache.lucene.gdata.gom.core.extension.GOMExtensionFactory#canHandleExtensionElement(QName)}
 * must implement this interface. <br>
 * GOM extensions can either be created via the
 * {@link org.apache.lucene.gdata.gom.core.extension.GOMExtensionFactory} or by
 * subclassing either {@link org.apache.lucene.gdata.gom.GOMFeed} or
 * {@link org.apache.lucene.gdata.gom.GOMEntry}.
 * 
 * @author Simon Willnauer
 * @see org.apache.lucene.gdata.gom.GOMExtensible
 * @see org.apache.lucene.gdata.gom.core.extension.GOMExtensionFactory 
 */
public interface GOMExtension extends GOMXmlEntity, GOMElement {

}
