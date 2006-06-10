package org.apache.lucene.document;
/**
 * Copyright 2004 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Similar to a {@link java.io.FileFilter}, the FieldSelector allows one to make decisions about
 * what Fields get loaded on a {@link Document} by {@link org.apache.lucene.index.IndexReader#document(int,org.apache.lucene.document.FieldSelector)}
 *
 **/
public interface FieldSelector {

  /**
   * 
   * @param fieldName
   * @return true if the {@link Field} with <code>fieldName</code> should be loaded or not
   */
  FieldSelectorResult accept(String fieldName);
}
