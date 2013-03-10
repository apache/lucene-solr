/*
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
package org.apache.solr.search;

import java.util.*;

import org.apache.solr.response.transform.DocTransformer;

/**
 * A class representing the return fields
 *
 *
 * @since solr 4.0
 */
public abstract class ReturnFields {
  /**
   * Set of field names with their exact names from the lucene index.
   * <p>
   * Class such as ResponseWriters pass this to {@link SolrIndexSearcher#doc(int, Set)}.
   * @return Set of field names or <code>null</code> (all fields).
   */
  public abstract Set<String> getLuceneFieldNames();

  /** Returns <code>true</code> if the specified field should be returned. */
  public abstract boolean wantsField(String name);

  /** Returns <code>true</code> if all fields should be returned. */
  public abstract boolean wantsAllFields();

  /** Returns <code>true</code> if the score should be returned. */
  public abstract boolean wantsScore();

  /** Returns the DocTransformer used to modify documents, or <code>null</code> */
  public abstract DocTransformer getTransformer();
}
