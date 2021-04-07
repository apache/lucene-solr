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

import java.util.Map;
import java.util.Set;

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
   * Class such as ResponseWriters pass this to {@link SolrIndexSearcher#doc(int, Set)}.
   * <p>
   * NOTE: In some situations, this method may return <code>null</code> even if {@link #wantsAllFields()} 
   * is <code>false</code>.  For example: When glob expressions are used ({@link #hasPatternMatching}), 
   * it is safer to request all field names then to attempt to resolve the globs against all possible 
   * dynamic field names in the index.
   * </p>
   * @return Set of field names or <code>null</code> (all fields).
   */
  public abstract Set<String> getLuceneFieldNames();

  /**
   * Set of field names with their exact names from the lucene index.
   *
   * @param ignoreWantsAll If true, it returns any additional specified field names, in spite of
   *                       also wanting all fields. Example: when fl=*,field1, returns ["field1"].
   *                       If false, the method returns null when all fields are wanted. Example: when fl=*,field1, returns null.
   *                       Note that this method returns null regardless of ignoreWantsAll if all fields
   *                       are requested and no explicit field names are specified.
   */
  public abstract Set<String> getLuceneFieldNames(boolean ignoreWantsAll);

  /**
   * The requested field names (includes pseudo fields)
   * @return Set of field names or <code>null</code> (all fields).
   */
  public abstract Set<String> getRequestedFieldNames();

  /**
   * The explicitly requested field names (includes pseudo fields)
   * @return Set of explicitly requested field names or <code>null</code> (no explict)
   */
  public abstract Set<String> getExplicitlyRequestedFieldNames();

  /**
   * Get the fields which have been renamed
   * @return a mapping of renamed fields
   */
  public abstract Map<String,String> getFieldRenames();

  /** 
   * Returns <code>true</code> if the specified field should be returned <em>to the external client</em> 
   * -- either using its own name, or via an alias. 
   * This method returns <code>false</code> even if the specified name is needed as an "extra" field
   * for use by transformers.
   */
  public abstract boolean wantsField(String name);

  /** 
   * Returns <code>true</code> if all fields should be returned <em>to the external client</em>. 
   */
  public abstract boolean wantsAllFields();

  /** Returns <code>true</code> if the score should be returned. */
  public abstract boolean wantsScore();

  /** Returns <code>true</code> if the fieldnames should be picked with a pattern */
  public abstract boolean hasPatternMatching();

  /** Returns the DocTransformer used to modify documents, or <code>null</code> */
  public abstract DocTransformer getTransformer();
}
