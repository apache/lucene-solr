package org.apache.lucene.facet.index.categorypolicy;

import java.io.Serializable;

import org.apache.lucene.facet.index.streaming.CategoryParentsStream;
import org.apache.lucene.facet.taxonomy.TaxonomyWriter;

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

/**
 * Filtering category ordinals in {@link CategoryParentsStream}, where a given
 * category ordinal is added to the stream, and than its parents are being added
 * one after the other using {@link TaxonomyWriter#getParent(int)}. <br>
 * That loop should have a stop point - the default approach (excluding the
 * ROOT) is implemented in {@link DefaultOrdinalPolicy}.
 * 
 * @lucene.experimental
 */
public interface OrdinalPolicy extends Serializable {

  /**
   * Check whether a given category ordinal should be added to the stream.
   * 
   * @param ordinal
   *            A given category ordinal which is to be tested for stream
   *            addition.
   * @return <code>true</code> if the category should be added.
   *         <code>false</code> otherwise.
   */
  public abstract boolean shouldAdd(int ordinal);

  /**
   * Initialize the policy with a TaxonomyWriter. This method can be
   * implemented as noop if the ordinal policy is not taxonomy dependent
   * 
   * @param taxonomyWriter
   *            A relevant taxonomyWriter object, with which ordinals sent to
   *            {@link #shouldAdd(int)} are examined.
   */
  public abstract void init(TaxonomyWriter taxonomyWriter);
}