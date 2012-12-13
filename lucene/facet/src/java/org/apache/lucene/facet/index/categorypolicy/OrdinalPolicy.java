package org.apache.lucene.facet.index.categorypolicy;

import java.io.Serializable;

import org.apache.lucene.facet.search.FacetsAccumulator;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;
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
 * A policy for adding category parent ordinals to the list of ordinals that are
 * encoded for a given document. The default {@link #ALL_PARENTS} policy always
 * adds all parents, where {@link #NO_PARENTS} never adds any parents.
 * 
 * @lucene.experimental
 */
public interface OrdinalPolicy extends Serializable {

  /**
   * An {@link OrdinalPolicy} which never stores parent ordinals. Useful if you
   * only want to store the exact categories that were added to the document.
   * Note that this is a rather expert policy, which requires a matching
   * {@link FacetsAccumulator} that computes the weight of the parent categories
   * on-the-fly.
   */
  public static final OrdinalPolicy NO_PARENTS = new OrdinalPolicy() {
    @Override
    public boolean shouldAdd(int ordinal) { return false; }

    @Override
    public void init(TaxonomyWriter taxonomyWriter) {}
  };

  /**
   * An {@link OrdinalPolicy} which stores all parent ordinals, except
   * {@link TaxonomyReader#ROOT_ORDINAL}. This is the default
   * {@link OrdinalPolicy} and works with the default {@link FacetsAccumulator}.
   */
  public static final OrdinalPolicy ALL_PARENTS = new OrdinalPolicy() {
    @Override
    public boolean shouldAdd(int ordinal) { return ordinal > TaxonomyReader.ROOT_ORDINAL; }
    
    @Override
    public void init(TaxonomyWriter taxonomyWriter) {}
  };
  
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