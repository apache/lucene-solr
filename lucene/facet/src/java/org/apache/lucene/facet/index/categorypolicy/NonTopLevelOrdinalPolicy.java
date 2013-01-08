package org.apache.lucene.facet.index.categorypolicy;

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
 * Filter out any "top level" category ordinals. <br> {@link #shouldAdd(int)}.
 * 
 * @lucene.experimental
 */
public class NonTopLevelOrdinalPolicy implements OrdinalPolicy {

  /**
   * The taxonomyWriter with which the given ordinals' parent is determined.
   */
  private TaxonomyWriter taxonomyWriter;

  /**
   * Constructs a new non-top-level-ordinal-filter. With a given
   * taxonomyWriter.
   * 
   */
  public NonTopLevelOrdinalPolicy() {
    this.taxonomyWriter = null;
  }

  /** 
   * @param taxonomyWriter
   *            A relevant taxonomyWriter object, with which ordinals sent to
   *            {@link #shouldAdd(int)} are examined.
   */
  @Override
  public void init(TaxonomyWriter taxonomyWriter) {
    this.taxonomyWriter = taxonomyWriter;
  }
  
  /**
   * Filters out ordinal which are ROOT or who's parent is ROOT. In order to
   * determine if a parent is root, there's a need for
   * {@link TaxonomyWriter#getParent(int)}.
   */
  @Override
  public boolean shouldAdd(int ordinal) {
    if (ordinal > TaxonomyReader.ROOT_ORDINAL) {
      try {
        if (this.taxonomyWriter.getParent(ordinal) > TaxonomyReader.ROOT_ORDINAL) {
          return true;
        }
      } catch (Exception e) {
        return false;
      }
    }
    return false;
  }

}
