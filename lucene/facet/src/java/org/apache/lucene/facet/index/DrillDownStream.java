package org.apache.lucene.facet.index;

import java.io.IOException;
import java.util.Iterator;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.facet.params.FacetIndexingParams;
import org.apache.lucene.facet.taxonomy.CategoryPath;

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
 * A {@link TokenStream} which creates category drill-down terms.
 * 
 * @lucene.experimental
 */
public class DrillDownStream extends TokenStream {

  private final FacetIndexingParams indexingParams;
  private final Iterator<CategoryPath> categories;
  private final CharTermAttribute termAttribute;
  
  private CategoryPath current;
  private boolean isParent;
  
  public DrillDownStream(Iterable<CategoryPath> categories, FacetIndexingParams indexingParams) {
    termAttribute = addAttribute(CharTermAttribute.class);
    this.categories = categories.iterator();
    this.indexingParams = indexingParams;
  }

  protected void addAdditionalAttributes(CategoryPath category, boolean isParent) {
    // a hook for AssociationsDrillDownStream to add the associations payload to
    // the drill-down terms
  }
  
  @Override
  public final boolean incrementToken() throws IOException {
    if (current.length == 0) {
      if (!categories.hasNext()) {
        return false; // no more categories
      }
      current = categories.next();
      termAttribute.resizeBuffer(current.fullPathLength());
      isParent = false;
    }

    // copy current as drill-down term (it's either the leaf node or PathPolicy
    // accepted it.
    int nChars = indexingParams.drillDownTermText(current, termAttribute.buffer());
    termAttribute.setLength(nChars);
    addAdditionalAttributes(current, isParent);
    
    // prepare current for next call by trimming the last component (parents)
    current = current.subpath(current.length - 1);
    isParent = true;
    return true;
  }

  @Override
  public void reset() throws IOException {
    current = categories.next();
    termAttribute.resizeBuffer(current.fullPathLength());
    isParent = false;
  }
  
}
