package org.apache.lucene.spatial;

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

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.queries.ChainedFilter;
import org.apache.lucene.search.BitsFilteredDocIdSet;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.FieldCache;
import org.apache.lucene.search.Filter;
import org.apache.lucene.spatial.query.SpatialArgs;
import org.apache.lucene.spatial.query.SpatialOperation;
import org.apache.lucene.util.Bits;

import java.io.IOException;

/**
 * A Spatial Filter implementing {@link SpatialOperation#IsDisjointTo} in terms
 * of a {@link SpatialStrategy}'s support for {@link SpatialOperation#Intersects}.
 * A document is considered disjoint if it has spatial data that does not
 * intersect with the query shape.  Another way of looking at this is that it's
 * a way to invert a query shape.
 *
 * @lucene.experimental
 */
public class DisjointSpatialFilter extends Filter {

  private final String field;//maybe null
  private final Filter intersectsFilter;

  /**
   *
   * @param strategy Needed to compute intersects
   * @param args Used in spatial intersection
   * @param field This field is used to determine which docs have spatial data via
   *               {@link org.apache.lucene.search.FieldCache#getDocsWithField(org.apache.lucene.index.AtomicReader, String)}.
   *              Passing null will assume all docs have spatial data.
   */
  public DisjointSpatialFilter(SpatialStrategy strategy, SpatialArgs args, String field) {
    this.field = field;

    // TODO consider making SpatialArgs cloneable
    SpatialOperation origOp = args.getOperation();//copy so we can restore
    args.setOperation(SpatialOperation.Intersects);//temporarily set to intersects
    intersectsFilter = strategy.makeFilter(args);
    args.setOperation(origOp);//restore so it looks like it was
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    DisjointSpatialFilter that = (DisjointSpatialFilter) o;

    if (field != null ? !field.equals(that.field) : that.field != null)
      return false;
    if (!intersectsFilter.equals(that.intersectsFilter)) return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = field != null ? field.hashCode() : 0;
    result = 31 * result + intersectsFilter.hashCode();
    return result;
  }

  @Override
  public DocIdSet getDocIdSet(AtomicReaderContext context, final Bits acceptDocs) throws IOException {
    Bits docsWithField;
    if (field == null) {
      docsWithField = null;//all docs
    } else {
      //NOTE By using the FieldCache we re-use a cache
      // which is nice but loading it in this way might be slower than say using an
      // intersects filter against the world bounds. So do we add a method to the
      // strategy, perhaps?  But the strategy can't cache it.
      docsWithField = FieldCache.DEFAULT.getDocsWithField(context.reader(), field);

      final int maxDoc = context.reader().maxDoc();
      if (docsWithField.length() != maxDoc )
        throw new IllegalStateException("Bits length should be maxDoc ("+maxDoc+") but wasn't: "+docsWithField);

      if (docsWithField instanceof Bits.MatchNoBits) {
        return null;//match nothing
      } else if (docsWithField instanceof Bits.MatchAllBits) {
        docsWithField = null;//all docs
      }
    }

    //not so much a chain but a way to conveniently invert the Filter
    DocIdSet docIdSet = new ChainedFilter(new Filter[]{intersectsFilter}, ChainedFilter.ANDNOT).getDocIdSet(context, acceptDocs);
    return BitsFilteredDocIdSet.wrap(docIdSet, docsWithField);
  }

}
