package org.apache.lucene.search;

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

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.util.OpenBitSet;

import java.io.IOException;
import java.util.Iterator;

/**
 * A term filter built on top of a cached single field (in FieldCache). It can be used only
 * with single-valued fields.
 * <p/>
 * FieldCacheTermsFilter builds a single cache for the field the first time it is used. Each
 * subsequent FieldCacheTermsFilter on the same field then re-uses this cache even if the terms
 * themselves are different.
 * <p/>
 * The FieldCacheTermsFilter is faster than building a TermsFilter each time.
 * FieldCacheTermsFilter are fast to build in cases where number of documents are far more than
 * unique terms. Internally, it creates a BitSet by term number and scans by document id.
 * <p/>
 * As with all FieldCache based functionality, FieldCacheTermsFilter is only valid for fields
 * which contain zero or one terms for each document. Thus it works on dates, prices and other
 * single value fields but will not work on regular text fields. It is preferable to use an
 * NOT_ANALYZED field to ensure that there is only a single term.
 * <p/>
 * Also, collation is performed at the time the FieldCache is built; to change collation you
 * need to override the getFieldCache() method to change the underlying cache.
 */
public class FieldCacheTermsFilter extends Filter {
  private String field;
  private Iterable terms;

  public FieldCacheTermsFilter(String field, Iterable terms) {
    this.field = field;
    this.terms = terms;
  }

  public FieldCache getFieldCache() {
    return FieldCache.DEFAULT;
  }

  public DocIdSet getDocIdSet(IndexReader reader) throws IOException {
    return new FieldCacheTermsFilterDocIdSet(getFieldCache().getStringIndex(reader, field));
  }

  protected class FieldCacheTermsFilterDocIdSet extends DocIdSet {
    private FieldCache.StringIndex fcsi;

    private OpenBitSet openBitSet;

    public FieldCacheTermsFilterDocIdSet(FieldCache.StringIndex fcsi) {
      this.fcsi = fcsi;
      openBitSet = new OpenBitSet(this.fcsi.lookup.length);
      for (Iterator it = terms.iterator(); it.hasNext();) {
        Object term = it.next();
        int termNumber = this.fcsi.binarySearchLookup((String) term);
        if (termNumber > 0) {
          openBitSet.fastSet(termNumber);
        }
      }
    }

    public DocIdSetIterator iterator() {
      return new FieldCacheTermsFilterDocIdSetIterator();
    }

    protected class FieldCacheTermsFilterDocIdSetIterator extends DocIdSetIterator {
      private int doc = -1;

      public int doc() {
        return doc;
      }

      public boolean next() {
        try {
          do {
            doc++;
          } while (!openBitSet.fastGet(fcsi.order[doc]));
          return true;
        } catch (ArrayIndexOutOfBoundsException e) {
          doc = Integer.MAX_VALUE;
          return false;
        }
      }

      public boolean skipTo(int target) {
        try {
          doc = target;
          while (!openBitSet.fastGet(fcsi.order[doc])) {
            doc++;
          }
          return true;
        } catch (ArrayIndexOutOfBoundsException e) {
          doc = Integer.MAX_VALUE;
          return false;
        }
      }
    }
  }
}
