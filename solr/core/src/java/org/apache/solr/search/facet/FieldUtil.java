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
package org.apache.solr.search.facet;

import java.io.IOException;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.QParser;
import org.apache.solr.search.QueryContext;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.uninverting.FieldCacheImpl;

/** @lucene.internal
 * Porting helper... may be removed if it offers no value in the future.
 */
public class FieldUtil {

  /** Simpler method that creates a request context and looks up the field for you */
  public static SortedDocValues getSortedDocValues(SolrIndexSearcher searcher, String field) throws IOException {
    SchemaField sf = searcher.getSchema().getField(field);
    QueryContext qContext = QueryContext.newContext(searcher);
    return getSortedDocValues( qContext, sf, null );
  }


  public static SortedDocValues getSortedDocValues(QueryContext context, SchemaField field, QParser qparser) throws IOException {
    SortedDocValues si = context.searcher().getSlowAtomicReader().getSortedDocValues( field.getName() );
    // if (!field.hasDocValues() && (field.getType() instanceof StrField || field.getType() instanceof TextField)) {
    // }

    return si == null ? DocValues.emptySorted() : si;
  }

  public static SortedSetDocValues getSortedSetDocValues(QueryContext context, SchemaField field, QParser qparser) throws IOException {
    SortedSetDocValues si = context.searcher().getSlowAtomicReader().getSortedSetDocValues(field.getName());
    return si == null ? DocValues.emptySortedSet() : si;
  }

  public static NumericDocValues getNumericDocValues(QueryContext context, SchemaField field, QParser qparser) throws IOException {
    SolrIndexSearcher searcher = context.searcher();
    NumericDocValues si = searcher.getSlowAtomicReader().getNumericDocValues(field.getName());
    return si == null ? DocValues.emptyNumeric() : si;
  }

  /** The following ord visitors and wrappers are a work in progress and experimental
   *  @lucene.experimental */
  @FunctionalInterface
  public interface OrdFunc {
    void handleOrd(int docid, int ord); // TODO: throw exception?
  }

  public static boolean isFieldCache(SortedDocValues singleDv) {
    return singleDv instanceof FieldCacheImpl.SortedDocValuesImpl.Iter;
  }

  public static void visitOrds(SortedDocValues singleDv, DocIdSetIterator disi, OrdFunc ordFunc) throws IOException {
    int doc;
    if (singleDv instanceof FieldCacheImpl.SortedDocValuesImpl.Iter) {
      FieldCacheImpl.SortedDocValuesImpl.Iter fc = (FieldCacheImpl.SortedDocValuesImpl.Iter) singleDv;
      while ((doc = disi.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
        ordFunc.handleOrd(doc, fc.getOrd(doc));
      }
    } else {
      while ((doc = disi.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
        if (singleDv.advanceExact(doc)) {
          ordFunc.handleOrd(doc, singleDv.ordValue());
        } else {
          // TODO: optionally pass in missingOrd?
        }
      }
    }
  }

  public static OrdValues getOrdValues(SortedDocValues singleDv, DocIdSetIterator disi) {
    if (singleDv instanceof FieldCacheImpl.SortedDocValuesImpl.Iter) {
      FieldCacheImpl.SortedDocValuesImpl.Iter fc = (FieldCacheImpl.SortedDocValuesImpl.Iter) singleDv;
      return new FCOrdValues(fc, disi);
    }
    return new DVOrdValues(singleDv, disi);
  }


  public static abstract class OrdValues extends SortedDocValues {
    int doc;
    int ord;

    public int getOrd() {
      return ord;
    }

    @Override
    public int docID() {
      return doc;
    }

    @Override
    public abstract int nextDoc() throws IOException;

    @Override
    public int advance(int target) throws IOException {
      return 0; // TODO
    }

    @Override
    public long cost() {
      return 0;
    }

    @Override
    public int getValueCount() {
      throw new UnsupportedOperationException();
    }
  }


  public static class FCOrdValues extends OrdValues {
    FieldCacheImpl.SortedDocValuesImpl.Iter vals;
    DocIdSetIterator disi;

    public FCOrdValues(FieldCacheImpl.SortedDocValuesImpl.Iter iter, DocIdSetIterator disi) {
      this.vals = iter;
      this.disi = disi;
    }

    @Override
    public int nextDoc() throws IOException {
      doc = disi.nextDoc();
      if (doc == NO_MORE_DOCS) return NO_MORE_DOCS;
      ord = vals.getOrd(doc); // todo: loop until a hit?
      return doc;
    }

    @Override
    public boolean advanceExact(int target) throws IOException {
      return false;
    }

    @Override
    public int ordValue() {
      return 0;
    }

    @Override
    public BytesRef lookupOrd(int ord) throws IOException {
      return null;
    }
  }

  public static class DVOrdValues extends OrdValues {
    SortedDocValues vals;
    DocIdSetIterator disi;
    int valDoc;

    public DVOrdValues(SortedDocValues vals, DocIdSetIterator disi) {
      this.vals = vals;
      this.disi = disi;
    }

    @Override
    public int nextDoc() throws IOException {
      for (;;) {
        // todo - use skipping when appropriate
        doc = disi.nextDoc();
        if (doc == NO_MORE_DOCS) return NO_MORE_DOCS;
        boolean match = vals.advanceExact(doc);
        if (match) {
          ord = vals.ordValue();
          return doc;
        }
      }
    }

    @Override
    public boolean advanceExact(int target) throws IOException {
      return false;
    }

    @Override
    public int ordValue() {
      return 0;
    }

    @Override
    public BytesRef lookupOrd(int ord) throws IOException {
      return null;
    }
  }
}
