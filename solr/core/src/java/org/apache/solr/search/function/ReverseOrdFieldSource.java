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
package org.apache.solr.search.function;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.docvalues.IntDocValues;
import org.apache.lucene.search.SortedSetSelector;
import org.apache.solr.common.SolrException;
import org.apache.solr.index.SlowCompositeReaderWrapper;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.Insanity;
import org.apache.solr.search.SolrIndexSearcher;

/**
 * Obtains the ordinal of the field value from {@link org.apache.lucene.index.LeafReader#getSortedDocValues}
 * and reverses the order.
 * <br>
 * The native lucene index order is used to assign an ordinal value for each field value.
 * <br>Field values (terms) are lexicographically ordered by unicode value, and numbered starting at 1.
 * <br>
 * Example of reverse ordinal (rord):<br>
 *  If there were only three field values: "apple","banana","pear"
 * <br>then rord("apple")=3, rord("banana")=2, ord("pear")=1
 * <p>
 *  WARNING: ord() depends on the position in an index and can thus change when other documents are inserted or deleted,
 *  or if a MultiSearcher is used.
 * <br>
 *  WARNING: as of Solr 1.4, ord() and rord() can cause excess memory use since they must use a FieldCache entry
 * at the top level reader, while sorting and function queries now use entries at the segment level.  Hence sorting
 * or using a different function query, in addition to ord()/rord() will double memory use.
 * 
 *
 */

public class ReverseOrdFieldSource extends ValueSource {
  public final String field;

  public ReverseOrdFieldSource(String field) {
    this.field = field;
  }

  @Override
  public String description() {
    return "rord("+field+')';
  }

  @Override
  @SuppressWarnings({"rawtypes"})
  public FunctionValues getValues(Map context, LeafReaderContext readerContext) throws IOException {
    final int off = readerContext.docBase;
    final LeafReader r;
    Object o = context.get("searcher");
    if (o instanceof SolrIndexSearcher) {
      @SuppressWarnings("resource")  final SolrIndexSearcher is = (SolrIndexSearcher) o;
      SchemaField sf = is.getSchema().getFieldOrNull(field);
      if (sf != null && sf.getType().isPointField()) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
            "rord() is not supported over Points based field " + field);
      }
      if (sf != null && sf.hasDocValues() == false && sf.multiValued() == false && sf.getType().getNumberType() != null) {
        // it's a single-valued numeric field: we must currently create insanity :(
        List<LeafReaderContext> leaves = is.getIndexReader().leaves();
        LeafReader insaneLeaves[] = new LeafReader[leaves.size()];
        int upto = 0;
        for (LeafReaderContext raw : leaves) {
          insaneLeaves[upto++] = Insanity.wrapInsanity(raw.reader(), field);
        }
        r = SlowCompositeReaderWrapper.wrap(new MultiReader(insaneLeaves));
      } else {
        // reuse ordinalmap
        r = ((SolrIndexSearcher)o).getSlowAtomicReader();
      }
    } else {
      IndexReader topReader = ReaderUtil.getTopLevelContext(readerContext).reader();
      r = SlowCompositeReaderWrapper.wrap(topReader);
    }
    // if it's e.g. tokenized/multivalued, emulate old behavior of single-valued fc
    final SortedDocValues sindex = SortedSetSelector.wrap(DocValues.getSortedSet(r, field), SortedSetSelector.Type.MIN);
    final int end = sindex.getValueCount();

    return new IntDocValues(this) {
      @Override
      public int intVal(int doc) throws IOException {
        if (doc+off > sindex.docID()) {
          sindex.advance(doc+off);
        }
        if (doc+off == sindex.docID()) {
          return (end - sindex.ordValue() - 1);
        } else {
          return end;
        }
      }
    };
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || (o.getClass() !=  ReverseOrdFieldSource.class)) return false;
    ReverseOrdFieldSource other = (ReverseOrdFieldSource)o;
    return this.field.equals(other.field);
  }

  private static final int hcode = ReverseOrdFieldSource.class.hashCode();
  @Override
  public int hashCode() {
    return hcode + field.hashCode();
  }

}
