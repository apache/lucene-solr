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
import java.util.function.IntFunction;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.SolrIndexSearcher;

class UniqueMultivaluedSlotAcc extends UniqueSlotAcc implements UnInvertedField.Callback {
  private UnInvertedField uif;
  private UnInvertedField.DocToTerm docToTerm;

  public UniqueMultivaluedSlotAcc(FacetContext fcontext, SchemaField field, int numSlots, HLLAgg.HLLFactory factory) throws IOException {
    super(fcontext, field, numSlots, factory);
    SolrIndexSearcher searcher = fcontext.qcontext.searcher();
    uif = UnInvertedField.getUnInvertedField(field.getName(), searcher);
    docToTerm = uif.new DocToTerm();
    fcontext.qcontext.addCloseHook(this);  // TODO: find way to close accumulators instead of using close hook?
    nTerms = uif.numTerms();
  }

  @Override
  protected BytesRef lookupOrd(int ord) throws IOException {
    return docToTerm.lookupOrd(ord);
  }

  private FixedBitSet bits;  // bits for the current slot, only set for the callback

  @Override
  public void call(int termNum) {
    bits.set(termNum);
  }

  @Override
  public void collect(int doc, int slotNum, IntFunction<SlotContext> slotContext) throws IOException {
    bits = arr[slotNum];
    if (bits == null) {
      bits = new FixedBitSet(nTerms);
      arr[slotNum] = bits;
    }
    docToTerm.getBigTerms(doc + currentDocBase, this);  // this will call back to our Callback.call(int termNum)
    docToTerm.getSmallTerms(doc + currentDocBase, this);
  }

  @Override
  public void close() throws IOException {
    if (docToTerm != null) {
      docToTerm.close();
      docToTerm = null;
    }
  }
}
