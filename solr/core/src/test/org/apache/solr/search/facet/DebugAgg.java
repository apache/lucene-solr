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
import java.util.concurrent.atomic.AtomicLong;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.search.DocSet;
import org.apache.solr.search.FunctionQParser;
import org.apache.solr.search.SyntaxError;
import org.apache.solr.search.ValueSourceParser;


public class DebugAgg extends AggValueSource {

  public static class Parser extends ValueSourceParser {
    @Override
    public ValueSource parse(FunctionQParser fp) throws SyntaxError {
      return new DebugAgg();
    }

    @Override
    public void init(NamedList args) {
    }
  }


  public DebugAgg() {
    super("debug");
  }

  public DebugAgg(String name) {
    super(name);
  }

  @Override
  public SlotAcc createSlotAcc(FacetContext fcontext, int numDocs, int numSlots) {
    return new Acc(fcontext, numDocs, numSlots);
  }

  @Override
  public int hashCode() {
    return 0;
  }

  @Override
  public String description() {
    return "debug()";
  }

  public static class Acc extends SlotAcc {
    public static AtomicLong creates = new AtomicLong(0);
    public static AtomicLong resets = new AtomicLong(0);
    public static AtomicLong resizes = new AtomicLong(0);
    public static Acc last;

    public CountSlotAcc sub;
    public int numDocs;
    public int numSlots;

    public Acc(FacetContext fcontext, int numDocs, int numSlots) {
      super(fcontext);
      this.last = this;
      this.numDocs = numDocs;
      this.numSlots = numSlots;
      creates.addAndGet(1);
      sub = new CountSlotArrAcc(fcontext, numSlots);
//      new RuntimeException("DEBUG Acc numSlots=" + numSlots).printStackTrace();
    }

    @Override
    public void collect(int doc, int slot) throws IOException {
      sub.collect(doc, slot);
    }

    @Override
    public int compare(int slotA, int slotB) {
      return sub.compare(slotA, slotB);
    }

    @Override
    public Object getValue(int slotNum) throws IOException {
      return sub.getValue(slotNum);
    }

    @Override
    public void reset() throws IOException {
      resets.addAndGet(1);
      sub.reset();
    }

    @Override
    public void resize(Resizer resizer) {
      resizes.addAndGet(1);
      this.numSlots = resizer.getNewSize();
      sub.resize(resizer);
    }

    @Override
    public void setNextReader(LeafReaderContext readerContext) throws IOException {
      sub.setNextReader(readerContext);
    }

    @Override
    public int collect(DocSet docs, int slot) throws IOException {
      return sub.collect(docs, slot);
    }

    @Override
    public void setValues(SimpleOrderedMap<Object> bucket, int slotNum) throws IOException {
      sub.key = this.key;  // TODO: Blech... this should be fixed
      sub.setValues(bucket, slotNum);
    }

    @Override
    public void close() throws IOException {
      sub.close();
    }
  }

  @Override
  public FacetMerger createFacetMerger(Object prototype) {
    return new FacetLongMerger();
  }

}
