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
import java.util.function.IntFunction;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.search.DocSet;
import org.apache.solr.search.FunctionQParser;
import org.apache.solr.search.SyntaxError;
import org.apache.solr.search.ValueSourceParser;


class DebugAgg extends AggValueSource {
  public static AtomicLong parses = new AtomicLong(0);

  public static class Parser extends ValueSourceParser {
    @Override
    public ValueSource parse(FunctionQParser fp) throws SyntaxError {
      parses.incrementAndGet();
      final String what = fp.hasMoreArguments() ? fp.parseId() : "wrap";

      switch (what) {
        case "wrap": return new DebugAgg(fp);
        case "numShards": return new DebugAggNumShards();
        default: /* No-Op */
      }
      throw new RuntimeException("No idea what to do with " + what);
    }

    @Override
    public void init(@SuppressWarnings({"rawtypes"})NamedList args) {
    }
  }

  /**
   * This exposes the raw localparams used by the FunctionQParser, it does <b>NOT</b>
   * wrap them in defaults from the request
   */
  public final SolrParams localParams;
  public final AggValueSource inner;
  
  public DebugAgg(FunctionQParser fp) throws SyntaxError { 
    super("debug");
    this.localParams = fp.getLocalParams();
    this.inner = fp.hasMoreArguments() ? fp.parseAgg(FunctionQParser.FLAG_IS_AGG) : new CountAgg();
  }

  @Override
  public SlotAcc createSlotAcc(FacetContext fcontext, int numDocs, int numSlots) throws IOException {
    return new Acc(fcontext, numDocs, numSlots, inner.createSlotAcc(fcontext, numDocs, numSlots));
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
    public static AtomicLong collectDocs = new AtomicLong(0);
    public static AtomicLong collectDocSets = new AtomicLong(0);
    public static Acc last;

    public SlotAcc sub;
    public int numDocs;
    public int numSlots;

    public Acc(FacetContext fcontext, int numDocs, int numSlots, SlotAcc sub) {
      super(fcontext);
      last = this;
      this.numDocs = numDocs;
      this.numSlots = numSlots;
      this.sub = sub;
      creates.addAndGet(1);
    }

    @Override
    public void collect(int doc, int slot, IntFunction<SlotContext> slotContext) throws IOException {
      collectDocs.addAndGet(1);
      sub.collect(doc, slot, slotContext);
    }
    

    @Override
    public int collect(DocSet docs, int slot, IntFunction<SlotContext> slotContext) throws IOException {
      collectDocSets.addAndGet(1);
      return sub.collect(docs, slot, slotContext);
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
    protected void resetIterators() throws IOException {
      sub.resetIterators();
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
    return inner.createFacetMerger(prototype);
  }

  /** A simple agg that just returns the number of shards contributing to a bucket */
  public static class DebugAggNumShards extends AggValueSource {
    public DebugAggNumShards() {
      super("debugNumShards");
    }
    
    @Override
    public SlotAcc createSlotAcc(FacetContext fcontext, int numDocs, int numSlots) {
      return new NumShardsAcc(fcontext, numDocs, numSlots);
    }
    
    @Override
    public int hashCode() {
      return 0;
    }
    
    @Override
    public String description() {
      return "debug(numShards)";
    }
    
    public static class NumShardsAcc extends SlotAcc {
      public NumShardsAcc(FacetContext fcontext, int numDocs, int numSlots) {
        super(fcontext);
      }
      
      @Override
      public void collect(int doc, int slot, IntFunction<SlotContext> slotContext) throws IOException {
        // No-Op
      }
      
      @Override
      public int compare(int slotA, int slotB) {
        return 0;
      }
      
      @Override
      public Object getValue(int slotNum) throws IOException {
        return 1L;
      }
      
      @Override
      public void reset() throws IOException {
        // No-Op
      }
      
      @Override
      public void resize(Resizer resizer) {
        // No-op
      }
      
    }
    
    @Override
    public FacetMerger createFacetMerger(Object prototype) {
      return new FacetModule.FacetLongMerger();
    }
    
  }
}
