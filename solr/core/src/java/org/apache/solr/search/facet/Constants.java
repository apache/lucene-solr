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
import org.apache.lucene.index.LeafReaderContext;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.search.DocSet;

/** constants used in facets package */
public class Constants {
  public static final SlotAcc.CountSlotAcc DEV_NULL_SLOT_ACC = new DevNullCountSlotAcc();

  private Constants() {}

  /**
   * This CountSlotAcc exists as a /dev/null sink for callers of collect(...) and other "write"-type
   * methods. It should be used in contexts where "read"-type access methods will never be called.
   */
  private static class DevNullCountSlotAcc extends SlotAcc.CountSlotAcc {

    public DevNullCountSlotAcc() {
      super(null);
    }

    @Override
    public void resize(Resizer resizer) {
      // No-op
    }

    @Override
    public void reset() throws IOException {
      // No-op
    }

    @Override
    public void collect(int doc, int slot, IntFunction<SlotContext> slotContext) throws IOException {
      // No-op
    }

    @Override
    public void incrementCount(int slot, int count) {
      // No-op
    }

    @Override
    public void setNextReader(LeafReaderContext readerContext) throws IOException {
      // No-op
    }

    @Override
    public int collect(DocSet docs, int slot, IntFunction<SlotContext> slotContext) throws IOException {
      return docs.size(); // dressed up no-op
    }

    @Override
    public Object getValue(int slotNum) throws IOException {
      throw new UnsupportedOperationException("not supported");
    }

    @Override
    public int compare(int slotA, int slotB) {
      throw new UnsupportedOperationException("not supported");
    }

    @Override
    public void setValues(SimpleOrderedMap<Object> bucket, int slotNum) throws IOException {
      throw new UnsupportedOperationException("not supported");
    }

    @Override
    public int getCount(int slot) {
      throw new UnsupportedOperationException("not supported");
    }
  }
}
