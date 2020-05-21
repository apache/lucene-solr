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
import java.util.Map;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;

public abstract class AggValueSource extends ValueSource {
  protected String name;

  public AggValueSource(String name) {
    this.name = name;
  }

  public String name() {
    return this.name;
  }

  public ValueSource[] getChildren() {
    return null;
  }

  @Override
  public boolean equals(Object o) {
    return this.getClass() == o.getClass() && name.equals(((AggValueSource) o).name);
  }

  @Override
  @SuppressWarnings({"rawtypes"})
  public FunctionValues getValues(Map context, LeafReaderContext readerContext) throws IOException {
    // FUTURE
    throw new UnsupportedOperationException("NOT IMPLEMENTED " + name + " " + this);
  }

  // TODO: make abstract
  public SlotAcc createSlotAcc(FacetContext fcontext, int numDocs, int numSlots) throws IOException {
    throw new UnsupportedOperationException("NOT IMPLEMENTED " + name + " " + this);
  }

  public abstract FacetMerger createFacetMerger(Object prototype);

}

