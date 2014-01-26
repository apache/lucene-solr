package org.apache.lucene.server;

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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.facet.sortedset.SortedSetDocValuesReaderState;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Weight;

// nocommit remove this: it's ridiculous if we can't use
// Lucene's IndexSearcher

/** Extends {@link IndexSearcher}, adding state and public
 *  methods. */
public class MyIndexSearcher extends IndexSearcher {

  // nocommit move this to an external class that hasa this
  // and hasa IndexSearcher:
  /** Maps each SSDV facets field to its reader state. */
  public final Map<String,SortedSetDocValuesReaderState> ssdvStates;

  /** Sole constructor. */
  public MyIndexSearcher(IndexReader r, IndexState state) throws IOException {
    super(r);

    ssdvStates = new HashMap<String,SortedSetDocValuesReaderState>();
    for (FieldDef field : state.getAllFields().values()) {
      if ("sortedSetDocValues".equals(field.faceted)) {
        String indexFieldName = state.facetsConfig.getDimConfig(field.name).indexFieldName;
        if (ssdvStates.containsKey(indexFieldName) == false) {
          // TODO: log how long this took
          ssdvStates.put(indexFieldName, new SortedSetDocValuesReaderState(r, indexFieldName));
        }
      }
    }
  }

  // nocommit ugly that we need to do this, to handle the
  // in order vs out of order chicken/egg issue:

  /** Runs a search, from a provided {@link Weight} and
   *  {@link Collector}; this method is not public in
   *  {@link IndexSearcher}. */
  public void search(Weight w, Collector c) throws IOException {
    super.search(getIndexReader().leaves(), w, c);
  }
}
