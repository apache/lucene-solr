package org.apache.solr.core;
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

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.docvalues.DoubleDocValues;
import org.apache.lucene.queries.function.valuesource.DoubleConstValueSource;
import org.apache.solr.search.FunctionQParser;
import org.apache.solr.search.SyntaxError;
import org.apache.solr.search.ValueSourceParser;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A Mock ValueSource parser that produces ValueSources that returns a constant 
 * value butalso keeps track of how many times it was asked for a value for any 
 * document via a static map and a user defined key.
 **/
public class CountUsageValueSourceParser extends ValueSourceParser {

  private static final ConcurrentMap<String,AtomicInteger> counters 
    = new ConcurrentHashMap<>();

  public static void clearCounters() {
    counters.clear();
  }
  public static int getAndClearCount(String key) {
    AtomicInteger counter = counters.remove(key);
    if (null == counter) {
      throw new IllegalArgumentException("Key has never been used in function: " + key);
    }
    return counter.get();
  }

  @Override
  public ValueSource parse(FunctionQParser fp) throws SyntaxError {
    String key = fp.parseArg();
    double val = fp.parseDouble();
    
    AtomicInteger counter = new AtomicInteger();
    if (null != counters.putIfAbsent(key, counter)) {
      throw new IllegalArgumentException("Key has already been used: " + key);
    } 
    return new CountDocsValueSource(counter, val);
  }

  static final private class CountDocsValueSource extends DoubleConstValueSource {
    private final AtomicInteger counter;
    private final double value;
    public CountDocsValueSource(AtomicInteger counter, double value) {
      super(value);
      this.value = value;
      this.counter = counter;
    }
    @Override
    public FunctionValues getValues(Map context, LeafReaderContext readerContext) throws IOException {
      return new DoubleDocValues(this) {
        @Override
        public double doubleVal(int doc) {
          counter.incrementAndGet();
          return value;
        }
      };
    }
  }

}
