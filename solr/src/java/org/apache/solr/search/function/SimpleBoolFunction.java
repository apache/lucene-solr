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

package org.apache.solr.search.function;

import org.apache.lucene.index.IndexReader.AtomicReaderContext;
import org.apache.lucene.search.IndexSearcher;

import java.io.IOException;
import java.util.Map;


public abstract class SimpleBoolFunction extends BoolFunction {
  protected final ValueSource source;

  public SimpleBoolFunction(ValueSource source) {
    this.source = source;
  }

  protected abstract String name();

  protected abstract boolean func(int doc, DocValues vals);

  @Override
  public BoolDocValues getValues(Map context, AtomicReaderContext readerContext) throws IOException {
    final DocValues vals =  source.getValues(context, readerContext);
    return new BoolDocValues(this) {
      @Override
      public boolean boolVal(int doc) {
	      return func(doc, vals);
      }
      @Override
      public String toString(int doc) {
	      return name() + '(' + vals.toString(doc) + ')';
      }
    };
  }

  @Override
  public String description() {
    return name() + '(' + source.description() + ')';
  }

  @Override
  public int hashCode() {
    return source.hashCode() + name().hashCode();
  }

  @Override
  public boolean equals(Object o) {
    if (this.getClass() != o.getClass()) return false;
    SingleFunction other = (SingleFunction)o;
    return this.source.equals(other.source);
  }

  @Override
  public void createWeight(Map context, IndexSearcher searcher) throws IOException {
    source.createWeight(context, searcher);
  }
}