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

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
import java.io.IOException;
import java.util.Map;

public abstract class SimpleAggValueSource extends AggValueSource {
  ValueSource arg;

  public SimpleAggValueSource(String name, ValueSource vs) {
    super(name);
    this.arg = vs;
  }

  public ValueSource getArg() {
    return arg;
  }

  @Override
  @SuppressWarnings({"rawtypes"})
  public FunctionValues getValues(Map context, LeafReaderContext readerContext) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean equals(Object o) {
    if (!super.equals(o)) return false;
    ValueSource otherArg = ((SimpleAggValueSource)o).arg;
    if (arg == otherArg) return true;
    return (arg != null && arg.equals(otherArg));
  }

  @Override
  public int hashCode() {
    return getClass().hashCode() + (arg == null ? 0 : arg.hashCode());
  }

  @Override
  public String description() {
    return name() + "(" + (arg==null ? "" : arg.description()) + ")";
  }

}


