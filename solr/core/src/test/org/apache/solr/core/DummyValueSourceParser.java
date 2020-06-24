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
package org.apache.solr.core;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.valuesource.SimpleFloatFunction;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.search.FunctionQParser;
import org.apache.solr.search.SyntaxError;
import org.apache.solr.search.ValueSourceParser;


/**
 * Mock ValueSource parser that doesn't do much of anything
 *
 **/
public class DummyValueSourceParser extends ValueSourceParser {
  @SuppressWarnings({"rawtypes"})
  private NamedList args;

  @Override
  public void init(@SuppressWarnings({"rawtypes"})NamedList args) {
    this.args = args;
  }

  @Override
  public ValueSource parse(FunctionQParser fp) throws SyntaxError {
    ValueSource source = fp.parseValueSource();
    ValueSource result = new SimpleFloatFunction(source) {
      @Override
      protected String name() {
        return "foo";
      }

      @Override
      protected float func(int doc, FunctionValues vals) {
        float result = 0;
        return result;
      }
    };
    return result;
  }


}
