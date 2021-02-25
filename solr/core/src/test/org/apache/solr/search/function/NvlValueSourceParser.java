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
package org.apache.solr.search.function;

import java.io.IOException;

import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.valuesource.SimpleFloatFunction;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.search.FunctionQParser;
import org.apache.solr.search.SyntaxError;
import org.apache.solr.search.ValueSourceParser;

/**
 * A sample ValueSourceParser for testing. Approximates the oracle NVL function,
 * letting you substitute a value when a "null" is encountered. In this case,
 * null is approximated by a float value, since ValueSource always returns a
 * float, even if the field is undefined for a document.
 * 
 * Initialization parameters:
 *  - nvlFloatValue: float value to consider as "NULL" when seen in a field. defaults to 0.0f.
 *  
 * Example:
 *   nvl(vs,2)   will return 2 if the vs is NULL (as defined by nvlFloatValue above) or the doc value otherwise
 * 
 */
public class NvlValueSourceParser extends ValueSourceParser {
    
    /**
     * Value to consider "null" when found in a ValueSource Defaults to 0.0
     */
    private float nvlFloatValue = 0.0f;

    @Override
    public ValueSource parse(FunctionQParser fp) throws SyntaxError {
      ValueSource source = fp.parseValueSource();
      final float nvl = fp.parseFloat();

      return new SimpleFloatFunction(source) {
        @Override
      protected String name() {
          return "nvl";
        }

        @Override
        protected float func(int doc, FunctionValues vals) throws IOException {
          float v = vals.floatVal(doc);
          if (v == nvlFloatValue) {
            return nvl;
          } else {
            return v;
          }
        }
      };
    }

  @Override
  public void init(@SuppressWarnings({"rawtypes"})NamedList args) {
    /* initialize the value to consider as null */
    Float nvlFloatValueArg = (Float) args.get("nvlFloatValue");
    if (nvlFloatValueArg != null) {
      this.nvlFloatValue = nvlFloatValueArg;
    }
  }
}
