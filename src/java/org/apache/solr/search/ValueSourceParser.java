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
package org.apache.solr.search;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.search.Query;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.search.function.BoostedQuery;
import org.apache.solr.search.function.DivFloatFunction;
import org.apache.solr.search.function.DocValues;
import org.apache.solr.search.function.LinearFloatFunction;
import org.apache.solr.search.function.MaxFloatFunction;
import org.apache.solr.search.function.OrdFieldSource;
import org.apache.solr.search.function.PowFloatFunction;
import org.apache.solr.search.function.ProductFloatFunction;
import org.apache.solr.search.function.QueryValueSource;
import org.apache.solr.search.function.RangeMapFloatFunction;
import org.apache.solr.search.function.ReciprocalFloatFunction;
import org.apache.solr.search.function.ReverseOrdFieldSource;
import org.apache.solr.search.function.ScaleFloatFunction;
import org.apache.solr.search.function.SimpleFloatFunction;
import org.apache.solr.search.function.SumFloatFunction;
import org.apache.solr.search.function.ValueSource;
import org.apache.solr.util.plugin.NamedListInitializedPlugin;

/**
 * A factory that parses user queries to generate ValueSource instances.
 * Intented usage is to create pluggable, named functions for use in function queries.
 */
public abstract class ValueSourceParser implements NamedListInitializedPlugin
{
  
  /**
   * Initialize the plugin.
   */
  public abstract void init( NamedList args );
  
  /**
   * Parse the user input into a ValueSource.
   * 
   * @param fp
   * @return
   * @throws ParseException
   */
  public abstract ValueSource parse(FunctionQParser fp) throws ParseException;

  /* standard functions */
  public static Map<String, ValueSourceParser> standardValueSourceParsers = new HashMap<String, ValueSourceParser>();
  static {
    standardValueSourceParsers.put("ord", new ValueSourceParser() {
      public ValueSource parse(FunctionQParser fp) throws ParseException {
        String field = fp.parseId();
        return new OrdFieldSource(field);
      }

      public void init(NamedList args) {
      }
      
    });
    standardValueSourceParsers.put("rord", new ValueSourceParser() {
      public ValueSource parse(FunctionQParser fp) throws ParseException {
        String field = fp.parseId();
        return new ReverseOrdFieldSource(field);
      }

      public void init(NamedList args) {
      }
      
    });
    standardValueSourceParsers.put("linear", new ValueSourceParser() {
      public ValueSource parse(FunctionQParser fp) throws ParseException {
        ValueSource source = fp.parseValueSource();
        float slope = fp.parseFloat();
        float intercept = fp.parseFloat();
        return new LinearFloatFunction(source,slope,intercept);
      }

      public void init(NamedList args) {
      }
      
    });
    standardValueSourceParsers.put("max", new ValueSourceParser() {
      public ValueSource parse(FunctionQParser fp) throws ParseException {
        ValueSource source = fp.parseValueSource();
        float val = fp.parseFloat();
        return new MaxFloatFunction(source,val);
      }

      public void init(NamedList args) {
      }
      
    });
    standardValueSourceParsers.put("recip", new ValueSourceParser() {
      public ValueSource parse(FunctionQParser fp) throws ParseException {
        ValueSource source = fp.parseValueSource();
        float m = fp.parseFloat();
        float a = fp.parseFloat();
        float b = fp.parseFloat();
        return new ReciprocalFloatFunction(source,m,a,b);
      }

      public void init(NamedList args) {
      }
      
    });
    standardValueSourceParsers.put("scale", new ValueSourceParser() {
      public ValueSource parse(FunctionQParser fp) throws ParseException {
        ValueSource source = fp.parseValueSource();
        float min = fp.parseFloat();
        float max = fp.parseFloat();
        return new ScaleFloatFunction(source,min,max);
      }

      public void init(NamedList args) {
      }
      
    });
    standardValueSourceParsers.put("pow", new ValueSourceParser() {
      public ValueSource parse(FunctionQParser fp) throws ParseException {
        ValueSource a = fp.parseValueSource();
        ValueSource b = fp.parseValueSource();
        return new PowFloatFunction(a,b);
      }

      public void init(NamedList args) {
      }
      
    });
    standardValueSourceParsers.put("div", new ValueSourceParser() {
      public ValueSource parse(FunctionQParser fp) throws ParseException {
        ValueSource a = fp.parseValueSource();
        ValueSource b = fp.parseValueSource();
        return new DivFloatFunction(a,b);
      }

      public void init(NamedList args) {
      }
      
    });
    standardValueSourceParsers.put("map", new ValueSourceParser() {
      public ValueSource parse(FunctionQParser fp) throws ParseException {
        ValueSource source = fp.parseValueSource();
        float min = fp.parseFloat();
        float max = fp.parseFloat();
        float target = fp.parseFloat();
        return new RangeMapFloatFunction(source,min,max,target);
      }

      public void init(NamedList args) {
      }
      
    });
    standardValueSourceParsers.put("sqrt", new ValueSourceParser() {
      public ValueSource parse(FunctionQParser fp) throws ParseException {
        ValueSource source = fp.parseValueSource();
        return new SimpleFloatFunction(source) {
          protected String name() {
            return "sqrt";
          }
          protected float func(int doc, DocValues vals) {
            return (float)Math.sqrt(vals.floatVal(doc));
          }
        };
      }
      public void init(NamedList args) {
      }
    });
    standardValueSourceParsers.put("log", new ValueSourceParser() {
      public ValueSource parse(FunctionQParser fp) throws ParseException {
        ValueSource source = fp.parseValueSource();
        return new SimpleFloatFunction(source) {
          protected String name() {
            return "log";
          }
          protected float func(int doc, DocValues vals) {
            return (float)Math.log10(vals.floatVal(doc));
          }
        };
      }

      public void init(NamedList args) {
      }
      
    });
    standardValueSourceParsers.put("abs", new ValueSourceParser() {
      public ValueSource parse(FunctionQParser fp) throws ParseException {
        ValueSource source = fp.parseValueSource();
        return new SimpleFloatFunction(source) {
          protected String name() {
            return "abs";
          }
          protected float func(int doc, DocValues vals) {
            return (float)Math.abs(vals.floatVal(doc));
          }
        };
      }

      public void init(NamedList args) {
      }
      
    });
    standardValueSourceParsers.put("sum", new ValueSourceParser() {
      public ValueSource parse(FunctionQParser fp) throws ParseException {
        List<ValueSource> sources = fp.parseValueSourceList();
        return new SumFloatFunction(sources.toArray(new ValueSource[sources.size()]));
      }

      public void init(NamedList args) {
      }
      
    });
    standardValueSourceParsers.put("product", new ValueSourceParser() {
      public ValueSource parse(FunctionQParser fp) throws ParseException {
        List<ValueSource> sources = fp.parseValueSourceList();
        return new ProductFloatFunction(sources.toArray(new ValueSource[sources.size()]));
      }

      public void init(NamedList args) {
      }
      
    });
    standardValueSourceParsers.put("query", new ValueSourceParser() {
      // boost(query($q),rating)
      public ValueSource parse(FunctionQParser fp) throws ParseException {
        Query q = fp.parseNestedQuery();
        float defVal = 0.0f;
        if (fp.hasMoreArguments()) {
          defVal = fp.parseFloat();
        }
        return new QueryValueSource(q, defVal);
      }

      public void init(NamedList args) {
      }
      
    });
    standardValueSourceParsers.put("boost", new ValueSourceParser() {
      public ValueSource parse(FunctionQParser fp) throws ParseException {
        Query q = fp.parseNestedQuery();
        ValueSource vs = fp.parseValueSource();
        BoostedQuery bq = new BoostedQuery(q, vs);
        return new QueryValueSource(bq, 0.0f);
      }

      public void init(NamedList args) {
      }
      
    });
  }

}
