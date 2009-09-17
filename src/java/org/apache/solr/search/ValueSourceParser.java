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

import java.util.*;
import java.io.IOException;

import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.search.Query;
import org.apache.lucene.index.IndexReader;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.SolrException;
import org.apache.solr.search.function.*;
import org.apache.solr.util.plugin.NamedListInitializedPlugin;
import org.apache.solr.schema.TrieDateField;
import org.apache.solr.schema.DateField;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.schema.LegacyDateField;

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
   * @throws ParseException
   */
  public abstract ValueSource parse(FunctionQParser fp) throws ParseException;

  /* standard functions */
  public static Map<String, ValueSourceParser> standardValueSourceParsers = new HashMap<String, ValueSourceParser>();
  static {
    standardValueSourceParsers.put("ord", new ValueSourceParser() {
      public ValueSource parse(FunctionQParser fp) throws ParseException {
        String field = fp.parseId();
        return new TopValueSource(new OrdFieldSource(field));
      }

      public void init(NamedList args) {
      }
      
    });
    standardValueSourceParsers.put("rord", new ValueSourceParser() {
      public ValueSource parse(FunctionQParser fp) throws ParseException {
        String field = fp.parseId();
        return new TopValueSource(new ReverseOrdFieldSource(field));
      }

      public void init(NamedList args) {
      }
      
    });
    standardValueSourceParsers.put("top", new ValueSourceParser() {
      public ValueSource parse(FunctionQParser fp) throws ParseException {
        ValueSource source = fp.parseValueSource();
        // nested top is redundant, and ord and rord get automatically wrapped
        if (source instanceof TopValueSource) return source;
        return new TopValueSource(source);
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
        return new TopValueSource(new ScaleFloatFunction(source,min,max));
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
        Float def = fp.hasMoreArguments() ? fp.parseFloat() : null;
        return new RangeMapFloatFunction(source,min,max,target,def);
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
    standardValueSourceParsers.put("sub", new ValueSourceParser() {
      public ValueSource parse(FunctionQParser fp) throws ParseException {
        ValueSource a = fp.parseValueSource();
        ValueSource b = fp.parseValueSource();
        return new DualFloatFunction(a,b) {
          protected String name() {
            return "sub";
          }
          protected float func(int doc, DocValues aVals, DocValues bVals) {
            return aVals.floatVal(doc) - bVals.floatVal(doc);
          }
        };
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
    standardValueSourceParsers.put("ms", new DateValueSourceParser());
  }

}




class DateValueSourceParser extends ValueSourceParser {
  DateField df = new TrieDateField();
  public void init(NamedList args) {}

  public Date getDate(FunctionQParser fp, String arg) {
    if (arg==null) return null;
    if (arg.startsWith("NOW") || (arg.length()>0 && Character.isDigit(arg.charAt(0)))) {
      return df.parseMathLenient(null, arg, fp.req);
    }
    return null;
  }

  public ValueSource getValueSource(FunctionQParser fp, String arg) {
    if (arg==null) return null;
    SchemaField f = fp.req.getSchema().getField(arg);
    if (f.getType().getClass() == DateField.class || f.getType().getClass() == LegacyDateField.class) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Can't use ms() function on non-numeric legacy date field " + arg);
    }
    return f.getType().getValueSource(f, fp);
  }

  public ValueSource parse(FunctionQParser fp) throws ParseException {
    String first = fp.parseArg();
    String second = fp.parseArg();
    if (first==null) first="NOW";

    Date d1=getDate(fp,first);
    ValueSource v1 = d1==null ? getValueSource(fp, first) : null;

    Date d2=getDate(fp,second);
    ValueSource v2 = d2==null ? getValueSource(fp, second) : null;

    // d     constant
    // v     field
    // dd    constant
    // dv    subtract field from constant
    // vd    subtract constant from field
    // vv    subtract fields

    final long ms1 = (d1 == null) ? 0 : d1.getTime();
    final long ms2 = (d2 == null) ? 0 : d2.getTime(); 

    // "d,dd" handle both constant cases

    if (d1 != null && v2==null) {
      return new LongConstValueSource(ms1-ms2);
    }

    // "v" just the date field
    if (v1 != null && v2==null && d2==null) {
      return v1;
    }


    // "dv"
    if (d1!=null && v2!=null)
      return new DualFloatFunction(new LongConstValueSource(ms1), v2) {
        protected String name() {
          return "ms";
        }
        protected float func(int doc, DocValues aVals, DocValues bVals) {
          return ms1 - bVals.longVal(doc);
        }
      };

    // "vd"
    if (v1!=null && d2!=null)
      return new DualFloatFunction(v1, new LongConstValueSource(ms2)) {
        protected String name() {
          return "ms";
        }
        protected float func(int doc, DocValues aVals, DocValues bVals) {
          return aVals.longVal(doc) - ms2;
        }
      };

    // "vv"
    if (v1!=null && v2!=null)
      return new DualFloatFunction(v1,v2) {
        protected String name() {
          return "ms";
        }
        protected float func(int doc, DocValues aVals, DocValues bVals) {
          return aVals.longVal(doc) - bVals.longVal(doc);
        }
      };

      return null; // shouldn't happen
  }

}


// Private for now - we need to revisit how to handle typing in function queries
class LongConstValueSource extends ValueSource {
  final long constant;

  public LongConstValueSource(long constant) {
    this.constant = constant;
  }

  public String description() {
    return "const(" + constant + ")";
  }

  public DocValues getValues(Map context, IndexReader reader) throws IOException {
    return new DocValues() {
      public float floatVal(int doc) {
        return constant;
      }
      public int intVal(int doc) {
        return (int)constant;
      }
      public long longVal(int doc) {
        return constant;
      }
      public double doubleVal(int doc) {
        return constant;
      }
      public String strVal(int doc) {
        return Long.toString(constant);
      }
      public String toString(int doc) {
        return description();
      }
    };
  }

  public int hashCode() {
    return (int)constant + (int)(constant>>>32);
  }

  public boolean equals(Object o) {
    if (LongConstValueSource.class != o.getClass()) return false;
    LongConstValueSource other = (LongConstValueSource)o;
    return this.constant == other.constant;
  }
}