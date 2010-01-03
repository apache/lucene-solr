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

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Searcher;
import org.apache.lucene.search.spell.JaroWinklerDistance;
import org.apache.lucene.search.spell.LevensteinDistance;
import org.apache.lucene.search.spell.NGramDistance;
import org.apache.lucene.search.spell.StringDistance;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.schema.DateField;
import org.apache.solr.schema.LegacyDateField;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.schema.TrieDateField;
import org.apache.solr.search.function.*;

import org.apache.solr.search.function.distance.*;
import org.apache.solr.util.plugin.NamedListInitializedPlugin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Collections;

/**
 * A factory that parses user queries to generate ValueSource instances.
 * Intented usage is to create pluggable, named functions for use in function queries.
 */
public abstract class ValueSourceParser implements NamedListInitializedPlugin {
  /**
   * Initialize the plugin.
   */
  public void init(NamedList args) {}

  /**
   * Parse the user input into a ValueSource.
   *
   * @param fp
   * @throws ParseException
   */
  public abstract ValueSource parse(FunctionQParser fp) throws ParseException;

  /* standard functions */
  public static Map<String, ValueSourceParser> standardValueSourceParsers = new HashMap<String, ValueSourceParser>();

  /** Adds a new parser for the name and returns any existing one that was overriden.
   *  This is not thread safe.
   */
  public static ValueSourceParser addParser(String name, ValueSourceParser p) {
    return standardValueSourceParsers.put(name, p);
  }

  /** Adds a new parser for the name and returns any existing one that was overriden.
   *  This is not thread safe.
   */
  public static ValueSourceParser addParser(NamedParser p) {
    return standardValueSourceParsers.put(p.name(), p);
  }

  private static void alias(String source, String dest) {
    standardValueSourceParsers.put(dest, standardValueSourceParsers.get(source));
  }

  static {
    addParser("ord", new ValueSourceParser() {
      public ValueSource parse(FunctionQParser fp) throws ParseException {
        String field = fp.parseId();
        return new TopValueSource(new OrdFieldSource(field));
      }
    });
    addParser("literal", new ValueSourceParser() {
      public ValueSource parse(FunctionQParser fp) throws ParseException {
        return new LiteralValueSource(fp.getString());
      }
    });
    addParser("rord", new ValueSourceParser() {
      public ValueSource parse(FunctionQParser fp) throws ParseException {
        String field = fp.parseId();
        return new TopValueSource(new ReverseOrdFieldSource(field));
      }
    });
    addParser("top", new ValueSourceParser() {
      public ValueSource parse(FunctionQParser fp) throws ParseException {
        ValueSource source = fp.parseValueSource();
        // nested top is redundant, and ord and rord get automatically wrapped
        if (source instanceof TopValueSource) return source;
        return new TopValueSource(source);
      }
    });
    addParser("linear", new ValueSourceParser() {
      public ValueSource parse(FunctionQParser fp) throws ParseException {
        ValueSource source = fp.parseValueSource();
        float slope = fp.parseFloat();
        float intercept = fp.parseFloat();
        return new LinearFloatFunction(source, slope, intercept);
      }
    });
    addParser("max", new ValueSourceParser() {
      public ValueSource parse(FunctionQParser fp) throws ParseException {
        ValueSource source = fp.parseValueSource();
        float val = fp.parseFloat();
        return new MaxFloatFunction(source, val);
      }
    });
    addParser("recip", new ValueSourceParser() {
      public ValueSource parse(FunctionQParser fp) throws ParseException {
        ValueSource source = fp.parseValueSource();
        float m = fp.parseFloat();
        float a = fp.parseFloat();
        float b = fp.parseFloat();
        return new ReciprocalFloatFunction(source, m, a, b);
      }
    });
    addParser("scale", new ValueSourceParser() {
      public ValueSource parse(FunctionQParser fp) throws ParseException {
        ValueSource source = fp.parseValueSource();
        float min = fp.parseFloat();
        float max = fp.parseFloat();
        return new TopValueSource(new ScaleFloatFunction(source, min, max));
      }
    });
    addParser("div", new ValueSourceParser() {
      public ValueSource parse(FunctionQParser fp) throws ParseException {
        ValueSource a = fp.parseValueSource();
        ValueSource b = fp.parseValueSource();
        return new DivFloatFunction(a, b);
      }
    });
    addParser("map", new ValueSourceParser() {
      public ValueSource parse(FunctionQParser fp) throws ParseException {
        ValueSource source = fp.parseValueSource();
        float min = fp.parseFloat();
        float max = fp.parseFloat();
        float target = fp.parseFloat();
        Float def = fp.hasMoreArguments() ? fp.parseFloat() : null;
        return new RangeMapFloatFunction(source, min, max, target, def);
      }
    });

    addParser("abs", new ValueSourceParser() {
      public ValueSource parse(FunctionQParser fp) throws ParseException {
        ValueSource source = fp.parseValueSource();
        return new SimpleFloatFunction(source) {
          protected String name() {
            return "abs";
          }

          protected float func(int doc, DocValues vals) {
            return Math.abs(vals.floatVal(doc));
          }
        };
      }
    });
    addParser("sum", new ValueSourceParser() {
      public ValueSource parse(FunctionQParser fp) throws ParseException {
        List<ValueSource> sources = fp.parseValueSourceList();
        return new SumFloatFunction(sources.toArray(new ValueSource[sources.size()]));
      }
    });
    alias("sum","add");    

    addParser("product", new ValueSourceParser() {
      public ValueSource parse(FunctionQParser fp) throws ParseException {
        List<ValueSource> sources = fp.parseValueSourceList();
        return new ProductFloatFunction(sources.toArray(new ValueSource[sources.size()]));
      }
    });
    alias("product","mul");

    addParser("sub", new ValueSourceParser() {
      public ValueSource parse(FunctionQParser fp) throws ParseException {
        ValueSource a = fp.parseValueSource();
        ValueSource b = fp.parseValueSource();
        return new DualFloatFunction(a, b) {
          protected String name() {
            return "sub";
          }

          protected float func(int doc, DocValues aVals, DocValues bVals) {
            return aVals.floatVal(doc) - bVals.floatVal(doc);
          }
        };
      }
    });
    addParser("vector", new ValueSourceParser(){
      public ValueSource parse(FunctionQParser fp) throws ParseException{
        return new VectorValueSource(fp.parseValueSourceList());
      }
    });
    addParser("query", new ValueSourceParser() {
      // boost(query($q),rating)
      public ValueSource parse(FunctionQParser fp) throws ParseException {
        Query q = fp.parseNestedQuery();
        float defVal = 0.0f;
        if (fp.hasMoreArguments()) {
          defVal = fp.parseFloat();
        }
        return new QueryValueSource(q, defVal);
      }
    });
    addParser("boost", new ValueSourceParser() {
      public ValueSource parse(FunctionQParser fp) throws ParseException {
        Query q = fp.parseNestedQuery();
        ValueSource vs = fp.parseValueSource();
        BoostedQuery bq = new BoostedQuery(q, vs);
        return new QueryValueSource(bq, 0.0f);
      }
    });
    addParser("hsin", new ValueSourceParser() {
      public ValueSource parse(FunctionQParser fp) throws ParseException {

        double radius = fp.parseDouble();
        MultiValueSource pv1;
        MultiValueSource pv2;

        ValueSource one = fp.parseValueSource();
        ValueSource two = fp.parseValueSource();
        if (fp.hasMoreArguments()) {
          List<ValueSource> s1 = new ArrayList<ValueSource>();
          s1.add(one);
          s1.add(two);
          pv1 = new VectorValueSource(s1);
          ValueSource x2 = fp.parseValueSource();
          ValueSource y2 = fp.parseValueSource();
          List<ValueSource> s2 = new ArrayList<ValueSource>();
          s2.add(x2);
          s2.add(y2);
          pv2 = new VectorValueSource(s2);
        } else {
          //check to see if we have multiValue source
          if (one instanceof MultiValueSource && two instanceof MultiValueSource){
            pv1 = (MultiValueSource) one;
            pv2 = (MultiValueSource) two;
          } else {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
                    "Input must either be 2 MultiValueSources, or there must be 4 ValueSources");
          }
        }
        boolean convert = false;
        if (fp.hasMoreArguments()){
          convert = Boolean.parseBoolean(fp.parseArg());
        }
        return new HaversineFunction(pv1, pv2, radius, convert);
      }
    });

    addParser("ghhsin", new ValueSourceParser() {
      public ValueSource parse(FunctionQParser fp) throws ParseException {
        double radius = fp.parseDouble();

        ValueSource gh1 = fp.parseValueSource();
        ValueSource gh2 = fp.parseValueSource();

        return new GeohashHaversineFunction(gh1, gh2, radius);
      }
    });

    addParser("geohash", new ValueSourceParser() {
      public ValueSource parse(FunctionQParser fp) throws ParseException {

        ValueSource lat = fp.parseValueSource();
        ValueSource lon = fp.parseValueSource();

        return new GeohashFunction(lat, lon);
      }
    });
    addParser("strdist", new ValueSourceParser() {
      public ValueSource parse(FunctionQParser fp) throws ParseException {

        ValueSource str1 = fp.parseValueSource();
        ValueSource str2 = fp.parseValueSource();
        String distClass = fp.parseArg();

        StringDistance dist = null;
        if (distClass.equalsIgnoreCase("jw")) {
          dist = new JaroWinklerDistance();
        } else if (distClass.equalsIgnoreCase("edit")) {
          dist = new LevensteinDistance();
        } else if (distClass.equalsIgnoreCase("ngram")) {
          int ngram = 2;
          if (fp.hasMoreArguments()) {
            ngram = fp.parseInt();
          }
          dist = new NGramDistance(ngram);
        } else {
          dist = (StringDistance) fp.req.getCore().getResourceLoader().newInstance(distClass);
        }
        return new StringDistanceFunction(str1, str2, dist);
      }
    });

    addParser(new DoubleParser("rad") {
      public double func(int doc, DocValues vals) {
        return vals.doubleVal(doc) * DistanceUtils.DEGREES_TO_RADIANS;
      }
    });
    addParser(new DoubleParser("deg") {
      public double func(int doc, DocValues vals) {
        return vals.doubleVal(doc) * DistanceUtils.RADIANS_TO_DEGREES;
      }
    });
    addParser(new DoubleParser("sqrt") {
      public double func(int doc, DocValues vals) {
        return Math.sqrt(vals.doubleVal(doc));
      }
    });
    addParser(new DoubleParser("cbrt") {
      public double func(int doc, DocValues vals) {
        return Math.cbrt(vals.doubleVal(doc));
      }
    });
    addParser(new DoubleParser("log") {
      public double func(int doc, DocValues vals) {
        return Math.log10(vals.doubleVal(doc));
      }
    });
    addParser(new DoubleParser("ln") {
      public double func(int doc, DocValues vals) {
        return Math.log(vals.doubleVal(doc));
      }
    });
    addParser(new DoubleParser("exp") {
      public double func(int doc, DocValues vals) {
        return Math.exp(vals.doubleVal(doc));
      }
    });
    addParser(new DoubleParser("sin") {
      public double func(int doc, DocValues vals) {
        return Math.sin(vals.doubleVal(doc));
      }
    });
    addParser(new DoubleParser("cos") {
      public double func(int doc, DocValues vals) {
        return Math.cos(vals.doubleVal(doc));
      }
    });
    addParser(new DoubleParser("tan") {
      public double func(int doc, DocValues vals) {
        return Math.tan(vals.doubleVal(doc));
      }
    });
    addParser(new DoubleParser("asin") {
      public double func(int doc, DocValues vals) {
        return Math.asin(vals.doubleVal(doc));
      }
    });
    addParser(new DoubleParser("acos") {
      public double func(int doc, DocValues vals) {
        return Math.acos(vals.doubleVal(doc));
      }
    });
    addParser(new DoubleParser("atan") {
      public double func(int doc, DocValues vals) {
        return Math.atan(vals.doubleVal(doc));
      }
    });
    addParser(new DoubleParser("sinh") {
      public double func(int doc, DocValues vals) {
        return Math.sinh(vals.doubleVal(doc));
      }
    });
    addParser(new DoubleParser("cosh") {
      public double func(int doc, DocValues vals) {
        return Math.cosh(vals.doubleVal(doc));
      }
    });
    addParser(new DoubleParser("tanh") {
      public double func(int doc, DocValues vals) {
        return Math.tanh(vals.doubleVal(doc));
      }
    });
    addParser(new DoubleParser("ceil") {
      public double func(int doc, DocValues vals) {
        return Math.ceil(vals.doubleVal(doc));
      }
    });
    addParser(new DoubleParser("floor") {
      public double func(int doc, DocValues vals) {
        return Math.floor(vals.doubleVal(doc));
      }
    });
    addParser(new DoubleParser("rint") {
      public double func(int doc, DocValues vals) {
        return Math.rint(vals.doubleVal(doc));
      }
    });
    addParser(new Double2Parser("pow") {
      public double func(int doc, DocValues a, DocValues b) {
        return Math.pow(a.doubleVal(doc), b.doubleVal(doc));
      }
    });
    addParser(new Double2Parser("hypot") {
      public double func(int doc, DocValues a, DocValues b) {
        return Math.hypot(a.doubleVal(doc), b.doubleVal(doc));
      }
    });
    addParser(new Double2Parser("atan2") {
      public double func(int doc, DocValues a, DocValues b) {
        return Math.atan2(a.doubleVal(doc), b.doubleVal(doc));
      }
    });

    addParser("sqedist", new ValueSourceParser() {
      public ValueSource parse(FunctionQParser fp) throws ParseException {
        List<ValueSource> sources = fp.parseValueSourceList();
        MVResult mvr = getMultiValueSources(sources);

        return new SquaredEuclideanFunction(mvr.mv1, mvr.mv2);
      }
    });

    addParser("dist", new ValueSourceParser() {
      public ValueSource parse(FunctionQParser fp) throws ParseException {
        float power = fp.parseFloat();
        List<ValueSource> sources = fp.parseValueSourceList();
        MVResult mvr = getMultiValueSources(sources);
        return new VectorDistanceFunction(power, mvr.mv1, mvr.mv2);
      }
    });
    addParser("ms", new DateValueSourceParser());

    
    addParser("pi", new ValueSourceParser() {
      public ValueSource parse(FunctionQParser fp) throws ParseException {
        return new DoubleConstValueSource(Math.PI);
      }
    });
    addParser("e", new ValueSourceParser() {
      public ValueSource parse(FunctionQParser fp) throws ParseException {
        return new DoubleConstValueSource(Math.E);
      }
    });
  }

  private static void splitSources(int dim, List<ValueSource> sources, List<ValueSource> dest1, List<ValueSource> dest2) {
    //Get dim value sources for the first vector
    for (int i = 0; i < dim; i++) {
      dest1.add(sources.get(i));
    }
    //Get dim value sources for the second vector
    for (int i = dim; i < sources.size(); i++) {
      dest2.add(sources.get(i));
    }
  }

  private static MVResult getMultiValueSources(List<ValueSource> sources) {
    MVResult mvr = new MVResult();
    if (sources.size() % 2 != 0) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Illegal number of sources.  There must be an even number of sources");
    }
    if (sources.size() == 2) {

      //check to see if these are MultiValueSource
      boolean s1MV = sources.get(0) instanceof MultiValueSource;
      boolean s2MV = sources.get(1) instanceof MultiValueSource;
      if (s1MV && s2MV) {
        mvr.mv1 = (MultiValueSource) sources.get(0);
        mvr.mv2 = (MultiValueSource) sources.get(1);
      } else if (s1MV ||
              s2MV) {
        //if one is a MultiValueSource, than the other one needs to be too.
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Illegal number of sources.  There must be an even number of sources");
      } else {
        mvr.mv1 = new VectorValueSource(Collections.singletonList(sources.get(0)));
        mvr.mv2 = new VectorValueSource(Collections.singletonList(sources.get(1)));
      }
    } else {
      int dim = sources.size() / 2;
      List<ValueSource> sources1 = new ArrayList<ValueSource>(dim);
      List<ValueSource> sources2 = new ArrayList<ValueSource>(dim);
      //Get dim value sources for the first vector
      splitSources(dim, sources, sources1, sources2);
      mvr.mv1 = new VectorValueSource(sources1);
      mvr.mv2 = new VectorValueSource(sources2);
    }

    return mvr;
  }

  private static class MVResult {
    MultiValueSource mv1;
    MultiValueSource mv2;
  }
}


class DateValueSourceParser extends ValueSourceParser {
  DateField df = new TrieDateField();

  public void init(NamedList args) {
  }

  public Date getDate(FunctionQParser fp, String arg) {
    if (arg == null) return null;
    if (arg.startsWith("NOW") || (arg.length() > 0 && Character.isDigit(arg.charAt(0)))) {
      return df.parseMathLenient(null, arg, fp.req);
    }
    return null;
  }

  public ValueSource getValueSource(FunctionQParser fp, String arg) {
    if (arg == null) return null;
    SchemaField f = fp.req.getSchema().getField(arg);
    if (f.getType().getClass() == DateField.class || f.getType().getClass() == LegacyDateField.class) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Can't use ms() function on non-numeric legacy date field " + arg);
    }
    return f.getType().getValueSource(f, fp);
  }

  public ValueSource parse(FunctionQParser fp) throws ParseException {
    String first = fp.parseArg();
    String second = fp.parseArg();
    if (first == null) first = "NOW";

    Date d1 = getDate(fp, first);
    ValueSource v1 = d1 == null ? getValueSource(fp, first) : null;

    Date d2 = getDate(fp, second);
    ValueSource v2 = d2 == null ? getValueSource(fp, second) : null;

    // d     constant
    // v     field
    // dd    constant
    // dv    subtract field from constant
    // vd    subtract constant from field
    // vv    subtract fields

    final long ms1 = (d1 == null) ? 0 : d1.getTime();
    final long ms2 = (d2 == null) ? 0 : d2.getTime();

    // "d,dd" handle both constant cases

    if (d1 != null && v2 == null) {
      return new LongConstValueSource(ms1 - ms2);
    }

    // "v" just the date field
    if (v1 != null && v2 == null && d2 == null) {
      return v1;
    }


    // "dv"
    if (d1 != null && v2 != null)
      return new DualFloatFunction(new LongConstValueSource(ms1), v2) {
        protected String name() {
          return "ms";
        }

        protected float func(int doc, DocValues aVals, DocValues bVals) {
          return ms1 - bVals.longVal(doc);
        }
      };

    // "vd"
    if (v1 != null && d2 != null)
      return new DualFloatFunction(v1, new LongConstValueSource(ms2)) {
        protected String name() {
          return "ms";
        }

        protected float func(int doc, DocValues aVals, DocValues bVals) {
          return aVals.longVal(doc) - ms2;
        }
      };

    // "vv"
    if (v1 != null && v2 != null)
      return new DualFloatFunction(v1, v2) {
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
  final double dv;
  final float fv;

  public LongConstValueSource(long constant) {
    this.constant = constant;
    this.dv = constant;
    this.fv = constant;
  }

  public String description() {
    return "const(" + constant + ")";
  }

  public DocValues getValues(Map context, IndexReader reader) throws IOException {
    return new DocValues() {
      public float floatVal(int doc) {
        return fv;
      }

      public int intVal(int doc) {
        return (int) constant;
      }

      public long longVal(int doc) {
        return constant;
      }

      public double doubleVal(int doc) {
        return dv;
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
    return (int) constant + (int) (constant >>> 32);
  }

  public boolean equals(Object o) {
    if (LongConstValueSource.class != o.getClass()) return false;
    LongConstValueSource other = (LongConstValueSource) o;
    return this.constant == other.constant;
  }
}

// Private for now - we need to revisit how to handle typing in function queries
class DoubleConstValueSource extends ValueSource {
  final double constant;
  private final float fv;
  private final long lv;

  public DoubleConstValueSource(double constant) {
    this.constant = constant;
    this.fv = (float)constant;
    this.lv = (long)constant;
  }

  public String description() {
    return "const(" + constant + ")";
  }

  public DocValues getValues(Map context, IndexReader reader) throws IOException {
    return new DocValues() {
      public float floatVal(int doc) {
        return fv;
      }

      public int intVal(int doc) {
        return (int) lv;
      }

      public long longVal(int doc) {
        return lv;
      }

      public double doubleVal(int doc) {
        return constant;
      }

      public String strVal(int doc) {
        return Double.toString(constant);
      }

      public String toString(int doc) {
        return description();
      }
    };
  }

  public int hashCode() {
    long bits = Double.doubleToRawLongBits(constant);
    return (int)(bits ^ (bits >>> 32));
  }

  public boolean equals(Object o) {
    if (DoubleConstValueSource.class != o.getClass()) return false;
    DoubleConstValueSource other = (DoubleConstValueSource) o;
    return this.constant == other.constant;
  }
}


abstract class NamedParser extends ValueSourceParser {
  private final String name;
  public NamedParser(String name) {
    this.name = name;
  }
  public String name() {
    return name;
  }
}


abstract class DoubleParser extends NamedParser {
  public DoubleParser(String name) {
    super(name);
  }

  public abstract double func(int doc, DocValues vals);

  public ValueSource parse(FunctionQParser fp) throws ParseException {
    return new Function(fp.parseValueSource());
  }

  class Function extends SingleFunction {
    public Function(ValueSource source) {
      super(source);
    }

    public String name() {
      return DoubleParser.this.name();
    }

    @Override
    public DocValues getValues(Map context, IndexReader reader) throws IOException {
      final DocValues vals =  source.getValues(context, reader);
      return new DocValues() {
        public float floatVal(int doc) {
          return (float)doubleVal(doc);
        }
        public int intVal(int doc) {
          return (int)doubleVal(doc);
        }
        public long longVal(int doc) {
          return (long)doubleVal(doc);
        }
        public double doubleVal(int doc) {
          return func(doc, vals);
        }
        public String strVal(int doc) {
          return Double.toString(doubleVal(doc));
        }
        public String toString(int doc) {
          return name() + '(' + vals.toString(doc) + ')';
        }
      };
    }
  }
}


abstract class Double2Parser extends NamedParser {
  public Double2Parser(String name) {
    super(name);
  }

  public abstract double func(int doc, DocValues a, DocValues b);

  public ValueSource parse(FunctionQParser fp) throws ParseException {
    return new Function(fp.parseValueSource(), fp.parseValueSource());
  }

  class Function extends ValueSource {
    private final ValueSource a;
    private final ValueSource b;

   /**
     * @param   a  the base.
     * @param   b  the exponent.
     */
    public Function(ValueSource a, ValueSource b) {
      this.a = a;
      this.b = b;
    }

    public String description() {
      return name() + "(" + a.description() + "," + b.description() + ")";
    }

    public DocValues getValues(Map context, IndexReader reader) throws IOException {
      final DocValues aVals =  a.getValues(context, reader);
      final DocValues bVals =  b.getValues(context, reader);
      return new DocValues() {
        public float floatVal(int doc) {
          return (float)doubleVal(doc);
        }
        public int intVal(int doc) {
          return (int)doubleVal(doc);
        }
        public long longVal(int doc) {
          return (long)doubleVal(doc);
        }
        public double doubleVal(int doc) {
          return func(doc, aVals, bVals);
        }
        public String strVal(int doc) {
          return Double.toString(doubleVal(doc));
        }
        public String toString(int doc) {
          return name() + '(' + aVals.toString(doc) + ',' + bVals.toString(doc) + ')';
        }
      };
    }

    @Override
    public void createWeight(Map context, Searcher searcher) throws IOException {
      a.createWeight(context,searcher);
      b.createWeight(context,searcher);
    }

    public int hashCode() {
      int h = a.hashCode();
      h ^= (h << 13) | (h >>> 20);
      h += b.hashCode();
      h ^= (h << 23) | (h >>> 10);
      h += name().hashCode();
      return h;
    }

    public boolean equals(Object o) {
      if (this.getClass() != o.getClass()) return false;
      Function other = (Function)o;
      return this.a.equals(other.a)
          && this.b.equals(other.b);
    }
  }

}
