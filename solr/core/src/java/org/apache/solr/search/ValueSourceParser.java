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
package org.apache.solr.search;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.function.FunctionScoreQuery;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.docvalues.BoolDocValues;
import org.apache.lucene.queries.function.docvalues.DoubleDocValues;
import org.apache.lucene.queries.function.docvalues.LongDocValues;
import org.apache.lucene.queries.function.valuesource.*;
import org.apache.lucene.queries.payloads.PayloadDecoder;
import org.apache.lucene.queries.payloads.PayloadFunction;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.spell.JaroWinklerDistance;
import org.apache.lucene.search.spell.LevenshteinDistance;
import org.apache.lucene.search.spell.NGramDistance;
import org.apache.lucene.search.spell.StringDistance;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrRequestInfo;
import org.apache.solr.schema.CurrencyFieldType;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.schema.StrField;
import org.apache.solr.schema.TextField;
import org.apache.solr.search.facet.AggValueSource;
import org.apache.solr.search.facet.AvgAgg;
import org.apache.solr.search.facet.CountAgg;
import org.apache.solr.search.facet.CountValsAgg;
import org.apache.solr.search.facet.HLLAgg;
import org.apache.solr.search.facet.MinMaxAgg;
import org.apache.solr.search.facet.MissingAgg;
import org.apache.solr.search.facet.PercentileAgg;
import org.apache.solr.search.facet.RelatednessAgg;
import org.apache.solr.search.facet.StddevAgg;
import org.apache.solr.search.facet.SumAgg;
import org.apache.solr.search.facet.SumsqAgg;
import org.apache.solr.search.facet.UniqueAgg;
import org.apache.solr.search.facet.UniqueBlockAgg;
import org.apache.solr.search.facet.VarianceAgg;
import org.apache.solr.search.function.CollapseScoreFunction;
import org.apache.solr.search.function.ConcatStringFunction;
import org.apache.solr.search.function.EqualFunction;
import org.apache.solr.search.function.OrdFieldSource;
import org.apache.solr.search.function.ReverseOrdFieldSource;
import org.apache.solr.search.function.SolrComparisonBoolFunction;
import org.apache.solr.search.function.distance.GeoDistValueSourceParser;
import org.apache.solr.search.function.distance.GeohashFunction;
import org.apache.solr.search.function.distance.GeohashHaversineFunction;
import org.apache.solr.search.function.distance.HaversineFunction;
import org.apache.solr.search.function.distance.SquaredEuclideanFunction;
import org.apache.solr.search.function.distance.StringDistanceFunction;
import org.apache.solr.search.function.distance.VectorDistanceFunction;
import org.apache.solr.search.join.ChildFieldValueSourceParser;
import org.apache.solr.util.DateMathParser;
import org.apache.solr.util.PayloadUtils;
import org.apache.solr.util.plugin.NamedListInitializedPlugin;
import org.locationtech.spatial4j.distance.DistanceUtils;

/**
 * A factory that parses user queries to generate ValueSource instances.
 * Intended usage is to create pluggable, named functions for use in function queries.
 */
public abstract class ValueSourceParser implements NamedListInitializedPlugin {
  /**
   * Initialize the plugin.
   */
  @Override
  public void init(NamedList args) {}

  /**
   * Parse the user input into a ValueSource.
   */
  public abstract ValueSource parse(FunctionQParser fp) throws SyntaxError;

  /** standard functions supported by default, filled in static class initialization */
  private static final Map<String, ValueSourceParser> standardVSParsers = new HashMap<>();
  
  /** standard functions supported by default */
  public static final Map<String, ValueSourceParser> standardValueSourceParsers
    = Collections.unmodifiableMap(standardVSParsers);

  /** Adds a new parser for the name and returns any existing one that was overridden.
   *  This is not thread safe.
   */
  private static ValueSourceParser addParser(String name, ValueSourceParser p) {
    return standardVSParsers.put(name, p);
  }

  /** Adds a new parser for the name and returns any existing one that was overridden.
   *  This is not thread safe.
   */
  private static ValueSourceParser addParser(NamedParser p) {
    return standardVSParsers.put(p.name(), p);
  }

  private static void alias(String source, String dest) {
    standardVSParsers.put(dest, standardVSParsers.get(source));
  }

  static {
    addParser("testfunc", new ValueSourceParser() {
      @Override
      public ValueSource parse(FunctionQParser fp) throws SyntaxError {
        final ValueSource source = fp.parseValueSource();
        return new TestValueSource(source);
      }
    });
    addParser("ord", new ValueSourceParser() {
      @Override
      public ValueSource parse(FunctionQParser fp) throws SyntaxError {
        String field = fp.parseId();
        return new OrdFieldSource(field);
      }
    });
    addParser("literal", new ValueSourceParser() {
      @Override
      public ValueSource parse(FunctionQParser fp) throws SyntaxError {
        return new LiteralValueSource(fp.parseArg());
      }
    });
    addParser("threadid", new ValueSourceParser() {
      @Override
      public ValueSource parse(FunctionQParser fp) throws SyntaxError {
        return new LongConstValueSource(Thread.currentThread().getId());
      }
    });
    addParser("sleep", new ValueSourceParser() {
      @Override
      public ValueSource parse(FunctionQParser fp) throws SyntaxError {
        int ms = fp.parseInt();
        ValueSource source = fp.parseValueSource();
        try {
          Thread.sleep(ms);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
        return source;
      }
    });
    addParser("rord", new ValueSourceParser() {
      @Override
      public ValueSource parse(FunctionQParser fp) throws SyntaxError {
        String field = fp.parseId();
        return new ReverseOrdFieldSource(field);
      }
    });
    addParser("top", new ValueSourceParser() {
      @Override
      public ValueSource parse(FunctionQParser fp) throws SyntaxError {
        // top(vs) is now a no-op
        ValueSource source = fp.parseValueSource();
        return source;
      }
    });
    addParser("linear", new ValueSourceParser() {
      @Override
      public ValueSource parse(FunctionQParser fp) throws SyntaxError {
        ValueSource source = fp.parseValueSource();
        float slope = fp.parseFloat();
        float intercept = fp.parseFloat();
        return new LinearFloatFunction(source, slope, intercept);
      }
    });
    addParser("recip", new ValueSourceParser() {
      @Override
      public ValueSource parse(FunctionQParser fp) throws SyntaxError {
        ValueSource source = fp.parseValueSource();
        float m = fp.parseFloat();
        float a = fp.parseFloat();
        float b = fp.parseFloat();
        return new ReciprocalFloatFunction(source, m, a, b);
      }
    });
    addParser("scale", new ValueSourceParser() {
      @Override
      public ValueSource parse(FunctionQParser fp) throws SyntaxError {
        ValueSource source = fp.parseValueSource();
        float min = fp.parseFloat();
        float max = fp.parseFloat();
        return new ScaleFloatFunction(source, min, max);
      }
    });
    addParser("div", new ValueSourceParser() {
      @Override
      public ValueSource parse(FunctionQParser fp) throws SyntaxError {
        ValueSource a = fp.parseValueSource();
        ValueSource b = fp.parseValueSource();
        return new DivFloatFunction(a, b);
      }
    });
    addParser("mod", new ValueSourceParser() {
      @Override
      public ValueSource parse(FunctionQParser fp) throws SyntaxError {
        ValueSource a = fp.parseValueSource();
        ValueSource b = fp.parseValueSource();
        return new DualFloatFunction(a, b) {
          @Override
          protected String name() {
            return "mod";
          }
          @Override
          protected float func(int doc, FunctionValues aVals, FunctionValues bVals) throws IOException {
            return aVals.floatVal(doc) % bVals.floatVal(doc);
          }
        };
      }
    });
    addParser("map", new ValueSourceParser() {
      @Override
      public ValueSource parse(FunctionQParser fp) throws SyntaxError {
        ValueSource source = fp.parseValueSource();
        float min = fp.parseFloat();
        float max = fp.parseFloat();
        ValueSource target = fp.parseValueSource();
        ValueSource def = fp.hasMoreArguments() ? fp.parseValueSource() : null;
        return new RangeMapFloatFunction(source, min, max, target, def);
      }
    });

    addParser("abs", new ValueSourceParser() {
      @Override
      public ValueSource parse(FunctionQParser fp) throws SyntaxError {
        ValueSource source = fp.parseValueSource();
        return new SimpleFloatFunction(source) {
          @Override
          protected String name() {
            return "abs";
          }

          @Override
          protected float func(int doc, FunctionValues vals) throws IOException {
            return Math.abs(vals.floatVal(doc));
          }
        };
      }
    });
    addParser("cscore", new ValueSourceParser() {
      @Override
      public ValueSource parse(FunctionQParser fp) throws SyntaxError {
        return new CollapseScoreFunction();
      }
    });
    addParser("sum", new ValueSourceParser() {
      @Override
      public ValueSource parse(FunctionQParser fp) throws SyntaxError {
        List<ValueSource> sources = fp.parseValueSourceList();
        return new SumFloatFunction(sources.toArray(new ValueSource[sources.size()]));
      }
    });
    alias("sum","add");    

    addParser("product", new ValueSourceParser() {
      @Override
      public ValueSource parse(FunctionQParser fp) throws SyntaxError {
        List<ValueSource> sources = fp.parseValueSourceList();
        return new ProductFloatFunction(sources.toArray(new ValueSource[sources.size()]));
      }
    });
    alias("product","mul");

    addParser("sub", new ValueSourceParser() {
      @Override
      public ValueSource parse(FunctionQParser fp) throws SyntaxError {
        ValueSource a = fp.parseValueSource();
        ValueSource b = fp.parseValueSource();
        return new DualFloatFunction(a, b) {
          @Override
          protected String name() {
            return "sub";
          }

          @Override
          protected float func(int doc, FunctionValues aVals, FunctionValues bVals) throws IOException {
            return aVals.floatVal(doc) - bVals.floatVal(doc);
          }
        };
      }
    });
    addParser("vector", new ValueSourceParser(){
      @Override
      public ValueSource parse(FunctionQParser fp) throws SyntaxError {
        return new VectorValueSource(fp.parseValueSourceList());
      }
    });
    addParser("query", new ValueSourceParser() {
      // boost(query($q),rating)
      @Override
      public ValueSource parse(FunctionQParser fp) throws SyntaxError {
        Query q = fp.parseNestedQuery();
        float defVal = 0.0f;
        if (fp.hasMoreArguments()) {
          defVal = fp.parseFloat();
        }
        return new QueryValueSource(q, defVal);
      }
    });
    addParser("boost", new ValueSourceParser() {
      @Override
      public ValueSource parse(FunctionQParser fp) throws SyntaxError {
        Query q = fp.parseNestedQuery();
        ValueSource vs = fp.parseValueSource();
        return new QueryValueSource(FunctionScoreQuery.boostByValue(q, vs.asDoubleValuesSource()), 0.0f);
      }
    });
    addParser("joindf", new ValueSourceParser() {
      @Override
      public ValueSource parse(FunctionQParser fp) throws SyntaxError {
        String f0 = fp.parseArg();
        String qf = fp.parseArg();
        return new JoinDocFreqValueSource( f0, qf );
      }
    });

    addParser("geodist", new GeoDistValueSourceParser());

    addParser("hsin", new ValueSourceParser() {
      @Override
      public ValueSource parse(FunctionQParser fp) throws SyntaxError {

        double radius = fp.parseDouble();
        //SOLR-2114, make the convert flag required, since the parser doesn't support much in the way of lookahead or the ability to convert a String into a ValueSource
        boolean convert = Boolean.parseBoolean(fp.parseArg());
        
        MultiValueSource pv1;
        MultiValueSource pv2;

        ValueSource one = fp.parseValueSource();
        ValueSource two = fp.parseValueSource();
        if (fp.hasMoreArguments()) {
          pv1 = new VectorValueSource(Arrays.asList(one, two));//x1, y1
          pv2 = new VectorValueSource(Arrays.asList(fp.parseValueSource(), fp.parseValueSource()));//x2, y2
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

        return new HaversineFunction(pv1, pv2, radius, convert);
      }
    });

    addParser("ghhsin", new ValueSourceParser() {
      @Override
      public ValueSource parse(FunctionQParser fp) throws SyntaxError {
        double radius = fp.parseDouble();

        ValueSource gh1 = fp.parseValueSource();
        ValueSource gh2 = fp.parseValueSource();

        return new GeohashHaversineFunction(gh1, gh2, radius);
      }
    });

    addParser("geohash", new ValueSourceParser() {
      @Override
      public ValueSource parse(FunctionQParser fp) throws SyntaxError {

        ValueSource lat = fp.parseValueSource();
        ValueSource lon = fp.parseValueSource();

        return new GeohashFunction(lat, lon);
      }
    });
    addParser("strdist", new ValueSourceParser() {
      @Override
      public ValueSource parse(FunctionQParser fp) throws SyntaxError {

        ValueSource str1 = fp.parseValueSource();
        ValueSource str2 = fp.parseValueSource();
        String distClass = fp.parseArg();

        StringDistance dist = null;
        if (distClass.equalsIgnoreCase("jw")) {
          dist = new JaroWinklerDistance();
        } else if (distClass.equalsIgnoreCase("edit")) {
          dist = new LevenshteinDistance();
        } else if (distClass.equalsIgnoreCase("ngram")) {
          int ngram = 2;
          if (fp.hasMoreArguments()) {
            ngram = fp.parseInt();
          }
          dist = new NGramDistance(ngram);
        } else {
          dist = fp.req.getCore().getResourceLoader().newInstance(distClass, StringDistance.class);
        }
        return new StringDistanceFunction(str1, str2, dist);
      }
    });
    addParser("field", new ValueSourceParser() {
      @Override
      public ValueSource parse(FunctionQParser fp) throws SyntaxError {

        String fieldName = fp.parseArg();
        SchemaField f = fp.getReq().getSchema().getField(fieldName);
        if (fp.hasMoreArguments()) {
          // multivalued selector option
          String s = fp.parseArg();
          FieldType.MultiValueSelector selector = FieldType.MultiValueSelector.lookup(s);
          if (null == selector) {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
                                    "Multi-Valued field selector '"+s+"' not supported");
          }
          return f.getType().getSingleValueSource(selector, f, fp);
        }
        // simple field ValueSource
        return f.getType().getValueSource(f, fp);
      }
    });
    addParser("currency", new ValueSourceParser() {
      @Override
      public ValueSource parse(FunctionQParser fp) throws SyntaxError {

        String fieldName = fp.parseArg();
        SchemaField f = fp.getReq().getSchema().getField(fieldName);
        if (! (f.getType() instanceof CurrencyFieldType)) {
          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
                                  "Currency function input must be the name of a CurrencyFieldType: " + fieldName);
        }
        CurrencyFieldType ft = (CurrencyFieldType) f.getType();
        String code = fp.hasMoreArguments() ? fp.parseArg() : null;
        return ft.getConvertedValueSource(code, ft.getValueSource(f, fp));
      }
    });

    addParser(new DoubleParser("rad") {
      @Override
      public double func(int doc, FunctionValues vals) throws IOException {
        return vals.doubleVal(doc) * DistanceUtils.DEGREES_TO_RADIANS;
      }
    });
    addParser(new DoubleParser("deg") {
      @Override
      public double func(int doc, FunctionValues vals) throws IOException {
        return vals.doubleVal(doc) * DistanceUtils.RADIANS_TO_DEGREES;
      }
    });
    addParser(new DoubleParser("sqrt") {
      @Override
      public double func(int doc, FunctionValues vals) throws IOException {
        return Math.sqrt(vals.doubleVal(doc));
      }
    });
    addParser(new DoubleParser("cbrt") {
      @Override
      public double func(int doc, FunctionValues vals) throws IOException {
        return Math.cbrt(vals.doubleVal(doc));
      }
    });
    addParser(new DoubleParser("log") {
      @Override
      public double func(int doc, FunctionValues vals) throws IOException {
        return Math.log10(vals.doubleVal(doc));
      }
    });
    addParser(new DoubleParser("ln") {
      @Override
      public double func(int doc, FunctionValues vals) throws IOException {
        return Math.log(vals.doubleVal(doc));
      }
    });
    addParser(new DoubleParser("exp") {
      @Override
      public double func(int doc, FunctionValues vals) throws IOException {
        return Math.exp(vals.doubleVal(doc));
      }
    });
    addParser(new DoubleParser("sin") {
      @Override
      public double func(int doc, FunctionValues vals) throws IOException {
        return Math.sin(vals.doubleVal(doc));
      }
    });
    addParser(new DoubleParser("cos") {
      @Override
      public double func(int doc, FunctionValues vals) throws IOException {
        return Math.cos(vals.doubleVal(doc));
      }
    });
    addParser(new DoubleParser("tan") {
      @Override
      public double func(int doc, FunctionValues vals) throws IOException {
        return Math.tan(vals.doubleVal(doc));
      }
    });
    addParser(new DoubleParser("asin") {
      @Override
      public double func(int doc, FunctionValues vals) throws IOException {
        return Math.asin(vals.doubleVal(doc));
      }
    });
    addParser(new DoubleParser("acos") {
      @Override
      public double func(int doc, FunctionValues vals) throws IOException {
        return Math.acos(vals.doubleVal(doc));
      }
    });
    addParser(new DoubleParser("atan") {
      @Override
      public double func(int doc, FunctionValues vals) throws IOException {
        return Math.atan(vals.doubleVal(doc));
      }
    });
    addParser(new DoubleParser("sinh") {
      @Override
      public double func(int doc, FunctionValues vals) throws IOException {
        return Math.sinh(vals.doubleVal(doc));
      }
    });
    addParser(new DoubleParser("cosh") {
      @Override
      public double func(int doc, FunctionValues vals) throws IOException {
        return Math.cosh(vals.doubleVal(doc));
      }
    });
    addParser(new DoubleParser("tanh") {
      @Override
      public double func(int doc, FunctionValues vals) throws IOException {
        return Math.tanh(vals.doubleVal(doc));
      }
    });
    addParser(new DoubleParser("ceil") {
      @Override
      public double func(int doc, FunctionValues vals) throws IOException {
        return Math.ceil(vals.doubleVal(doc));
      }
    });
    addParser(new DoubleParser("floor") {
      @Override
      public double func(int doc, FunctionValues vals) throws IOException {
        return Math.floor(vals.doubleVal(doc));
      }
    });
    addParser(new DoubleParser("rint") {
      @Override
      public double func(int doc, FunctionValues vals) throws IOException {
        return Math.rint(vals.doubleVal(doc));
      }
    });
    addParser(new Double2Parser("pow") {
      @Override
      public double func(int doc, FunctionValues a, FunctionValues b) throws IOException {
        return Math.pow(a.doubleVal(doc), b.doubleVal(doc));
      }
    });
    addParser(new Double2Parser("hypot") {
      @Override
      public double func(int doc, FunctionValues a, FunctionValues b) throws IOException {
        return Math.hypot(a.doubleVal(doc), b.doubleVal(doc));
      }
    });
    addParser(new Double2Parser("atan2") {
      @Override
      public double func(int doc, FunctionValues a, FunctionValues b) throws IOException {
        return Math.atan2(a.doubleVal(doc), b.doubleVal(doc));
      }
    });
    addParser("max", new ValueSourceParser() {
      @Override
      public ValueSource parse(FunctionQParser fp) throws SyntaxError {
        List<ValueSource> sources = fp.parseValueSourceList();
        return new MaxFloatFunction(sources.toArray(new ValueSource[sources.size()]));
      }
    });
    addParser("min", new ValueSourceParser() {
      @Override
      public ValueSource parse(FunctionQParser fp) throws SyntaxError {
        List<ValueSource> sources = fp.parseValueSourceList();
        return new MinFloatFunction(sources.toArray(new ValueSource[sources.size()]));
      }
    });

    addParser("sqedist", new ValueSourceParser() {
      @Override
      public ValueSource parse(FunctionQParser fp) throws SyntaxError {
        List<ValueSource> sources = fp.parseValueSourceList();
        MVResult mvr = getMultiValueSources(sources);

        return new SquaredEuclideanFunction(mvr.mv1, mvr.mv2);
      }
    });

    addParser("dist", new ValueSourceParser() {
      @Override
      public ValueSource parse(FunctionQParser fp) throws SyntaxError {
        float power = fp.parseFloat();
        List<ValueSource> sources = fp.parseValueSourceList();
        MVResult mvr = getMultiValueSources(sources);
        return new VectorDistanceFunction(power, mvr.mv1, mvr.mv2);
      }
    });
    addParser("ms", new DateValueSourceParser());

    
    addParser("pi", new ValueSourceParser() {
      @Override
      public ValueSource parse(FunctionQParser fp) {
        return new DoubleConstValueSource(Math.PI);
      }
    });
    addParser("e", new ValueSourceParser() {
      @Override
      public ValueSource parse(FunctionQParser fp) {
        return new DoubleConstValueSource(Math.E);
      }
    });


    addParser("docfreq", new ValueSourceParser() {
      @Override
      public ValueSource parse(FunctionQParser fp) throws SyntaxError {
        TInfo tinfo = parseTerm(fp);
        return new DocFreqValueSource(tinfo.field, tinfo.val, tinfo.indexedField, tinfo.indexedBytes.get());
      }
    });

    addParser("totaltermfreq", new ValueSourceParser() {
      @Override
      public ValueSource parse(FunctionQParser fp) throws SyntaxError {
        TInfo tinfo = parseTerm(fp);
        return new TotalTermFreqValueSource(tinfo.field, tinfo.val, tinfo.indexedField, tinfo.indexedBytes.get());
      }
    });
    alias("totaltermfreq","ttf");

    addParser("sumtotaltermfreq", new ValueSourceParser() {
      @Override
      public ValueSource parse(FunctionQParser fp) throws SyntaxError {
        String field = fp.parseArg();
        return new SumTotalTermFreqValueSource(field);
      }
    });
    alias("sumtotaltermfreq","sttf");

    addParser("idf", new ValueSourceParser() {
      @Override
      public ValueSource parse(FunctionQParser fp) throws SyntaxError {
        TInfo tinfo = parseTerm(fp);
        return new IDFValueSource(tinfo.field, tinfo.val, tinfo.indexedField, tinfo.indexedBytes.get());
      }
    });

    addParser("termfreq", new ValueSourceParser() {
      @Override
      public ValueSource parse(FunctionQParser fp) throws SyntaxError {
        TInfo tinfo = parseTerm(fp);
        return new TermFreqValueSource(tinfo.field, tinfo.val, tinfo.indexedField, tinfo.indexedBytes.get());
      }
    });

    addParser("tf", new ValueSourceParser() {
      @Override
      public ValueSource parse(FunctionQParser fp) throws SyntaxError {
        TInfo tinfo = parseTerm(fp);
        return new TFValueSource(tinfo.field, tinfo.val, tinfo.indexedField, tinfo.indexedBytes.get());
      }
    });

    addParser("norm", new ValueSourceParser() {
      @Override
      public ValueSource parse(FunctionQParser fp) throws SyntaxError {
        String field = fp.parseArg();
        return new NormValueSource(field);
      }
    });

    addParser("maxdoc", new ValueSourceParser() {
      @Override
      public ValueSource parse(FunctionQParser fp) {
        return new MaxDocValueSource();
      }
    });

    addParser("numdocs", new ValueSourceParser() {
      @Override
      public ValueSource parse(FunctionQParser fp) {
        return new NumDocsValueSource();
      }
    });

    addParser("payload", new ValueSourceParser() {
      @Override
      public ValueSource parse(FunctionQParser fp) throws SyntaxError {
        // payload(field,value[,default, ['min|max|average|first']])
        //   defaults to "average" and 0.0 default value

        TInfo tinfo = parseTerm(fp); // would have made this parser a new separate class and registered it, but this handy method is private :/

        ValueSource defaultValueSource;
        if (fp.hasMoreArguments()) {
          defaultValueSource = fp.parseValueSource();
        } else {
          defaultValueSource = new ConstValueSource(0.0f);
        }

        PayloadFunction payloadFunction = null;
        String func = "average";
        if (fp.hasMoreArguments()) {
          func = fp.parseArg();
        }
        payloadFunction = PayloadUtils.getPayloadFunction(func);

        // Support func="first" by payloadFunction=null
        if(payloadFunction == null && !"first".equals(func)) {
          // not "first" (or average, min, or max)
          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Invalid payload function: " + func);
        }

        IndexSchema schema = fp.getReq().getCore().getLatestSchema();
        PayloadDecoder decoder = schema.getPayloadDecoder(tinfo.field);

        if (decoder==null) {
          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "No payload decoder found for field: " + tinfo.field);
        }

        return new FloatPayloadValueSource(
            tinfo.field,
            tinfo.val,
            tinfo.indexedField,
            tinfo.indexedBytes.get(),
            decoder,
            payloadFunction,
            defaultValueSource);
      }
    });

    addParser("true", new ValueSourceParser() {
      @Override
      public ValueSource parse(FunctionQParser fp) {
        return new BoolConstValueSource(true);
      }
    });

    addParser("false", new ValueSourceParser() {
      @Override
      public ValueSource parse(FunctionQParser fp) {
        return new BoolConstValueSource(false);
      }
    });

    addParser("exists", new ValueSourceParser() {
      @Override
      public ValueSource parse(FunctionQParser fp) throws SyntaxError {
        ValueSource vs = fp.parseValueSource();
        return new SimpleBoolFunction(vs) {
          @Override
          protected String name() {
            return "exists";
          }
          @Override
          protected boolean func(int doc, FunctionValues vals) throws IOException {
            return vals.exists(doc);
          }
        };
      }
    });

    addParser("not", new ValueSourceParser() {
      @Override
      public ValueSource parse(FunctionQParser fp) throws SyntaxError {
        ValueSource vs = fp.parseValueSource();
        return new SimpleBoolFunction(vs) {
          @Override
          protected boolean func(int doc, FunctionValues vals) throws IOException {
            return !vals.boolVal(doc);
          }
          @Override
          protected String name() {
            return "not";
          }
        };
      }
    });


    addParser("and", new ValueSourceParser() {
      @Override
      public ValueSource parse(FunctionQParser fp) throws SyntaxError {
        List<ValueSource> sources = fp.parseValueSourceList();
        return new MultiBoolFunction(sources) {
          @Override
          protected String name() {
            return "and";
          }
          @Override
          protected boolean func(int doc, FunctionValues[] vals) throws IOException {
            for (FunctionValues dv : vals)
              if (!dv.boolVal(doc)) return false;
            return true;
          }
        };
      }
    });

    addParser("or", new ValueSourceParser() {
      @Override
      public ValueSource parse(FunctionQParser fp) throws SyntaxError {
        List<ValueSource> sources = fp.parseValueSourceList();
        return new MultiBoolFunction(sources) {
          @Override
          protected String name() {
            return "or";
          }
          @Override
          protected boolean func(int doc, FunctionValues[] vals) throws IOException {
            for (FunctionValues dv : vals)
              if (dv.boolVal(doc)) return true;
            return false;
          }
        };
      }
    });

    addParser("xor", new ValueSourceParser() {
      @Override
      public ValueSource parse(FunctionQParser fp) throws SyntaxError {
        List<ValueSource> sources = fp.parseValueSourceList();
        return new MultiBoolFunction(sources) {
          @Override
          protected String name() {
            return "xor";
          }
          @Override
          protected boolean func(int doc, FunctionValues[] vals) throws IOException {
            int nTrue=0, nFalse=0;
            for (FunctionValues dv : vals) {
              if (dv.boolVal(doc)) nTrue++;
              else nFalse++;
            }
            return nTrue != 0 && nFalse != 0;
          }
        };
      }
    });

    addParser("if", new ValueSourceParser() {
      @Override
      public ValueSource parse(FunctionQParser fp) throws SyntaxError {
        ValueSource ifValueSource = fp.parseValueSource();
        ValueSource trueValueSource = fp.parseValueSource();
        ValueSource falseValueSource = fp.parseValueSource();

        return new IfFunction(ifValueSource, trueValueSource, falseValueSource);
      }
    });

    addParser("gt", new ValueSourceParser() {
      @Override
      public ValueSource parse(FunctionQParser fp) throws SyntaxError {
        ValueSource lhsValSource = fp.parseValueSource();
        ValueSource rhsValSource = fp.parseValueSource();

        return new SolrComparisonBoolFunction(lhsValSource, rhsValSource, "gt", (cmp) -> cmp > 0);
      }
    });

    addParser("lt", new ValueSourceParser() {
      @Override
      public ValueSource parse(FunctionQParser fp) throws SyntaxError {
        ValueSource lhsValSource = fp.parseValueSource();
        ValueSource rhsValSource = fp.parseValueSource();

        return new SolrComparisonBoolFunction(lhsValSource, rhsValSource, "lt", (cmp) -> cmp < 0);
      }
    });

    addParser("gte", new ValueSourceParser() {
      @Override
      public ValueSource parse(FunctionQParser fp) throws SyntaxError {
        ValueSource lhsValSource = fp.parseValueSource();
        ValueSource rhsValSource = fp.parseValueSource();

        return new SolrComparisonBoolFunction(lhsValSource, rhsValSource, "gte", (cmp) -> cmp >= 0);

      }
    });

    addParser("lte", new ValueSourceParser() {
      @Override
      public ValueSource parse(FunctionQParser fp) throws SyntaxError {
        ValueSource lhsValSource = fp.parseValueSource();
        ValueSource rhsValSource = fp.parseValueSource();

        return new SolrComparisonBoolFunction(lhsValSource, rhsValSource, "lte", (cmp) -> cmp <= 0);
      }
    });

    addParser("eq", new ValueSourceParser() {
      @Override
      public ValueSource parse(FunctionQParser fp) throws SyntaxError {
        ValueSource lhsValSource = fp.parseValueSource();
        ValueSource rhsValSource = fp.parseValueSource();

        return new EqualFunction(lhsValSource, rhsValSource, "eq");
      }
    });

    addParser("def", new ValueSourceParser() {
      @Override
      public ValueSource parse(FunctionQParser fp) throws SyntaxError {
        return new DefFunction(fp.parseValueSourceList());
      }
    });

    addParser("concat", new ValueSourceParser() {
      @Override
      public ValueSource parse(FunctionQParser fp) throws SyntaxError {
        List<ValueSource> sources = fp.parseValueSourceList();
        return new ConcatStringFunction(sources.toArray(new ValueSource[sources.size()]));
      }
    });


    addParser("agg", new ValueSourceParser() {
      @Override
      public AggValueSource parse(FunctionQParser fp) throws SyntaxError {
        return fp.parseAgg(FunctionQParser.FLAG_DEFAULT);
      }
    });

    addParser("agg_count", new ValueSourceParser() {
      @Override
      public ValueSource parse(FunctionQParser fp) throws SyntaxError {
        return new CountAgg();
      }
    });

    addParser("agg_unique", new ValueSourceParser() {
      @Override
      public ValueSource parse(FunctionQParser fp) throws SyntaxError {
        return new UniqueAgg(fp.parseArg());
      }
    });

    addParser("agg_uniqueBlock", new ValueSourceParser() {
      @Override
      public ValueSource parse(FunctionQParser fp) throws SyntaxError {
        return new UniqueBlockAgg(fp.parseArg());
      }
    });

    addParser("agg_hll", new ValueSourceParser() {
      @Override
      public ValueSource parse(FunctionQParser fp) throws SyntaxError {
        return new HLLAgg(fp.parseArg());
      }
    });

    addParser("agg_sum", new ValueSourceParser() {
      @Override
      public ValueSource parse(FunctionQParser fp) throws SyntaxError {
        return new SumAgg(fp.parseValueSource());
      }
    });

    addParser("agg_avg", new ValueSourceParser() {
      @Override
      public ValueSource parse(FunctionQParser fp) throws SyntaxError {
        return new AvgAgg(fp.parseValueSource());
      }
    });

    addParser("agg_sumsq", new ValueSourceParser() {
      @Override
      public ValueSource parse(FunctionQParser fp) throws SyntaxError {
        return new SumsqAgg(fp.parseValueSource());
      }
    });

    addParser("agg_variance", new ValueSourceParser() {
      @Override
      public ValueSource parse(FunctionQParser fp) throws SyntaxError {
        return new VarianceAgg(fp.parseValueSource());
      }
    });
    
    addParser("agg_stddev", new ValueSourceParser() {
      @Override
      public ValueSource parse(FunctionQParser fp) throws SyntaxError {
        return new StddevAgg(fp.parseValueSource());
      }
    });

    addParser("agg_missing", new ValueSourceParser() {
      @Override
      public ValueSource parse(FunctionQParser fp) throws SyntaxError {
        return new MissingAgg(fp.parseValueSource(FunctionQParser.FLAG_DEFAULT | FunctionQParser.FLAG_USE_FIELDNAME_SOURCE));
      }
    });

    addParser("agg_countvals", new ValueSourceParser() {
      @Override
      public ValueSource parse(FunctionQParser fp) throws SyntaxError {
        return new CountValsAgg(fp.parseValueSource(FunctionQParser.FLAG_DEFAULT | FunctionQParser.FLAG_USE_FIELDNAME_SOURCE));
      }
    });
    
    /***
     addParser("agg_multistat", new ValueSourceParser() {
    @Override
    public ValueSource parse(FunctionQParser fp) throws SyntaxError {
    return null;
    }
    });
     ***/

    addParser("agg_min", new ValueSourceParser() {
      @Override
      public ValueSource parse(FunctionQParser fp) throws SyntaxError {
        return new MinMaxAgg("min", fp.parseValueSource(FunctionQParser.FLAG_DEFAULT | FunctionQParser.FLAG_USE_FIELDNAME_SOURCE));
      }
    });

    addParser("agg_max", new ValueSourceParser() {
      @Override
      public ValueSource parse(FunctionQParser fp) throws SyntaxError {
        return new MinMaxAgg("max", fp.parseValueSource(FunctionQParser.FLAG_DEFAULT | FunctionQParser.FLAG_USE_FIELDNAME_SOURCE));
      }
    });

    addParser("agg_percentile", new PercentileAgg.Parser());
    
    addParser("agg_" + RelatednessAgg.NAME, new ValueSourceParser() {
      @Override
      public ValueSource parse(FunctionQParser fp) throws SyntaxError {
        // TODO: (fore & back)-ground should be optional -- use hasMoreArguments
        // if only one arg, assume it's the foreground
        // (background is the one that will most commonly just be "*:*")
        // see notes in RelatednessAgg constructor about why we don't do this yet
        RelatednessAgg agg = new RelatednessAgg(fp.parseNestedQuery(), fp.parseNestedQuery());
        agg.setOpts(fp);
        return agg;
      }
    });
    
    addParser("childfield", new ChildFieldValueSourceParser());
  }

  ///////////////////////////////////////////////////////////////////////////////
  ///////////////////////////////////////////////////////////////////////////////
  ///////////////////////////////////////////////////////////////////////////////


  private static TInfo parseTerm(FunctionQParser fp) throws SyntaxError {
    TInfo tinfo = new TInfo();

    tinfo.indexedField = tinfo.field = fp.parseArg();
    tinfo.val = fp.parseArg();
    tinfo.indexedBytes = new BytesRefBuilder();

    FieldType ft = fp.getReq().getSchema().getFieldTypeNoEx(tinfo.field);
    if (ft == null) ft = new StrField();

    if (ft instanceof TextField) {
      // need to do analysis on the term
      String indexedVal = tinfo.val;
      Query q = ft.getFieldQuery(fp, fp.getReq().getSchema().getFieldOrNull(tinfo.field), tinfo.val);
      if (q instanceof TermQuery) {
        Term term = ((TermQuery)q).getTerm();
        tinfo.indexedField = term.field();
        indexedVal = term.text();
      }
      tinfo.indexedBytes.copyChars(indexedVal);
    } else {
      ft.readableToIndexed(tinfo.val, tinfo.indexedBytes);
    }

    return tinfo;
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
      List<ValueSource> sources1 = new ArrayList<>(dim);
      List<ValueSource> sources2 = new ArrayList<>(dim);
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

  private static class TInfo {
    String field;
    String val;
    String indexedField;
    BytesRefBuilder indexedBytes;
  }

}


class DateValueSourceParser extends ValueSourceParser {
  @Override
  public void init(NamedList args) {
  }

  public Date getDate(FunctionQParser fp, String arg) {
    if (arg == null) return null;
    // check character index 1 to be a digit.  Index 0 might be a +/-.
    if (arg.startsWith("NOW") || (arg.length() > 1 && Character.isDigit(arg.charAt(1)))) {
      Date now = null;//TODO pull from params?
      return DateMathParser.parseMath(now, arg);
    }
    return null;
  }

  public ValueSource getValueSource(FunctionQParser fp, String arg) {
    if (arg == null) return null;
    SchemaField f = fp.req.getSchema().getField(arg);
    return f.getType().getValueSource(f, fp);
  }

  @Override
  public ValueSource parse(FunctionQParser fp) throws SyntaxError {
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
        @Override
        protected String name() {
          return "ms";
        }

        @Override
        protected float func(int doc, FunctionValues aVals, FunctionValues bVals) throws IOException {
          return ms1 - bVals.longVal(doc);
        }
      };

    // "vd"
    if (v1 != null && d2 != null)
      return new DualFloatFunction(v1, new LongConstValueSource(ms2)) {
        @Override
        protected String name() {
          return "ms";
        }

        @Override
        protected float func(int doc, FunctionValues aVals, FunctionValues bVals) throws IOException {
          return aVals.longVal(doc) - ms2;
        }
      };

    // "vv"
    if (v1 != null && v2 != null)
      return new DualFloatFunction(v1, v2) {
        @Override
        protected String name() {
          return "ms";
        }

        @Override
        protected float func(int doc, FunctionValues aVals, FunctionValues bVals) throws IOException {
          return aVals.longVal(doc) - bVals.longVal(doc);
        }
      };

    return null; // shouldn't happen
  }

}


// Private for now - we need to revisit how to handle typing in function queries
class LongConstValueSource extends ConstNumberSource {
  final long constant;
  final double dv;
  final float fv;

  public LongConstValueSource(long constant) {
    this.constant = constant;
    this.dv = constant;
    this.fv = constant;
  }

  @Override
  public String description() {
    return "const(" + constant + ")";
  }

  @Override
  public FunctionValues getValues(Map context, LeafReaderContext readerContext) throws IOException {
    return new LongDocValues(this) {
      @Override
      public float floatVal(int doc) {
        return fv;
      }

      @Override
      public int intVal(int doc) {
        return (int) constant;
      }

      @Override
      public long longVal(int doc) {
        return constant;
      }

      @Override
      public double doubleVal(int doc) {
        return dv;
      }

      @Override
      public String toString(int doc) {
        return description();
      }
    };
  }

  @Override
  public int hashCode() {
    return (int) constant + (int) (constant >>> 32);
  }

  @Override
  public boolean equals(Object o) {
    if (LongConstValueSource.class != o.getClass()) return false;
    LongConstValueSource other = (LongConstValueSource) o;
    return this.constant == other.constant;
  }

  @Override
  public int getInt() {
    return (int)constant;
  }

  @Override
  public long getLong() {
    return constant;
  }

  @Override
  public float getFloat() {
    return fv;
  }

  @Override
  public double getDouble() {
    return dv;
  }

  @Override
  public Number getNumber() {
    return constant;
  }

  @Override
  public boolean getBool() {
    return constant != 0;
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

  public abstract double func(int doc, FunctionValues vals) throws IOException;

  @Override
  public ValueSource parse(FunctionQParser fp) throws SyntaxError {
    return new Function(fp.parseValueSource());
  }

  class Function extends SingleFunction {
    public Function(ValueSource source) {
      super(source);
    }

    @Override
    public String name() {
      return DoubleParser.this.name();
    }

    @Override
    public FunctionValues getValues(Map context, LeafReaderContext readerContext) throws IOException {
      final FunctionValues vals =  source.getValues(context, readerContext);
      return new DoubleDocValues(this) {
        @Override
        public double doubleVal(int doc) throws IOException {
          return func(doc, vals);
        }
        @Override
        public String toString(int doc) throws IOException {
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

  public abstract double func(int doc, FunctionValues a, FunctionValues b) throws IOException;

  @Override
  public ValueSource parse(FunctionQParser fp) throws SyntaxError {
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

    @Override
    public String description() {
      return name() + "(" + a.description() + "," + b.description() + ")";
    }

    @Override
    public FunctionValues getValues(Map context, LeafReaderContext readerContext) throws IOException {
      final FunctionValues aVals =  a.getValues(context, readerContext);
      final FunctionValues bVals =  b.getValues(context, readerContext);
      return new DoubleDocValues(this) {
        @Override
        public double doubleVal(int doc) throws IOException {
          return func(doc, aVals, bVals);
        }
        @Override
        public String toString(int doc) throws IOException {
          return name() + '(' + aVals.toString(doc) + ',' + bVals.toString(doc) + ')';
        }
      };
    }

    @Override
    public void createWeight(Map context, IndexSearcher searcher) throws IOException {
    }

    @Override
    public int hashCode() {
      int h = a.hashCode();
      h ^= (h << 13) | (h >>> 20);
      h += b.hashCode();
      h ^= (h << 23) | (h >>> 10);
      h += name().hashCode();
      return h;
    }

    @Override
    public boolean equals(Object o) {
      if (this.getClass() != o.getClass()) return false;
      Function other = (Function)o;
      return this.a.equals(other.a)
          && this.b.equals(other.b);
    }
  }

}


class BoolConstValueSource extends ConstNumberSource {
  final boolean constant;

  public BoolConstValueSource(boolean constant) {
    this.constant = constant;
  }

  @Override
  public String description() {
    return "const(" + constant + ")";
  }

  @Override
  public FunctionValues getValues(Map context, LeafReaderContext readerContext) throws IOException {
    return new BoolDocValues(this) {
      @Override
      public boolean boolVal(int doc) {
        return constant;
      }
    };
  }

  @Override
  public int hashCode() {
    return constant ? 0x12345678 : 0x87654321;
  }

  @Override
  public boolean equals(Object o) {
    if (BoolConstValueSource.class != o.getClass()) return false;
    BoolConstValueSource other = (BoolConstValueSource) o;
    return this.constant == other.constant;
  }

  @Override
  public int getInt() {
    return constant ? 1 : 0;
  }

  @Override
  public long getLong() {
    return constant ? 1 : 0;
  }

  @Override
  public float getFloat() {
    return constant ? 1 : 0;
  }

  @Override
  public double getDouble() {
    return constant ? 1 : 0;
  }

  @Override
  public Number getNumber() {
    return constant ? 1 : 0;
  }

  @Override
  public boolean getBool() {
    return constant;
  }
}


class TestValueSource extends ValueSource {
  ValueSource source;
  
  public TestValueSource(ValueSource source) {
    this.source = source;
  }
  
  @Override
  public FunctionValues getValues(Map context, LeafReaderContext readerContext) throws IOException {
    if (context.get(this) == null) {
      SolrRequestInfo requestInfo = SolrRequestInfo.getRequestInfo();
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "testfunc: unweighted value source detected.  delegate="+source + " request=" + (requestInfo==null ? "null" : requestInfo.getReq()));
    }
    return source.getValues(context, readerContext);
  }

  @Override
  public boolean equals(Object o) {
    return o instanceof TestValueSource && source.equals(((TestValueSource)o).source);
  }

  @Override
  public int hashCode() {
    return source.hashCode() + TestValueSource.class.hashCode();
  }

  @Override
  public String description() {
    return "testfunc(" + source.description() + ')';
  }

  @Override
  public void createWeight(Map context, IndexSearcher searcher) throws IOException {
    context.put(this, this);
  }

  @Override
  public SortField getSortField(boolean reverse) {
    return super.getSortField(reverse);
  }
}
