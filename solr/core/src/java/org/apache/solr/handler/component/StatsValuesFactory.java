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

package org.apache.solr.handler.component;

import java.io.IOException;
import java.util.*;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.common.EnumFieldValue;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.handler.component.StatsField.Stat;
import org.apache.solr.schema.*;

/**
 * Factory class for creating instance of {@link org.apache.solr.handler.component.StatsValues}
 */
public class StatsValuesFactory {

  /**
   * Creates an instance of StatsValues which supports values from the specified {@link StatsField}
   *
   * @param statsField {@link StatsField} whose statistics will be created by the resulting {@link StatsValues}
   * @return Instance of {@link StatsValues} that will create statistics from values from the specified {@link StatsField}
   */
  public static StatsValues createStatsValues(StatsField statsField) {

    final SchemaField sf = statsField.getSchemaField();

    if (null == sf) {
      // function stats
      return new NumericStatsValues(statsField);
    } 

    final FieldType fieldType = sf.getType(); // TODO: allow FieldType to provide impl.
    
    if (TrieDateField.class.isInstance(fieldType)) {
      return new DateStatsValues(statsField);
    } else if (TrieField.class.isInstance(fieldType)) {
      return new NumericStatsValues(statsField);
    } else if (StrField.class.isInstance(fieldType)) {
      return new StringStatsValues(statsField);
    } else if (sf.getType().getClass().equals(EnumField.class)) {
      return new EnumStatsValues(statsField);
    } else {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Field type " + fieldType + " is not currently supported");
    }
  }
}

/**
 * Abstract implementation of {@link org.apache.solr.handler.component.StatsValues} 
 * that provides the default behavior for most StatsValues implementations.
 *
 * There are very few requirements placed on what statistics concrete implementations 
 * should collect, with the only required statistics being the minimum and maximum values.
 */
abstract class AbstractStatsValues<T> implements StatsValues {
  private static final String FACETS = "facets";

  /** Tracks all data about tthe stats we need to collect */
  final protected StatsField statsField;

  /** may be null if we are collecting stats directly from a function ValueSource */
  final protected SchemaField sf;
  /** may be null if we are collecting stats directly from a function ValueSource */
  final protected FieldType ft;

  // final booleans from StatsField to allow better inlining & JIT optimizing
  final protected boolean computeCount;
  final protected boolean computeMissing;
  final protected boolean computeCalcDistinct;
  final protected boolean computeMin;
  final protected boolean computeMax;
  final protected boolean computeMinOrMax;

  /** 
   * Either a function value source to collect from, or the ValueSource associated 
   * with a single valued field we are collecting from.  Will be null until/unless 
   * {@link #setNextReader} is called at least once
   */
  private ValueSource valueSource;
  /** 
   * Context to use when retrieving FunctionValues, will be null until/unless 
   * {@link #setNextReader} is called at least once
   */
  private Map vsContext;
  /** 
   * Values to collect, will be null until/unless {@link #setNextReader} is called 
   * at least once 
   */
  protected FunctionValues values;

  protected T max;
  protected T min;
  protected long missing;
  protected long count;
  protected long countDistinct;
  protected final Set<T> distinctValues;
  
  // facetField   facetValue
  protected Map<String, Map<String, StatsValues>> facets = new HashMap<>();

  protected AbstractStatsValues(StatsField statsField) {
    this.statsField = statsField;
    this.computeCount = statsField.calculateStats(Stat.count);
    this.computeMissing = statsField.calculateStats(Stat.missing);
    this.computeCalcDistinct = statsField.calculateStats(Stat.calcdistinct);
    this.computeMin = statsField.calculateStats(Stat.min);
    this.computeMax = statsField.calculateStats(Stat.max);
    this.computeMinOrMax = computeMin || computeMax;
      
    this.distinctValues = computeCalcDistinct ? new TreeSet<>() : null;

    // alternatively, we could refactor a common base class that doesn't know/care
    // about either SchemaField or ValueSource - but then there would be a lot of
    // duplicate code between "NumericSchemaFieldStatsValues" and 
    // "NumericValueSourceStatsValues" which would have diff parent classes
    //
    // part of the complexity here being that the StatsValues API serves two 
    // masters: collecting concrete Values from things like DocValuesStats and 
    // the distributed aggregation logic, but also collecting docIds which it then
    // uses to go out and pull concreate values from the ValueSource 
    // (from a func, or single valued field)
    if (null != statsField.getSchemaField()) {
      assert null == statsField.getValueSource();
      this.sf = statsField.getSchemaField();
      this.ft = sf.getType();
    } else {
      assert null != statsField.getValueSource();
      assert null == statsField.getSchemaField();
      this.sf = null;
      this.ft = null;
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void accumulate(NamedList stv) {
    if (computeCount) {
      count += (Long) stv.get("count");
    }
    if (computeMissing) {
      missing += (Long) stv.get("missing");
    }
    if (computeCalcDistinct) {
      distinctValues.addAll((Collection<T>) stv.get("distinctValues"));
      countDistinct = distinctValues.size();
    }
    
    if (computeMinOrMax) {
      updateMinMax((T) stv.get("min"), (T) stv.get("max"));
    }
    updateTypeSpecificStats(stv);

    NamedList f = (NamedList) stv.get(FACETS);
    if (f == null) {
      return;
    }

    for (int i = 0; i < f.size(); i++) {
      String field = f.getName(i);
      NamedList vals = (NamedList) f.getVal(i);
      Map<String, StatsValues> addTo = facets.get(field);
      if (addTo == null) {
        addTo = new HashMap<>();
        facets.put(field, addTo);
      }
      for (int j = 0; j < vals.size(); j++) {
        String val = vals.getName(j);
        StatsValues vvals = addTo.get(val);
        if (vvals == null) {
          vvals = StatsValuesFactory.createStatsValues(statsField);
          addTo.put(val, vvals);
        }
        vvals.accumulate((NamedList) vals.getVal(j));
      }
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void accumulate(BytesRef value, int count) {
    if (null == ft) {
      throw new IllegalStateException("Can't collect & convert BytesRefs on stats that do't use a a FieldType: " + statsField);
    }
    T typedValue = (T)ft.toObject(sf, value);
    accumulate(typedValue, count);
  }

  public void accumulate(T value, int count) { 
    if (computeCount) {
      this.count += count;
    }
    if (computeCalcDistinct) {
      distinctValues.add(value);
      countDistinct = distinctValues.size();
    }
    if (computeMinOrMax) {
      updateMinMax(value, value);
    }
    updateTypeSpecificStats(value, count);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void missing() {
    if (computeMissing) {
      missing++;
    }
  }
   
  /**
   * {@inheritDoc}
   */
  @Override
  public void addMissing(int count) {
    missing += count;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void addFacet(String facetName, Map<String, StatsValues> facetValues) {
    facets.put(facetName, facetValues);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public NamedList<?> getStatsValues() {
    NamedList<Object> res = new SimpleOrderedMap<>();

    if (statsField.includeInResponse(Stat.min)) {
      res.add("min", min);
    }
    if (statsField.includeInResponse(Stat.max)) {
      res.add("max", max);
    }
    if (statsField.includeInResponse(Stat.count)) {
      res.add("count", count);
    }
    if (statsField.includeInResponse(Stat.missing)) {
      res.add("missing", missing);
    }
    if (statsField.includeInResponse(Stat.calcdistinct)) {
      res.add("distinctValues", distinctValues);
      res.add("countDistinct", countDistinct);
    }

    addTypeSpecificStats(res);
    
    if (!facets.isEmpty()) {

      // add the facet stats
     NamedList<NamedList<?>> nl = new SimpleOrderedMap<>();
     for (Map.Entry<String, Map<String, StatsValues>> entry : facets.entrySet()) {
       NamedList<NamedList<?>> nl2 = new SimpleOrderedMap<>();
       nl.add(entry.getKey(), nl2);
       for (Map.Entry<String, StatsValues> e2 : entry.getValue().entrySet()) {
         nl2.add(e2.getKey(), e2.getValue().getStatsValues());
       }
     }
     res.add(FACETS, nl);
    }

    return res;
  }

  /**
   * {@inheritDoc}
   */
   public void setNextReader(LeafReaderContext ctx) throws IOException {
    if (valueSource == null) {
      // first time we've collected local values, get the right ValueSource
      valueSource = (null == ft) 
        ? statsField.getValueSource()
        : ft.getValueSource(sf, null);
      vsContext = ValueSource.newContext(statsField.getSearcher());
    }
    values = valueSource.getValues(vsContext, ctx);
  }

  /**
   * Updates the minimum and maximum statistics based on the given values
   *
   * @param min Value that the current minimum should be updated against
   * @param max Value that the current maximum should be updated against
   */
  protected abstract void updateMinMax(T min, T max);

  /**
   * Updates the type specific statistics based on the given value
   *
   * @param value Value the statistics should be updated against
   * @param count Number of times the value is being accumulated
   */
  protected abstract void updateTypeSpecificStats(T value, int count);

  /**
   * Updates the type specific statistics based on the values in the given list
   *
   * @param stv List containing values the current statistics should be updated against
   */
  protected abstract void updateTypeSpecificStats(NamedList stv);

  /**
   * Add any type specific statistics to the given NamedList
   *
   * @param res NamedList to add the type specific statistics too
   */
  protected abstract void addTypeSpecificStats(NamedList<Object> res);
}

 /**
 * Implementation of StatsValues that supports Double values
 */
class NumericStatsValues extends AbstractStatsValues<Number> {

  double sum;
  double sumOfSquares;

  double minD; // perf optimization, only valid if (null != this.min)
  double maxD; // perf optimization, only valid if (null != this.max)

  final protected boolean computeSum;
  final protected boolean computeSumOfSquares;

  public NumericStatsValues(StatsField statsField) {
    super(statsField);
    this.computeSum = statsField.calculateStats(Stat.sum);
    this.computeSumOfSquares = statsField.calculateStats(Stat.sumOfSquares);
  }

  @Override
  public void accumulate(int docID) {
    if (values.exists(docID)) {
      accumulate((Number) values.objectVal(docID), 1);
    } else {
      missing();
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void updateTypeSpecificStats(NamedList stv) {
    if (computeSum) {
      sum += ((Number)stv.get("sum")).doubleValue();
    }
    if (computeSumOfSquares) {
      sumOfSquares += ((Number)stv.get("sumOfSquares")).doubleValue();
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void updateTypeSpecificStats(Number v, int count) {
    double value = v.doubleValue();
    if (computeSumOfSquares) {
      sumOfSquares += (value * value * count); // for std deviation
    }
    if (computeSum) {
      sum += value * count;
    }
  }

   /**
   * {@inheritDoc}
   */
  @Override
  protected void updateMinMax(Number min, Number max) {
    // we always use the double values, because that way the response Object class is 
    // consistent regardless of whether we only have 1 value or many that we min/max
    //
    // TODO: would be nice to have subclasses for each type of Number ... breaks backcompat

    if (computeMin) { // nested if to encourage JIT to optimize aware final var?
      if (null != min) {
        double minD = min.doubleValue();
        if (null == this.min || minD < this.minD) {
          // Double for result & cached primitive doulbe to minimize unboxing in future comparisons
          this.min = this.minD = minD;
        }
      }
    }
    if (computeMax) { // nested if to encourage JIT to optimize aware final var?
      if (null != max) {
        double maxD = max.doubleValue();
        if (null == this.max || this.maxD < maxD) {
          // Double for result & cached primitive doulbe to minimize unboxing in future comparisons
          this.max = this.maxD = maxD;
        }
      }
    }
  }

  /**
   * Adds sum, sumOfSquares, mean and standard deviation statistics to the given NamedList
   *
   * @param res NamedList to add the type specific statistics too
   */
  @Override
  protected void addTypeSpecificStats(NamedList<Object> res) {
    if (statsField.includeInResponse(Stat.sum)) {
      res.add("sum", sum);
    }
    if (statsField.includeInResponse(Stat.sumOfSquares)) {
      res.add("sumOfSquares", sumOfSquares);
    }
    if (statsField.includeInResponse(Stat.mean)) {
      res.add("mean", sum / count);
    }
    if (statsField.includeInResponse(Stat.stddev)) {
      res.add("stddev", getStandardDeviation());
    }
  }

  /**
   * Calculates the standard deviation statistic
   *
   * @return Standard deviation statistic
   */
  private double getStandardDeviation() {
    if (count <= 1.0D) {
      return 0.0D;
    }

    return Math.sqrt(((count * sumOfSquares) - (sum * sum)) / (count * (count - 1.0D)));
  }
}

/**
 * Implementation of StatsValues that supports EnumField values
 */
class EnumStatsValues extends AbstractStatsValues<EnumFieldValue> {

  public EnumStatsValues(StatsField statsField) {
    super(statsField);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void accumulate(int docID) {
    if (values.exists(docID)) {
      Integer intValue = (Integer) values.objectVal(docID);
      String stringValue = values.strVal(docID);
      EnumFieldValue enumFieldValue = new EnumFieldValue(intValue, stringValue);
      accumulate(enumFieldValue, 1);
    } else {
      missing();
    }
  }

  /**
   * {@inheritDoc}
   */
  protected void updateMinMax(EnumFieldValue min, EnumFieldValue max) {
    if (computeMin) { // nested if to encourage JIT to optimize aware final var?
      if (null != min) {
        if (null == this.min || (min.compareTo(this.min) < 0)) {
          this.min = min;
        }
      }
    }
    if (computeMax) { // nested if to encourage JIT to optimize aware final var?
      if (null != max) {
        if (null == this.max || (max.compareTo(this.max) > 0)) {
          this.max = max;
        }
      }
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected void updateTypeSpecificStats(NamedList stv) {
    // No type specific stats
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected void updateTypeSpecificStats(EnumFieldValue value, int count) {
    // No type specific stats
  }

  /**
   * Adds no type specific statistics
   */
  @Override
  protected void addTypeSpecificStats(NamedList<Object> res) {
    // Add no statistics
  }


}

/**
 * /**
 * Implementation of StatsValues that supports Date values
 */
class DateStatsValues extends AbstractStatsValues<Date> {

  private long sum = 0;
  double sumOfSquares = 0;

  final protected boolean computeSum;
  final protected boolean computeSumOfSquares;

  public DateStatsValues(StatsField statsField) {
    super(statsField);
    this.computeSum = statsField.calculateStats(Stat.sum);
    this.computeSumOfSquares = statsField.calculateStats(Stat.sumOfSquares);
  }

  @Override
  public void accumulate(int docID) {
    if (values.exists(docID)) {
      accumulate((Date) values.objectVal(docID), 1);
    } else {
      missing();
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected void updateTypeSpecificStats(NamedList stv) {
    if (computeSum) {
      sum += ((Date) stv.get("sum")).getTime();
    }
    if (computeSumOfSquares) {
      sumOfSquares += ((Number)stv.get("sumOfSquares")).doubleValue();
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void updateTypeSpecificStats(Date v, int count) {
    long value = v.getTime();
    if (computeSumOfSquares) {
      sumOfSquares += (value * value * count); // for std deviation
    }
    if (computeSum) {
      sum += value * count;
    }
  }

   /**
   * {@inheritDoc}
   */
  @Override
  protected void updateMinMax(Date min, Date max) {
    if (computeMin) { // nested if to encourage JIT to optimize aware final var?
      if (null != min && (this.min==null || this.min.after(min))) {
        this.min = min;
      }
    }
    if (computeMax) { // nested if to encourage JIT to optimize aware final var?
      if (null != max && (this.max==null || this.max.before(max))) {
        this.max = max;
      }
    }
  }

  /**
   * Adds sum and mean statistics to the given NamedList
   *
   * @param res NamedList to add the type specific statistics too
   */
  @Override
  protected void addTypeSpecificStats(NamedList<Object> res) {
    if (statsField.includeInResponse(Stat.sum)) {
      res.add("sum", new Date(sum));
    }
    if (statsField.includeInResponse(Stat.mean)) {
      res.add("mean", (count > 0) ? new Date(sum / count) : null);
    }
    if (statsField.includeInResponse(Stat.sumOfSquares)) {
      res.add("sumOfSquares", sumOfSquares);
    }
    if (statsField.includeInResponse(Stat.stddev)) {
      res.add("stddev", getStandardDeviation());
    }
  }
  

  
  /**
   * Calculates the standard deviation.  For dates, this is really the MS deviation
   *
   * @return Standard deviation statistic
   */
  private double getStandardDeviation() {
    if (count <= 1) {
      return 0.0D;
    }
    return Math.sqrt(((count * sumOfSquares) - (sum * sum)) / (count * (count - 1.0D)));
  }
}

/**
 * Implementation of StatsValues that supports String values
 */
class StringStatsValues extends AbstractStatsValues<String> {

  public StringStatsValues(StatsField statsField) {
    super(statsField);
  }

  @Override
  public void accumulate(int docID) {
    if (values.exists(docID)) {
      String value = values.strVal(docID);
      if (value != null)
        accumulate(value, 1);
      else
        missing();
    } else {
      missing();
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected void updateTypeSpecificStats(NamedList stv) {
    // No type specific stats
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected void updateTypeSpecificStats(String value, int count) {
    // No type specific stats
  }

   /**
   * {@inheritDoc}
   */
  @Override
  protected void updateMinMax(String min, String max) {
    if (computeMin) { // nested if to encourage JIT to optimize aware final var?
      this.min = min(this.min, min);
    }
    if (computeMax) { // nested if to encourage JIT to optimize aware final var?
      this.max = max(this.max, max);
    }
  }

  /**
   * Adds no type specific statistics
   */
  @Override
  protected void addTypeSpecificStats(NamedList<Object> res) {
    // Add no statistics
  }

  /** 
   * Determines which of the given Strings is the maximum, as computed by {@link String#compareTo(String)}
   *
   * @param str1 String to compare against b
   * @param str2 String compared against a
   * @return str1 if it is considered greater by {@link String#compareTo(String)}, str2 otherwise
   */
  private static String max(String str1, String str2) {
    if (str1 == null) {
      return str2;
    } else if (str2 == null) {
      return str1;
    }
    return (str1.compareTo(str2) > 0) ? str1 : str2;
  }

  /**
   * Determines which of the given Strings is the minimum, as computed by {@link String#compareTo(String)}
   *
   * @param str1 String to compare against b
   * @param str2 String compared against a
   * @return str1 if it is considered less by {@link String#compareTo(String)}, str2 otherwise
   */
  private static String min(String str1, String str2) {
    if (str1 == null) {
      return str2;
    } else if (str2 == null) {
      return str1;
    }
    return (str1.compareTo(str2) < 0) ? str1 : str2;
  }
}
