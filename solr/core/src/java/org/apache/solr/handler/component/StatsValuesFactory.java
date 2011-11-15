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

package org.apache.solr.handler.component;

import java.util.Date;
import java.util.Map;
import java.util.HashMap;

import org.apache.lucene.util.BytesRef;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.schema.*;

/**
 * Factory class for creating instance of {@link org.apache.solr.handler.component.StatsValues}
 */
public class StatsValuesFactory {

  /**
   * Creates an instance of StatsValues which supports values from a field of the given FieldType
   *
   * @param sf SchemaField for the field whose statistics will be created by the resulting StatsValues
   * @return Instance of StatsValues that will create statistics from values from a field of the given type
   */
  public static StatsValues createStatsValues(SchemaField sf) {
    FieldType fieldType = sf.getType();
    if (DoubleField.class.isInstance(fieldType) ||
        IntField.class.isInstance(fieldType) ||
        LongField.class.isInstance(fieldType) ||
        ShortField.class.isInstance(fieldType) ||
        FloatField.class.isInstance(fieldType) ||
        ByteField.class.isInstance(fieldType) ||
        TrieField.class.isInstance(fieldType) ||
        SortableDoubleField.class.isInstance(fieldType) ||
        SortableIntField.class.isInstance(fieldType) ||
        SortableLongField.class.isInstance(fieldType) ||
        SortableFloatField.class.isInstance(fieldType)) {
      return new NumericStatsValues(sf);
    } else if (DateField.class.isInstance(fieldType)) {
      return new DateStatsValues(sf);
    } else if (StrField.class.isInstance(fieldType)) {
      return new StringStatsValues(sf);
    } else {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Field type " + fieldType + " is not currently supported");
    }
  }
}

/**
 * Abstract implementation of {@link org.apache.solr.handler.component.StatsValues} that provides the default behavior
 * for most StatsValues implementations.
 *
 * There are very few requirements placed on what statistics concrete implementations should collect, with the only required
 * statistics being the minimum and maximum values.
 */
abstract class AbstractStatsValues<T> implements StatsValues {
  private static final String FACETS = "facets";
  final protected SchemaField sf;
  final protected FieldType ft;
  protected T max;
  protected T min;
  protected long missing;
  protected long count;
  
  // facetField   facetValue
  protected Map<String, Map<String, StatsValues>> facets = new HashMap<String, Map<String, StatsValues>>();

  protected AbstractStatsValues(SchemaField sf) {
    this.sf = sf;
    this.ft = sf.getType();
  }

  /**
   * {@inheritDoc}
   */
  public void accumulate(NamedList stv) {
    count += (Long) stv.get("count");
    missing += (Long) stv.get("missing");

    updateMinMax((T) stv.get("min"), (T) stv.get("max"));
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
        addTo = new HashMap<String, StatsValues>();
        facets.put(field, addTo);
      }
      for (int j = 0; j < vals.size(); j++) {
        String val = vals.getName(j);
        StatsValues vvals = addTo.get(val);
        if (vvals == null) {
          vvals = StatsValuesFactory.createStatsValues(sf);
          addTo.put(val, vvals);
        }
        vvals.accumulate((NamedList) vals.getVal(j));
      }
    }
  }
  
  /**
   * {@inheritDoc}
   */
  public void accumulate(BytesRef value) {
    count++;
    T typedValue = (T)ft.toObject(sf, value);
    updateMinMax(typedValue, typedValue);
    updateTypeSpecificStats(typedValue);
  }

  /**
   * {@inheritDoc}
   */
  public void accumulate(BytesRef value, int count) {
    this.count += count;
    T typedValue = (T)ft.toObject(sf, value);
    updateMinMax(typedValue, typedValue);
    updateTypeSpecificStats(typedValue, count);
  }
  
  /**
   * {@inheritDoc}
   */
  public void missing() {
    missing++;
  }
   
  /**
   * {@inheritDoc}
   */
  public void addMissing(int count) {
    missing += count;
  }

  /**
   * {@inheritDoc}
   */
  public void addFacet(String facetName, Map<String, StatsValues> facetValues) {
    facets.put(facetName, facetValues);
  }

  /**
   * {@inheritDoc}
   */
  public NamedList<?> getStatsValues() {
    NamedList<Object> res = new SimpleOrderedMap<Object>();

    res.add("min", min);
    res.add("max", max);
    res.add("count", count);
    res.add("missing", missing);
    addTypeSpecificStats(res);

     // add the facet stats
    NamedList<NamedList<?>> nl = new SimpleOrderedMap<NamedList<?>>();
    for (Map.Entry<String, Map<String, StatsValues>> entry : facets.entrySet()) {
      NamedList<NamedList<?>> nl2 = new SimpleOrderedMap<NamedList<?>>();
      nl.add(entry.getKey(), nl2);
      for (Map.Entry<String, StatsValues> e2 : entry.getValue().entrySet()) {
        nl2.add(e2.getKey(), e2.getValue().getStatsValues());
      }
      res.add(FACETS, nl);
    }
    return res;
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
   */
  protected abstract void updateTypeSpecificStats(T value);

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

  public NumericStatsValues(SchemaField sf) {
    super(sf);
    min = Double.POSITIVE_INFINITY;
    max = Double.NEGATIVE_INFINITY;
  }

  /**
   * {@inheritDoc}
   */
  public void updateTypeSpecificStats(NamedList stv) {
    sum += ((Number)stv.get("sum")).doubleValue();
    sumOfSquares += ((Number)stv.get("sumOfSquares")).doubleValue();
  }

  /**
   * {@inheritDoc}
   */
  public void updateTypeSpecificStats(Number v) {
    double value = v.doubleValue();
    sumOfSquares += (value * value); // for std deviation
    sum += value;
  }

  /**
   * {@inheritDoc}
   */
  public void updateTypeSpecificStats(Number v, int count) {
    double value = v.doubleValue();
    sumOfSquares += (value * value * count); // for std deviation
    sum += value * count;
  }

   /**
   * {@inheritDoc}
   */
  protected void updateMinMax(Number min, Number max) {
    this.min = Math.min(this.min.doubleValue(), min.doubleValue());
    this.max = Math.max(this.max.doubleValue(), max.doubleValue());
  }

  /**
   * Adds sum, sumOfSquares, mean and standard deviation statistics to the given NamedList
   *
   * @param res NamedList to add the type specific statistics too
   */
  protected void addTypeSpecificStats(NamedList<Object> res) {
    res.add("sum", sum);
    res.add("sumOfSquares", sumOfSquares);
    res.add("mean", sum / count);
    res.add("stddev", getStandardDeviation());
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
 * Implementation of StatsValues that supports Date values
 */
class DateStatsValues extends AbstractStatsValues<Date> {

  private static final DateField DATE_FIELD = new DateField();

  private long sum;

  public DateStatsValues(SchemaField sf) {
    super(sf);
  }

  /**
   * {@inheritDoc}
   */
  protected void updateTypeSpecificStats(NamedList stv) {
    sum += ((Date) stv.get("sum")).getTime();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void updateTypeSpecificStats(Date value) {
    sum += value.getTime();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void updateTypeSpecificStats(Date value, int count) {
    sum += value.getTime() * count;
  }

   /**
   * {@inheritDoc}
   */
  protected void updateMinMax(Date min, Date max) {
    if(this.min==null || this.min.after(min)) {
      this.min = min;
    }
    if(this.max==null || this.max.before(min)) {
      this.max = max;
    }
  }

  /**
   * Adds sum and mean statistics to the given NamedList
   *
   * @param res NamedList to add the type specific statistics too
   */
  protected void addTypeSpecificStats(NamedList<Object> res) {
    res.add("sum", new Date(sum));
    res.add("mean", new Date(sum / count));
  }
}

/**
 * Implementation of StatsValues that supports String values
 */
class StringStatsValues extends AbstractStatsValues<String> {

  public StringStatsValues(SchemaField sf) {
    super(sf);
  }

  /**
   * {@inheritDoc}
   */
  protected void updateTypeSpecificStats(NamedList stv) {
    // No type specific stats
  }

  /**
   * {@inheritDoc}
   */
  protected void updateTypeSpecificStats(String value) {
    // No type specific stats
  }

  /**
   * {@inheritDoc}
   */
  protected void updateTypeSpecificStats(String value, int count) {
    // No type specific stats
  }

   /**
   * {@inheritDoc}
   */
  protected void updateMinMax(String min, String max) {
    this.max = max(this.max, max);
    this.min = min(this.min, min);
  }

  /**
   * Adds no type specific statistics
   */
  protected void addTypeSpecificStats(NamedList<Object> res) {
    // Add no statistics
  }

  /**
   * Determines which of the given Strings is the maximum, as computed by {@link String#compareTo(Object)}
   *
   * @param str1 String to compare against b
   * @param str2 String compared against a
   * @return str1 if it is considered greater by {@link String#compareTo(Object)}, str2 otherwise
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
   * Determines which of the given Strings is the minimum, as computed by {@link String#compareTo(Object)}
   *
   * @param str1 String to compare against b
   * @param str2 String compared against a
   * @return str1 if it is considered less by {@link String#compareTo(Object)}, str2 otherwise
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
