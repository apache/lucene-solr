package org.apache.solr.request;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;

import org.apache.lucene.document.FieldType.NumericType;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Filter;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.request.IntervalFacets.FacetInterval;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.schema.TrieDateField;
import org.apache.solr.search.DocIterator;
import org.apache.solr.search.DocSet;
import org.apache.solr.search.QueryParsing;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.search.SyntaxError;

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

/**
 * Computes interval facets for docvalues field (single or multivalued).
 * <p/>
 * Given a set of intervals for a field and a DocSet, it calculates the number
 * of documents that match each of the intervals provided. The final count for
 * each interval should be exactly the same as the number of results of a range
 * query using the DocSet and the range as filters. This means that the count
 * of {@code facet.query=field:[A TO B]} should be the same as the count of
 * {@code f.field.facet.interval.set=[A,B]}, however, this method will usually
 * be faster in cases where there are a larger number of intervals per field.
 * <p/>
 * To use this class, create an instance using
 * {@link #IntervalFacets(SchemaField, SolrIndexSearcher, DocSet, String[], SolrParams)}
 * and then iterate the {@link FacetInterval} using {@link #iterator()}
 * <p/>
 * Intervals Format</br>
 * Intervals must begin with either '(' or '[', be followed by the start value,
 * then a comma ',', the end value, and finally ')' or ']'. For example:
 * <ul>
 * <li> (1,10) -&gt; will include values greater than 1 and lower than 10
 * <li> [1,10) -&gt; will include values greater or equal to 1 and lower than 10
 * <li> [1,10] -&gt; will include values greater or equal to 1 and lower or equal to 10
 * </ul>
 * The initial and end values can't be empty, if the interval needs to be unbounded,
 * the special character '*' can be used for both, start and end limit. When using
 * '*', '(' and '[', and ')' and ']' will be treated equal. [*,*] will include all
 * documents with a value in the field<p>
 * The interval limits may be strings, there is no need to add quotes, all the text
 * until the comma will be treated as the start limit, and the text after that will be
 * the end limit, for example: [Buenos Aires,New York]. Keep in mind that a string-like
 * comparison will be done to match documents in string intervals (case-sensitive). The
 * comparator can't be changed.
 * Commas, brackets and square brackets can be escaped by using '\' in front of them.
 * Whitespaces before and after the values will be omitted. Start limit can't be grater
 * than the end limit. Equal limits are allowed.<p>
 * As with facet.query, the key used to display the result can be set by using local params
 * syntax, for example:<p>
 * <code>{!key='First Half'}[0,5) </code>
 * <p/>
 * To use this class:
 * <pre>
 * IntervalFacets intervalFacets = new IntervalFacets(schemaField, searcher, docs, intervalStrs, params);
 * for (FacetInterval interval : intervalFacets) {
 *     results.add(interval.getKey(), interval.getCount());
 * }
 * </pre>
 */
public class IntervalFacets implements Iterable<FacetInterval> {
  private final SchemaField schemaField;
  private final SolrIndexSearcher searcher;
  private final DocSet docs;
  private final FacetInterval[] intervals;

  public IntervalFacets(SchemaField schemaField, SolrIndexSearcher searcher, DocSet docs, String[] intervals, SolrParams params) throws SyntaxError, IOException {
    this.schemaField = schemaField;
    this.searcher = searcher;
    this.docs = docs;
    this.intervals = getSortedIntervals(intervals, params);
    doCount();
  }

  private FacetInterval[] getSortedIntervals(String[] intervals, SolrParams params) throws SyntaxError {
    FacetInterval[] sortedIntervals = new FacetInterval[intervals.length];
    int idx = 0;
    for (String intervalStr : intervals) {
      sortedIntervals[idx++] = new FacetInterval(schemaField, intervalStr, params);
    }
    
    /*
     * This comparator sorts the intervals by start value from lower to greater
     */
    Arrays.sort(sortedIntervals, new Comparator<FacetInterval>() {

      @Override
      public int compare(FacetInterval o1, FacetInterval o2) {
        assert o1 != null;
        assert o2 != null;
        return compareStart(o1, o2);
      }

      private int compareStart(FacetInterval o1, FacetInterval o2) {
        if (o1.start == null) {
          if (o2.start == null) {
            return 0;
          }
          return -1;
        }
        if (o2.start == null) {
          return 1;
        }
        return o1.start.compareTo(o2.start);
      }
    });
    return sortedIntervals;
  }

  private void doCount() throws IOException {
    if (schemaField.getType().getNumericType() != null && !schemaField.multiValued()) {
      getCountNumeric();
    } else {
      getCountString();
    }
  }

  private void getCountNumeric() throws IOException {
    final FieldType ft = schemaField.getType();
    final String fieldName = schemaField.getName();
    final NumericType numericType = ft.getNumericType();
    if (numericType == null) {
      throw new IllegalStateException();
    }
    final List<LeafReaderContext> leaves = searcher.getIndexReader().leaves();

    final Iterator<LeafReaderContext> ctxIt = leaves.iterator();
    LeafReaderContext ctx = null;
    NumericDocValues longs = null;
    Bits docsWithField = null;
    for (DocIterator docsIt = docs.iterator(); docsIt.hasNext(); ) {
      final int doc = docsIt.nextDoc();
      if (ctx == null || doc >= ctx.docBase + ctx.reader().maxDoc()) {
        do {
          ctx = ctxIt.next();
        } while (ctx == null || doc >= ctx.docBase + ctx.reader().maxDoc());
        assert doc >= ctx.docBase;
        switch (numericType) {
          case LONG:
            longs = DocValues.getNumeric(ctx.reader(), fieldName);
            break;
          case INT:
            longs = DocValues.getNumeric(ctx.reader(), fieldName);
            break;
          case FLOAT:
            final NumericDocValues floats = DocValues.getNumeric(ctx.reader(), fieldName);
            // TODO: this bit flipping should probably be moved to tie-break in the PQ comparator
            longs = new NumericDocValues() {
              @Override
              public long get(int docID) {
                long bits = floats.get(docID);
                if (bits < 0) bits ^= 0x7fffffffffffffffL;
                return bits;
              }
            };
            break;
          case DOUBLE:
            final NumericDocValues doubles = DocValues.getNumeric(ctx.reader(), fieldName);
            // TODO: this bit flipping should probably be moved to tie-break in the PQ comparator
            longs = new NumericDocValues() {
              @Override
              public long get(int docID) {
                long bits = doubles.get(docID);
                if (bits < 0) bits ^= 0x7fffffffffffffffL;
                return bits;
              }
            };
            break;
          default:
            throw new AssertionError();
        }
        docsWithField = DocValues.getDocsWithField(ctx.reader(), schemaField.getName());
      }
      long v = longs.get(doc - ctx.docBase);
      if (v != 0 || docsWithField.get(doc - ctx.docBase)) {
        accumIntervalWithValue(v);
      }
    }
  }

  private void getCountString() throws IOException {
    Filter filter = docs.getTopFilter();
    List<LeafReaderContext> leaves = searcher.getTopReaderContext().leaves();
    for (int subIndex = 0; subIndex < leaves.size(); subIndex++) {
      LeafReaderContext leaf = leaves.get(subIndex);
      DocIdSet dis = filter.getDocIdSet(leaf, null); // solr docsets already exclude any deleted docs
      if (dis == null) {
        continue;
      }
      DocIdSetIterator disi = dis.iterator();
      if (disi != null) {
        if (schemaField.multiValued()) {
          SortedSetDocValues sub = leaf.reader().getSortedSetDocValues(schemaField.getName());
          if (sub == null) {
            continue;
          }
          final SortedDocValues singleton = DocValues.unwrapSingleton(sub);
          if (singleton != null) {
            // some codecs may optimize SORTED_SET storage for single-valued fields
            accumIntervalsSingle(singleton, disi, dis.bits());
          } else {
            accumIntervalsMulti(sub, disi, dis.bits());
          }
        } else {
          SortedDocValues sub = leaf.reader().getSortedDocValues(schemaField.getName());
          if (sub == null) {
            continue;
          }
          accumIntervalsSingle(sub, disi, dis.bits());
        }
      }
    }
  }

  private void accumIntervalsMulti(SortedSetDocValues ssdv,
                                   DocIdSetIterator disi, Bits bits) throws IOException {
    // First update the ordinals in the intervals for this segment
    for (FacetInterval interval : intervals) {
      interval.updateContext(ssdv);
    }

    int doc;
    while ((doc = disi.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
      if (bits != null && bits.get(doc) == false) {
        continue;
      }
      ssdv.setDocument(doc);
      long currOrd;
      int currentInterval = 0;
      while ((currOrd = ssdv.nextOrd()) != SortedSetDocValues.NO_MORE_ORDS) {
        boolean evaluateNextInterval = true;
        while (evaluateNextInterval && currentInterval < intervals.length) {
          IntervalCompareResult result = intervals[currentInterval].includes(currOrd);
          switch (result) {
            case INCLUDED:
              /*
               * Increment the current interval and move to the next one using
               * the same value
               */
              intervals[currentInterval].incCount();
              currentInterval++;
              break;
            case LOWER_THAN_START:
              /*
               * None of the next intervals will match this value (all of them have 
               * higher start value). Move to the next value for this document. 
               */
              evaluateNextInterval = false;
              break;
            case GREATER_THAN_END:
              /*
               * Next interval may match this value
               */
              currentInterval++;
              break;
          }
        }
      }
    }
  }

  private void accumIntervalsSingle(SortedDocValues sdv, DocIdSetIterator disi, Bits bits) throws IOException {
    // First update the ordinals in the intervals to this segment
    for (FacetInterval interval : intervals) {
      interval.updateContext(sdv);
    }
    int doc;
    while ((doc = disi.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
      if (bits != null && bits.get(doc) == false) {
        continue;
      }
      int ord = sdv.getOrd(doc);
      if (ord >= 0) {
        accumInterval(ord);
      }
    }
  }

  private void accumInterval(int ordinal) {
    assert ordinal >= 0;
    accumIntervalWithValue(ordinal);
  }

  private void accumIntervalWithValue(long value) {
    for (int i = 0; i < intervals.length; i++) {
      FacetInterval interval = intervals[i];
      IntervalCompareResult result = interval.includes(value);
      if (result == IntervalCompareResult.INCLUDED) {
        interval.incCount();
      } else if (result == IntervalCompareResult.LOWER_THAN_START) {
        // All intervals after this will have equal or grater start value, 
        // we can skip them
        break;
      }
    }
  }

  static enum IntervalCompareResult {
    LOWER_THAN_START,
    INCLUDED,
    GREATER_THAN_END,
  }

  /**
   * Helper class to match and count of documents in specified intervals
   */
  static class FacetInterval {

    /**
     * Key to represent this interval
     */
    private final String key;

    /**
     * Start value for this interval as indicated in the request
     */
    final BytesRef start;

    /**
     * End value for this interval as indicated in the request
     */
    final BytesRef end;

    /**
     * Whether or not this interval includes or not the lower limit
     */
    private final boolean startOpen;

    /**
     * Whether or not this interval includes or not the upper limit
     */
    private final boolean endOpen;

    /**
     * Lower limit to which compare a document value. If the field in which we
     * are faceting is single value numeric, then this number will be the
     * {@code long} representation of {@link #start}, and in this case
     * the limit doesn't need to be updated once it is set (will be set in the
     * constructor and remain equal for the life of this object). If the field
     * is multivalued and/or non-numeric, then this number will be the lower limit
     * ordinal for a value to be included in this interval. In this case,
     * {@link #startLimit} needs to be set using either {@link #updateContext(SortedDocValues)} or
     * {@link #updateContext(SortedSetDocValues)} (depending on the field type) for
     * every segment before calling {@link #includes(long)} for any document in the
     * segment.
     */
    private long startLimit;

    /**
     * Upper limit to which compare a document value. If the field in which we
     * are faceting is single value numeric, then this number will be the
     * {@code long} representation of {@link #end}, and in this case
     * the limit doesn't need to be updated once it is set (will be set in the
     * constructor and remain equal for the life of this object). If the field
     * is multivalued and/or non-numeric, then this number will be the upper limit
     * ordinal for a value to be included in this interval. In this case,
     * {@link #endLimit} needs to be set using either {@link #updateContext(SortedDocValues)} or
     * {@link #updateContext(SortedSetDocValues)} (depending on the field type) for
     * every segment before calling {@link #includes(long)} for any document in the
     * segment.
     */
    private long endLimit;

    /**
     * The current count of documents in that match this interval
     */
    private int count;

    FacetInterval(SchemaField schemaField, String intervalStr, SolrParams params) throws SyntaxError {
      if (intervalStr == null) throw new SyntaxError("empty facet interval");
      intervalStr = intervalStr.trim();
      if (intervalStr.length() == 0) throw new SyntaxError("empty facet interval");
      
      try {
        SolrParams localParams = QueryParsing.getLocalParams(intervalStr, params);
        if (localParams != null ) {
          int localParamEndIdx = 2; // omit index of {!
          while (true) {
            localParamEndIdx = intervalStr.indexOf(QueryParsing.LOCALPARAM_END, localParamEndIdx);
            // Local param could be escaping '}'
            if (intervalStr.charAt(localParamEndIdx - 1) != '\\') {
              break;
            }
            localParamEndIdx++;
          }
          intervalStr = intervalStr.substring(localParamEndIdx + 1);
          key = localParams.get(CommonParams.OUTPUT_KEY, intervalStr);
        } else {
          key = intervalStr;
        }
      } catch (SyntaxError e) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, e);
      }
      if (intervalStr.charAt(0) == '(') {
        startOpen = true;
      } else if (intervalStr.charAt(0) == '[') {
        startOpen = false;
      } else {
        throw new SyntaxError("Invalid start character " + intervalStr.charAt(0) + " in facet interval " + intervalStr);
      }

      final int lastNdx = intervalStr.length() - 1;
      if (intervalStr.charAt(lastNdx) == ')') {
        endOpen = true;
      } else if (intervalStr.charAt(lastNdx) == ']') {
        endOpen = false;
      } else {
        throw new SyntaxError("Invalid end character " + intervalStr.charAt(0) + " in facet interval " + intervalStr);
      }

      StringBuilder startStr = new StringBuilder(lastNdx);
      int i = unescape(intervalStr, 1, lastNdx, startStr);
      if (i == lastNdx) {
        if (intervalStr.charAt(lastNdx - 1) == ',') {
          throw new SyntaxError("Empty interval limit");
        }
        throw new SyntaxError("Missing unescaped comma separating interval ends in " + intervalStr);
      }
      try {
        start = getLimitFromString(schemaField, startStr);
      } catch (SyntaxError | SolrException e) {
        throw new SyntaxError(String.format(Locale.ROOT, "Invalid start interval for key '%s': %s", key, e.getMessage()), e);
      }


      StringBuilder endStr = new StringBuilder(lastNdx);
      i = unescape(intervalStr, i, lastNdx, endStr);
      if (i != lastNdx) {
        throw new SyntaxError("Extra unescaped comma at index " + i + " in interval " + intervalStr);
      }
      try {
        end = getLimitFromString(schemaField, endStr);
      } catch (SyntaxError | SolrException e) {
        throw new SyntaxError(String.format(Locale.ROOT, "Invalid end interval for key '%s': %s", key, e.getMessage()), e);
      }
      // TODO: what about escaping star (*)?
      // TODO: escaping spaces on ends?
      if (schemaField.getType().getNumericType() != null) {
        setNumericLimits(schemaField);
      }
      if (start != null && end != null && start.compareTo(end) > 0) {
        throw new SyntaxError("Start is higher than end in interval for key: " + key);
      }
    }

    /**
     * Set startLimit and endLimit for numeric values. The limits in this case
     * are going to be the <code>long</code> representation of the original
     * value. <code>startLimit</code> will be incremented by one in case of the
     * interval start being exclusive. <code>endLimit</code> will be decremented by
     * one in case of the interval end being exclusive.
     */
    private void setNumericLimits(SchemaField schemaField) {
      if (start == null) {
        startLimit = Long.MIN_VALUE;
      } else {
        switch (schemaField.getType().getNumericType()) {
          case LONG:
            if (schemaField.getType() instanceof TrieDateField) {
              startLimit = ((Date) schemaField.getType().toObject(schemaField, start)).getTime();
            } else {
              startLimit = (long) schemaField.getType().toObject(schemaField, start);
            }
            break;
          case INT:
            startLimit = ((Integer) schemaField.getType().toObject(schemaField, start)).longValue();
            break;
          case FLOAT:
            startLimit = NumericUtils.floatToSortableInt((float) schemaField.getType().toObject(schemaField, start));
            break;
          case DOUBLE:
            startLimit = NumericUtils.doubleToSortableLong((double) schemaField.getType().toObject(schemaField, start));
            break;
          default:
            throw new AssertionError();
        }
        if (startOpen) {
          startLimit++;
        }
      }


      if (end == null) {
        endLimit = Long.MAX_VALUE;
      } else {
        switch (schemaField.getType().getNumericType()) {
          case LONG:
            if (schemaField.getType() instanceof TrieDateField) {
              endLimit = ((Date) schemaField.getType().toObject(schemaField, end)).getTime();
            } else {
              endLimit = (long) schemaField.getType().toObject(schemaField, end);
            }
            break;
          case INT:
            endLimit = ((Integer) schemaField.getType().toObject(schemaField, end)).longValue();
            break;
          case FLOAT:
            endLimit = NumericUtils.floatToSortableInt((float) schemaField.getType().toObject(schemaField, end));
            break;
          case DOUBLE:
            endLimit = NumericUtils.doubleToSortableLong((double) schemaField.getType().toObject(schemaField, end));
            break;
          default:
            throw new AssertionError();
        }
        if (endOpen) {
          endLimit--;
        }
      }
    }

    private BytesRef getLimitFromString(SchemaField schemaField, StringBuilder builder) throws SyntaxError {
      String value = builder.toString().trim();
      if (value.length() == 0) {
        throw new SyntaxError("Empty interval limit");
      }
      if ("*".equals(value)) {
        return null;
      }
      return new BytesRef(schemaField.getType().toInternal(value));
    }

    /**
     * Update the ordinals based on the current reader. This method
     * (or {@link #updateContext(SortedSetDocValues)} depending on the
     * DocValues type) needs to be called for every reader before
     * {@link #includes(long)} is called on any document of the reader.
     *
     * @param sdv DocValues for the current reader
     */
    public void updateContext(SortedDocValues sdv) {
      if (start == null) {
        /*
         * Unset start. All ordinals will be greater than -1.
         */
        startLimit = -1;
      } else {
        startLimit = sdv.lookupTerm(start);
        if (startLimit < 0) {
          /*
           * The term was not found in this segment. We'll use inserting-point as
           * start ordinal (then, to be included in the interval, an ordinal needs to be
           * greater or equal to startLimit)
           */
          startLimit = (startLimit * -1) - 1;
        } else {
          /*
           * The term exists in this segment, If the interval has start open (the limit is
           * excluded), then we move one ordinal higher. Then, to be included in the 
           * interval, an ordinal needs to be greater or equal to startLimit
           */
          if (startOpen) {
            startLimit++;
          }
        }
      }
      if (end == null) {
        /*
         * Unset end. All ordinals will be lower than Long.MAX_VALUE.
         */
        endLimit = Long.MAX_VALUE;
      } else {
        endLimit = sdv.lookupTerm(end);
        if (endLimit < 0) {
          /*
           * The term was not found in this segment. We'll use insertion-point -1 as
           * endLimit. To be included in this interval, ordinals must be lower or 
           * equal to endLimit
           */
          endLimit = (endLimit * -1) - 2;
        } else {
          if (endOpen) {
            /*
             * The term exists in this segment, If the interval has start open (the 
             * limit is excluded), then we move one ordinal lower. Then, to be
             * included in the interval, an ordinal needs to be lower or equal to  
             * endLimit
             */
            endLimit--;
          }
        }
      }

    }

    /**
     * Update the ordinals based on the current reader. This method
     * (or {@link #updateContext(SortedDocValues)} depending on the
     * DocValues type) needs to be called for every reader before
     * {@link #includes(long)} is called on any document of the reader.
     *
     * @param sdv DocValues for the current reader
     */
    public void updateContext(SortedSetDocValues sdv) {
      if (start == null) {
        /*
         * Unset start. All ordinals will be greater than -1.
         */
        startLimit = -1;
      } else {
        startLimit = sdv.lookupTerm(start);
        if (startLimit < 0) {
          /*
           * The term was not found in this segment. We'll use inserting-point as
           * start ordinal (then, to be included in the interval, an ordinal needs to be
           * greater or equal to startLimit)
           */
          startLimit = (startLimit * -1) - 1;
        } else {
          /*
           * The term exists in this segment, If the interval has start open (the limit is
           * excluded), then we move one ordinal higher. Then, to be included in the 
           * interval, an ordinal needs to be greater or equal to startLimit
           */
          if (startOpen) {
            startLimit++;
          }
        }
      }
      if (end == null) {
        /*
         * Unset end. All ordinals will be lower than Long.MAX_VALUE.
         */
        endLimit = Long.MAX_VALUE;
      } else {
        endLimit = sdv.lookupTerm(end);
        if (endLimit < 0) {
          /*
           * The term was not found in this segment. We'll use insertion-point -1 as
           * endLimit. To be included in this interval, ordinals must be lower or 
           * equal to endLimit
           */
          endLimit = (endLimit * -1) - 2;
        } else {
          /*
           * The term exists in this segment, If the interval has start open (the 
           * limit is excluded), then we move one ordinal lower. Then, to be
           * included in the interval, an ordinal needs to be lower or equal to  
           * endLimit
           */
          if (endOpen) {
            endLimit--;
          }
        }
      }
    }

    /**
     * Method to use to check whether a document should be counted for
     * an interval or not. Before calling this method on a multi-valued
     * and/or non-numeric field make sure you call {@link #updateContext(SortedDocValues)}
     * or {@link #updateContext(SortedSetDocValues)} (depending on the DV type). It
     * is OK to call this method without other previous calls on numeric fields
     * (with {@link NumericDocValues})
     *
     * @param value For numeric single value fields, this {@code value}
     *              should be the {@code long} representation of the value of the document
     *              in the specified field. For multi-valued and/or non-numeric fields, {@code value}
     *              should be the ordinal of the term in the current segment
     * @return <ul><li>{@link IntervalCompareResult#INCLUDED} if the value is included in the interval
     * <li>{@link IntervalCompareResult#GREATER_THAN_END} if the value is greater than {@code endLimit}
     * <li>{@link IntervalCompareResult#LOWER_THAN_START} if the value is lower than {@code startLimit}
     * </ul>
     * @see NumericUtils#floatToSortableInt(float)
     * @see NumericUtils#doubleToSortableLong(double)
     */
    public IntervalCompareResult includes(long value) {
      if (startLimit > value) {
        return IntervalCompareResult.LOWER_THAN_START;
      }
      if (endLimit < value) {
        return IntervalCompareResult.GREATER_THAN_END;
      }
      return IntervalCompareResult.INCLUDED;
    }

    /* Fill in sb with a string from i to the first unescaped comma, or n.
       Return the index past the unescaped comma, or n if no unescaped comma exists */
    private int unescape(String s, int i, int n, StringBuilder sb) throws SyntaxError {
      for (; i < n; ++i) {
        char c = s.charAt(i);
        if (c == '\\') {
          ++i;
          if (i < n) {
            c = s.charAt(i);
          } else {
            throw new SyntaxError("Unfinished escape at index " + i + " in facet interval " + s);
          }
        } else if (c == ',') {
          return i + 1;
        }
        sb.append(c);
      }
      return n;
    }

    @Override
    public String toString() {
      return this.getClass().getSimpleName() +
          " [key=" + key + ", start=" + start + ", end=" + end
          + ", startOpen=" + startOpen + ", endOpen=" + endOpen + "]";
    }

    /**
     * @return The count of document that matched this interval
     */
    public int getCount() {
      return this.count;
    }

    /**
     * Increment the number of documents that match this interval
     */
    void incCount() {
      this.count++;
    }

    /**
     * @return Human readable key for this interval
     */
    public String getKey() {
      return this.key;
    }

  }

  /**
   * Iterate over all the intervals
   */
  @Override
  public Iterator<FacetInterval> iterator() {

    return new ArrayList<FacetInterval>(Arrays.asList(intervals)).iterator();
  }

}
