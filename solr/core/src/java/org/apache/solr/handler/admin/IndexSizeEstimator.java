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
package org.apache.solr.handler.admin;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import org.apache.commons.math3.stat.descriptive.SummaryStatistics;
import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.StandardDirectoryReader;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.PriorityQueue;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.SuppressForbidden;
import org.apache.lucene.util.UnicodeUtil;
import org.apache.solr.common.MapWriter;
import org.apache.solr.common.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Estimates the raw size of all uncompressed indexed data by scanning term, docValues and
 * stored fields data. This utility also provides detailed statistics about term, docValues,
 * postings and stored fields distributions.
 */
public class IndexSizeEstimator {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static final String TERMS = "terms";
  public static final String STORED_FIELDS = "storedFields";
  public static final String NORMS = "norms";
  public static final String DOC_VALUES = "docValues";
  public static final String POINTS = "points";
  public static final String TERM_VECTORS = "termVectors";
  public static final String SUMMARY = "summary";
  public static final String DETAILS = "details";
  public static final String FIELDS_BY_SIZE = "fieldsBySize";
  public static final String TYPES_BY_SIZE = "typesBySize";

  public static final int DEFAULT_SAMPLING_THRESHOLD = 100_000;
  public static final float DEFAULT_SAMPLING_PERCENT = 5.0f;

  private final IndexReader reader;
  private final int topN;
  private final int maxLength;
  private final boolean withSummary;
  private final boolean withDetails;
  private int samplingThreshold = DEFAULT_SAMPLING_THRESHOLD;
  private float samplingPercent = DEFAULT_SAMPLING_PERCENT;
  private int samplingStep = 1;

  public static final class Estimate implements MapWriter {
    private final Map<String, Long> fieldsBySize;
    private final Map<String, Long> typesBySize;
    private final Map<String, Object> summary;
    private final Map<String, Object> details;

    public Estimate(Map<String, Long> fieldsBySize, Map<String, Long> typesBySize, Map<String, Object> summary, Map<String, Object> details) {
      Objects.requireNonNull(fieldsBySize);
      Objects.requireNonNull(typesBySize);
      this.fieldsBySize = fieldsBySize;
      this.typesBySize = typesBySize;
      this.summary = summary;
      this.details = details;
    }

    public Map<String, Long> getFieldsBySize() {
      return fieldsBySize;
    }

    public Map<String, Long> getTypesBySize() {
      return typesBySize;
    }

    public Map<String, String> getHumanReadableFieldsBySize() {
      LinkedHashMap<String, String> result = new LinkedHashMap<>();
      fieldsBySize.forEach((field, size) -> result.put(field, RamUsageEstimator.humanReadableUnits(size)));
      return result;
    }

    public Map<String, String> getHumanReadableTypesBySize() {
      LinkedHashMap<String, String> result = new LinkedHashMap<>();
      typesBySize.forEach((field, size) -> result.put(field, RamUsageEstimator.humanReadableUnits(size)));
      return result;
    }

    public Map<String, Object> getSummary() {
      return summary;
    }

    public Map<String, Object> getDetails() {
      return details;
    }

    @Override
    public void writeMap(EntryWriter ew) throws IOException {
      ew.put(FIELDS_BY_SIZE, fieldsBySize);
      ew.put(TYPES_BY_SIZE, typesBySize);
      if (summary != null) {
        ew.put(SUMMARY, summary);
      }
      if (details != null) {
        ew.put(DETAILS, details);
      }
    }
  }

  public IndexSizeEstimator(IndexReader reader, int topN, int maxLength, boolean withSummary, boolean withDetails) {
    this.reader = reader;
    this.topN = topN;
    this.maxLength = maxLength;
    this.withSummary = withSummary;
    this.withDetails = withDetails;
  }

  /**
   * Set the sampling threshold. If the index has more documents than this threshold
   * then only some values will be sampled and the totals will be extrapolated.
   * @param threshold size threshold (number of documents). Default value is {@link #DEFAULT_SAMPLING_THRESHOLD}.
   *                  Setting this to values &lt;= 0 means no threshold (and no sampling).
   */
  public void setSamplingThreshold(int threshold) {
    if (threshold <= 0) {
      threshold = Integer.MAX_VALUE;
    }
    this.samplingThreshold = threshold;
  }

  /**
   * Sampling percent (a number greater than 0 and less or equal to 100). When index size exceeds
   * the threshold then approximately only this percent of data will be retrieved from the index and the
   * totals will be extrapolated.
   * @param percent sample percent. Default value is {@link #DEFAULT_SAMPLING_PERCENT}.
   * @throws IllegalArgumentException when value is less than or equal to 0.0 or greater than 100.0, or
   *        the sampling percent is so small that less than 10 documents would be sampled.
   */
  public void setSamplingPercent(float percent) throws IllegalArgumentException {
    if (percent <= 0 || percent > 100) {
      throw new IllegalArgumentException("samplingPercent must be 0 < percent <= 100");
    }
    if (reader.maxDoc() > samplingThreshold) {
      samplingStep = Math.round(100.0f / samplingPercent);
      log.info("- number of documents {} larger than {}, sampling percent is {} and sampling step {}", reader.maxDoc(), samplingThreshold, samplingPercent, samplingStep);
      if (reader.maxDoc() / samplingStep < 10) {
        throw new IllegalArgumentException("Out of " + reader.maxDoc() + " less than 10 documents would be sampled, which is too unreliable. Increase the samplingPercent.");
      }
    }
    this.samplingPercent = percent;
  }

  public Estimate estimate() throws Exception {
    Map<String, Object> details = new LinkedHashMap<>();
    Map<String, Object> summary = new LinkedHashMap<>();
    estimateStoredFields(details);
    estimateTerms(details);
    estimateNorms(details);
    estimatePoints(details);
    estimateTermVectors(details);
    estimateDocValues(details);
    estimateSummary(details, summary);
    if (samplingStep > 1) {
      details.put("samplingPercent", samplingPercent);
      details.put("samplingStep", samplingStep);
    }
    ItemPriorityQueue fieldSizeQueue = new ItemPriorityQueue(summary.size());
    summary.forEach((field, perField) -> {
      long size = ((AtomicLong)((Map<String, Object>)perField).get("totalSize")).get();
      if (size > 0) {
        fieldSizeQueue.insertWithOverflow(new Item(field, size));
      }
    });
    Map<String, Long> fieldsBySize = new LinkedHashMap<>();
    fieldSizeQueue._forEachEntry((k, v) -> fieldsBySize.put((String)k, (Long)v));
    Map<String, AtomicLong> typeSizes = new HashMap<>();
    summary.forEach((field, perField) -> {
      Map<String, Object> perType = (Map<String, Object>)((Map<String, Object>)perField).get("perType");
      perType.forEach((type, size) -> {
        if (type.contains("_lengths")) {
          AtomicLong totalSize = typeSizes.computeIfAbsent(type.replace("_lengths", ""), t -> new AtomicLong());
          totalSize.addAndGet(((AtomicLong)size).get());
        }
      });
    });
    ItemPriorityQueue typesSizeQueue = new ItemPriorityQueue(typeSizes.size());
    typeSizes.forEach((type, size) -> {
      if (size.get() > 0) {
        typesSizeQueue.insertWithOverflow(new Item(type, size.get()));
      }
    });
    Map<String, Long> typesBySize = new LinkedHashMap<>();
    typesSizeQueue._forEachEntry((k, v) -> typesBySize.put((String)k, (Long)v));
    // sort summary by field size
    Map<String, Object> newSummary = new LinkedHashMap<>();
    fieldsBySize.keySet().forEach(k -> newSummary.put(String.valueOf(k), summary.get(k)));
    // convert everything to maps and primitives
    convert(newSummary);
    convert(details);
    return new Estimate(fieldsBySize, typesBySize, withSummary ? newSummary : null, withDetails ? details : null);
  }

  private void convert(Map<String, Object> result) {
    for (Map.Entry<String, Object> entry : result.entrySet()) {
      Object value = entry.getValue();
      if (value instanceof ItemPriorityQueue) {
        ItemPriorityQueue queue = (ItemPriorityQueue)value;
        Map<String, Object> map = new LinkedHashMap<>();
        queue.toMap(map);
        entry.setValue(map);
      } else if (value instanceof MapWriterSummaryStatistics) {
        MapWriterSummaryStatistics stats = (MapWriterSummaryStatistics)value;
        Map<String, Object> map = new LinkedHashMap<>();
        stats.toMap(map);
        entry.setValue(map);
      } else if (value instanceof AtomicLong) {
        entry.setValue(((AtomicLong)value).longValue());
      } else if (value instanceof Map) {
        // recurse
        convert((Map<String, Object>)value);
      }
    }
  }

  private void estimateSummary(Map<String, Object> details, Map<String, Object> summary) {
    log.info("- preparing summary...");
    details.forEach((type, perType) -> {
      ((Map<String, Object>)perType).forEach((field, perField) -> {
        Map<String, Object> perFieldSummary = (Map<String, Object>)summary.computeIfAbsent(field, f -> new HashMap<>());
        ((Map<String, Object>)perField).forEach((k, val) -> {
          if (val instanceof SummaryStatistics) {
            SummaryStatistics stats = (SummaryStatistics)val;
            if (k.startsWith("lengths")) {
              AtomicLong total = (AtomicLong)perFieldSummary.computeIfAbsent("totalSize", kt -> new AtomicLong());
              total.addAndGet((long)stats.getSum());
            }
            Map<String, Object> perTypeSummary = (Map<String, Object>)perFieldSummary.computeIfAbsent("perType", pt -> new HashMap<>());
            AtomicLong total = (AtomicLong)perTypeSummary.computeIfAbsent(type + "_" + k, t -> new AtomicLong());
            total.addAndGet((long)stats.getSum());
          }
        });
      });
    });
  }

  private void estimateNorms(Map<String, Object> result) throws IOException {
    log.info("- estimating norms...");
    Map<String, Map<String, Object>> stats = new HashMap<>();
    for (LeafReaderContext leafReaderContext : reader.leaves()) {
      LeafReader leafReader = leafReaderContext.reader();
      FieldInfos fieldInfos = leafReader.getFieldInfos();
      for (FieldInfo info : fieldInfos) {
        NumericDocValues norms = leafReader.getNormValues(info.name);
        if (norms == null) {
          continue;
        }
        Map<String, Object> perField = stats.computeIfAbsent(info.name, n -> new HashMap<>());
        SummaryStatistics lengthSummary = (SummaryStatistics)perField.computeIfAbsent("lengths", s -> new MapWriterSummaryStatistics());
        while (norms.advance(norms.docID() + samplingStep) != DocIdSetIterator.NO_MORE_DOCS) {
          for (int i = 0; i < samplingStep; i++) {
            lengthSummary.addValue(8);
          }
        }
      }
    }
    result.put(NORMS, stats);
  }

  private void estimatePoints(Map<String, Object> result) throws IOException {
    log.info("- estimating points...");
    Map<String, Map<String, Object>> stats = new HashMap<>();
    for (LeafReaderContext leafReaderContext : reader.leaves()) {
      LeafReader leafReader = leafReaderContext.reader();
      FieldInfos fieldInfos = leafReader.getFieldInfos();
      for (FieldInfo info : fieldInfos) {
        PointValues values = leafReader.getPointValues(info.name);
        if (values == null) {
          continue;
        }
        Map<String, Object> perField = stats.computeIfAbsent(info.name, n -> new HashMap<>());
        SummaryStatistics lengthSummary = (SummaryStatistics)perField.computeIfAbsent("lengths", s -> new MapWriterSummaryStatistics());
        lengthSummary.addValue(values.size() * values.getBytesPerDimension() * values.getNumIndexDimensions());
      }
    }
    result.put(POINTS, stats);
  }

  private void estimateTermVectors(Map<String, Object> result) throws IOException {
    log.info("- estimating term vectors...");
    Map<String, Map<String, Object>> stats = new HashMap<>();
    for (LeafReaderContext leafReaderContext : reader.leaves()) {
      LeafReader leafReader = leafReaderContext.reader();
      Bits liveDocs = leafReader.getLiveDocs();
      for (int docId = 0; docId < leafReader.maxDoc(); docId += samplingStep) {
        if (liveDocs != null && !liveDocs.get(docId)) {
          continue;
        }
        Fields termVectors = leafReader.getTermVectors(docId);
        if (termVectors == null) {
          continue;
        }
        for (String field : termVectors) {
          Terms terms = termVectors.terms(field);
          if (terms == null) {
            continue;
          }
          estimateTermStats(field, terms, stats, true);
        }
      }
    }
    result.put(TERM_VECTORS, stats);
  }

  private void estimateDocValues(Map<String, Object> result) throws IOException {
    log.info("- estimating docValues...");
    Map<String, Map<String, Object>> stats = new HashMap<>();
    for (LeafReaderContext context : reader.leaves()) {
      LeafReader leafReader = context.reader();
      FieldInfos fieldInfos = leafReader.getFieldInfos();
      for (FieldInfo info : fieldInfos) {
        // binary
        countDocValues(stats, info.name, "binary", leafReader.getBinaryDocValues(info.name), values -> {
          try {
            BytesRef value = ((BinaryDocValues) values).binaryValue();
            return value.length;
          } catch (IOException e) {
            // ignore
          }
          return 0;
        });
        // numeric
        countDocValues(stats, info.name, "numeric", leafReader.getNumericDocValues(info.name), values -> 8);
        countDocValues(stats, info.name, "sorted", leafReader.getSortedDocValues(info.name), values -> {
          try {
            TermsEnum termsEnum = ((SortedDocValues) values).termsEnum();
            BytesRef term;
            while ((term = termsEnum.next()) != null) {
              return term.length;
            }
          } catch (IOException e) {
            // ignore
          }
          return 0;
        });
        countDocValues(stats, info.name, "sortedNumeric", leafReader.getSortedNumericDocValues(info.name),
            values -> ((SortedNumericDocValues) values).docValueCount() * 8);
        countDocValues(stats, info.name, "sortedSet", leafReader.getSortedSetDocValues(info.name), values -> {
          try {
            TermsEnum termsEnum = ((SortedSetDocValues) values).termsEnum();
            BytesRef term;
            while ((term = termsEnum.next()) != null) {
              return term.length;
            }
          } catch (IOException e) {
            // ignore
          }
          return 0;
        });
      }
    }
    result.put(DOC_VALUES, stats);
  }

  private void countDocValues(Map<String, Map<String, Object>> stats, String field, String type, DocIdSetIterator values,
                              Function<DocIdSetIterator, Integer> valueLength) throws IOException {
    if (values == null) {
      return;
    }
    Map<String, Object> perField = stats.computeIfAbsent(field, n -> new HashMap<>());
    SummaryStatistics lengthSummary = (SummaryStatistics)perField.computeIfAbsent("lengths_" + type, s -> new MapWriterSummaryStatistics());
    while (values.advance(values.docID() + samplingStep) != DocIdSetIterator.NO_MORE_DOCS) {
      int len = valueLength.apply(values);
      for (int i = 0; i < samplingStep; i++) {
        lengthSummary.addValue(len);
      }
    }
  }

  private void estimateTerms(Map<String, Object> result) throws IOException {
    log.info("- estimating terms...");
    Map<String, Map<String, Object>> stats = new HashMap<>();
    for (LeafReaderContext context : reader.leaves()) {
      LeafReader leafReader = context.reader();
      FieldInfos fieldInfos = leafReader.getFieldInfos();
      for (FieldInfo info : fieldInfos) {
        Terms terms = leafReader.terms(info.name);
        if (terms == null) {
          continue;
        }
        estimateTermStats(info.name, terms, stats, false);
      }
    }
    result.put(TERMS, stats);
  }

  private void estimateTermStats(String field, Terms terms, Map<String, Map<String, Object>> stats, boolean isSampling) throws IOException {
    Map<String, Object> perField = stats.computeIfAbsent(field, n -> new HashMap<>());
    SummaryStatistics lengthSummary = (SummaryStatistics)perField.computeIfAbsent("lengths_terms", s -> new MapWriterSummaryStatistics());
    SummaryStatistics docFreqSummary = (SummaryStatistics)perField.computeIfAbsent("docFreqs", s -> new MapWriterSummaryStatistics());
    SummaryStatistics totalFreqSummary = (SummaryStatistics)perField.computeIfAbsent("lengths_postings", s -> new MapWriterSummaryStatistics());
    // TODO: add this at some point
    //SummaryStatistics impactsSummary = (SummaryStatistics)perField.computeIfAbsent("lengths_impacts", s -> new MapWriterSummaryStatistics());
    SummaryStatistics payloadSummary = null;
    if (terms.hasPayloads()) {
      payloadSummary = (SummaryStatistics)perField.computeIfAbsent("lengths_payloads", s -> new MapWriterSummaryStatistics());
    }
    ItemPriorityQueue topLen = (ItemPriorityQueue)perField.computeIfAbsent("topLen", s -> new ItemPriorityQueue(topN));
    ItemPriorityQueue topTotalFreq = (ItemPriorityQueue)perField.computeIfAbsent("topTotalFreq", s -> new ItemPriorityQueue(topN));
    TermsEnum termsEnum = terms.iterator();
    BytesRef term;
    PostingsEnum postings = null;
    while ((term = termsEnum.next()) != null) {
      if (isSampling) {
        for (int i = 0; i < samplingStep; i++) {
          lengthSummary.addValue(term.length);
          docFreqSummary.addValue(termsEnum.docFreq());
          totalFreqSummary.addValue(termsEnum.totalTermFreq());
        }
      } else {
        lengthSummary.addValue(term.length);
        docFreqSummary.addValue(termsEnum.docFreq());
        totalFreqSummary.addValue(termsEnum.totalTermFreq());
      }
      if (terms.hasPayloads()) {
        postings = termsEnum.postings(postings, PostingsEnum.ALL);
        while (postings.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
          int freq = postings.freq();
          for (int i = 0; i < freq; i++) {
            if (postings.nextPosition() < 0) {
              break;
            }
            BytesRef payload = postings.getPayload();
            if (payload != null) {
              if (isSampling) {
                for (int k = 0; k < samplingStep; k++) {
                  payloadSummary.addValue(payload.length);
                }
              } else {
                payloadSummary.addValue(payload.length);
              }
            }
          }
        }
      }
      String value = term.utf8ToString();
      if (value.length() > maxLength) {
        value = value.substring(0, maxLength);
      }
      topLen.insertWithOverflow(new Item(value, term.length));
      topTotalFreq.insertWithOverflow(new Item(value, termsEnum.totalTermFreq()));
    }
  }


  private void estimateStoredFields(Map<String, Object> result) throws IOException {
    log.info("- estimating stored fields...");
    Map<String, Map<String, Object>> stats = new HashMap<>();
    for (LeafReaderContext context : reader.leaves()) {
      LeafReader leafReader = context.reader();
      EstimatingVisitor visitor = new EstimatingVisitor(stats, topN, maxLength, samplingStep);
      Bits liveDocs = leafReader.getLiveDocs();
      if (leafReader instanceof CodecReader) {
        CodecReader codecReader = (CodecReader)leafReader;
        StoredFieldsReader storedFieldsReader = codecReader.getFieldsReader();
        // this instance may be faster for a full sequential pass
        StoredFieldsReader mergeInstance = storedFieldsReader.getMergeInstance();
        for (int docId = 0; docId < leafReader.maxDoc(); docId += samplingStep) {
          if (liveDocs != null && !liveDocs.get(docId)) {
            continue;
          }
          mergeInstance.visitDocument(docId, visitor);
        }
        if (mergeInstance != storedFieldsReader) {
          mergeInstance.close();
        }
      } else {
        for (int docId = 0; docId < leafReader.maxDoc(); docId += samplingStep) {
          if (liveDocs != null && !liveDocs.get(docId)) {
            continue;
          }
          leafReader.document(docId, visitor);
        }
      }
    }
    result.put(STORED_FIELDS, stats);
  }

  public static class Item {
    Object value;
    long size;

    public Item(Object value, long size) {
      this.value = value;
      this.size = size;
    }

    public String toString() {
      return "size=" + size + ", value=" + value;
    }
  }

  public static class MapWriterSummaryStatistics extends SummaryStatistics implements MapWriter {

    @Override
    public void writeMap(EntryWriter ew) throws IOException {
      ew.put("n", getN());
      ew.put("min", getMin());
      ew.put("max", getMax());
      ew.put("sum", getSum());
      ew.put("mean", getMean());
      ew.put("geoMean", getGeometricMean());
      ew.put("variance", getVariance());
      ew.put("populationVariance", getPopulationVariance());
      ew.put("stddev", getStandardDeviation());
      ew.put("secondMoment", getSecondMoment());
      ew.put("sumOfSquares", getSumsq());
      ew.put("sumOfLogs", getSumOfLogs());
    }
  }

  public static class ItemPriorityQueue extends PriorityQueue<Item> implements MapWriter {

    public ItemPriorityQueue(int maxSize) {
      super(maxSize);
    }

    @Override
    protected boolean lessThan(Item a, Item b) {
      return a.size < b.size;
    }

    public String toString() {
      StringBuilder sb = new StringBuilder();
      Iterator<Item> it = iterator();
      while (it.hasNext()) {
        if (sb.length() > 0) {
          sb.append('\n');
        }
        sb.append(it.next());
      }
      return sb.toString();
    }

    // WARNING: destructive! empties the queue
    @Override
    public void writeMap(EntryWriter ew) throws IOException {
      Item[] items = new Item[size()];
      int pos = size() - 1;
      while (size() > 0) {
        items[pos] = pop();
        pos--;
      }
      for (Item item : items) {
        ew.put(String.valueOf(item.value), item.size);
      }
    }
  }

  private static class EstimatingVisitor extends StoredFieldVisitor {
    final Map<String, Map<String, Object>> stats;
    final int topN;
    final int maxLength;
    final int samplingStep;

    EstimatingVisitor(Map<String, Map<String, Object>> stats, int topN, int maxLength, int samplingStep) {
      this.stats = stats;
      this.topN = topN;
      this.maxLength = maxLength;
      this.samplingStep = samplingStep;
    }

    /** Process a binary field.
     * @param value newly allocated byte array with the binary contents.
     */
    public void binaryField(FieldInfo fieldInfo, byte[] value) throws IOException {
      // trim the value if needed
      int len = value != null ? value.length : 0;
      if (len > maxLength) {
        byte[] newValue = new byte[maxLength];
        System.arraycopy(value, 0, newValue, 0, maxLength);
        value = newValue;
      }
      String strValue = new BytesRef(value).toString();
      countItem(fieldInfo.name, strValue, len);
    }

    /** Process a string field. */
    public void stringField(FieldInfo fieldInfo, String value) throws IOException {
      // trim the value if needed
      int len = value != null ? UnicodeUtil.calcUTF16toUTF8Length(value, 0, value.length()) : 0;
      if (value.length() > maxLength) {
        value = value.substring(0, maxLength);
      }
      countItem(fieldInfo.name, value, len);
    }

    /** Process a int numeric field. */
    public void intField(FieldInfo fieldInfo, int value) throws IOException {
      countItem(fieldInfo.name, String.valueOf(value), 4);
    }

    /** Process a long numeric field. */
    public void longField(FieldInfo fieldInfo, long value) throws IOException {
      countItem(fieldInfo.name, String.valueOf(value), 8);
    }

    /** Process a float numeric field. */
    public void floatField(FieldInfo fieldInfo, float value) throws IOException {
      countItem(fieldInfo.name, String.valueOf(value), 4);
    }

    /** Process a double numeric field. */
    public void doubleField(FieldInfo fieldInfo, double value) throws IOException {
      countItem(fieldInfo.name, String.valueOf(value), 8);
    }

    private void countItem(String field, Object value, int size) {
      Map<String, Object> perField = stats.computeIfAbsent(field, n -> new HashMap<>());
      SummaryStatistics summary = (SummaryStatistics)perField.computeIfAbsent("lengths", s -> new MapWriterSummaryStatistics());
      for (int i = 0; i < samplingStep; i++) {
        summary.addValue(size);
      }
      ItemPriorityQueue topNqueue = (ItemPriorityQueue)perField.computeIfAbsent("topLen", s-> new ItemPriorityQueue(topN));
      topNqueue.insertWithOverflow(new Item(value, size));
    }

    @Override
    public Status needsField(FieldInfo fieldInfo) throws IOException {
      return Status.YES;
    }
  }

  @SuppressForbidden(reason = "System.err and System.out required for a command-line utility")
  public static void main(String[] args) throws Exception {
    if (args.length == 0) {
      System.err.println("Usage: " + IndexSizeEstimator.class.getName() + " [-topN NUM] [-maxLen NUM] [-summary] [-details] <indexDir>");
      System.err.println();
      System.err.println("\t<indexDir>\tpath to the index (parent path of 'segments_N' file)");
      System.err.println("\t-topN NUM\tnumber of top largest items to collect");
      System.err.println("\t-maxLen NUM\ttruncate the largest items to NUM bytes / characters");
      System.err.println(-1);
    }
    String path = null;
    int topN = 20;
    int maxLen = 100;
    boolean details = false;
    boolean summary = false;
    for (int i = 0; i < args.length; i++) {
      if (args[i].equals("-topN")) {
        topN = Integer.parseInt(args[++i]);
      } else if (args[i].equals("-maxLen")) {
        maxLen = Integer.parseInt(args[++i]);
      } else if (args[i].equals("-details")) {
        details = true;
      } else if (args[i].equals("-summary")) {
        summary = true;
      } else {
        path = args[i];
      }
    }
    if (path == null) {
      System.err.println("ERROR: <indexDir> argument is required.");
      System.exit(-2);
    }
    Directory dir = FSDirectory.open(Paths.get(path));
    DirectoryReader reader = StandardDirectoryReader.open(dir);
    IndexSizeEstimator stats = new IndexSizeEstimator(reader, topN, maxLen, summary, details);
    System.out.println(Utils.toJSONString(stats.estimate()));
    System.exit(0);
  }
}
