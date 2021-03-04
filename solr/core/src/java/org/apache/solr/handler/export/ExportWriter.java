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

package org.apache.solr.handler.export;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.lang.invoke.MethodHandles;
import java.nio.charset.StandardCharsets;
import java.util.*;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.FixedBitSet;
import org.apache.solr.client.solrj.impl.BinaryResponseParser;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.stream.ParallelStream;
import org.apache.solr.client.solrj.io.stream.StreamContext;
import org.apache.solr.client.solrj.io.stream.TupleStream;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionNamedParameter;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionParser;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;
import org.apache.solr.common.IteratorWriter;
import org.apache.solr.common.MapWriter;
import org.apache.solr.common.MapWriter.EntryWriter;
import org.apache.solr.common.PushWriter;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.params.StreamParams;
import org.apache.solr.common.util.JavaBinCodec;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.core.SolrCore;
import org.apache.solr.metrics.SolrMetricsContext;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestInfo;
import org.apache.solr.response.BinaryResponseWriter;
import org.apache.solr.response.JSONResponseWriter;
import org.apache.solr.response.QueryResponseWriter;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.schema.BoolField;
import org.apache.solr.schema.DateValueFieldType;
import org.apache.solr.schema.DoubleValueFieldType;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.FloatValueFieldType;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.IntValueFieldType;
import org.apache.solr.schema.LongValueFieldType;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.schema.SortableTextField;
import org.apache.solr.schema.StrField;
import org.apache.solr.search.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.apache.solr.common.util.Utils.makeMap;

/**
 * Prepares and writes the documents requested by /export requests
 *
 * {@link ExportWriter} gathers and sorts the documents for a core using "stream sorting".
 * <p>
 * Stream sorting works by repeatedly processing and modifying a bitmap of matching documents.  Each pass over the
 * bitmap identifies the smallest docs (default is {@link #DEFAULT_BATCH_SIZE}) that haven't been sent yet and stores them in a
 * Priority Queue.  They are then exported (written across the wire) and marked as sent (unset in the bitmap).
 * This process repeats until all matching documents have been sent.
 */
public class ExportWriter implements SolrCore.RawWriter, Closeable {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static final String BATCH_SIZE_PARAM = "batchSize";
  public static final String QUEUE_SIZE_PARAM = "queueSize";

  public static final String SOLR_CACHE_KEY = "exportCache";

  public static final int DEFAULT_BATCH_SIZE = 30000;
  public static final int DEFAULT_QUEUE_SIZE = 150000;


  private OutputStreamWriter respWriter;
  final SolrQueryRequest req;
  final SolrQueryResponse res;
  final StreamContext initialStreamContext;
  final SolrMetricsContext solrMetricsContext;
  final String metricsPath;
  //The batch size for the output writer thread.
  final int batchSize;
  //The max combined size of the segment level priority queues.
  private int priorityQueueSize;
  StreamExpression streamExpression;
  StreamContext streamContext;
  FieldWriter[] fieldWriters;
  int totalHits = 0;
  FixedBitSet[] sets = null;
  PushWriter writer;
  final private String wt;
  final int numWorkers;
  final int workerId;
  final String fieldList;
  final List<String> partitionKeys;
  final String partitionCacheKey;

  // per-segment caches for already populated partitioning filters when parallel() is in use
  final SolrCache<IndexReader.CacheKey, SolrCache<String, FixedBitSet>> partitionCaches;

  // per-segment partitioning filters that are incomplete (still being updated from the current request)
  final Map<IndexReader.CacheKey, FixedBitSet> tempPartitionCaches;



  public ExportWriter(SolrQueryRequest req, SolrQueryResponse res, String wt,
                      StreamContext initialStreamContext, SolrMetricsContext solrMetricsContext,
                      String metricsPath) throws Exception {
    this.req = req;
    this.res = res;
    this.wt = wt;
    this.initialStreamContext = initialStreamContext;
    this.solrMetricsContext = solrMetricsContext;
    this.metricsPath = metricsPath;
    this.priorityQueueSize = req.getParams().getInt(QUEUE_SIZE_PARAM, DEFAULT_QUEUE_SIZE);
    this.numWorkers = req.getParams().getInt(ParallelStream.NUM_WORKERS_PARAM, 1);
    this.workerId = req.getParams().getInt(ParallelStream.WORKER_ID_PARAM, 0);
    boolean useHashQuery = req.getParams().getBool(ParallelStream.USE_HASH_QUERY_PARAM, false);
    if (numWorkers > 1 && !useHashQuery) {
      String keysList = req.getParams().get(ParallelStream.PARTITION_KEYS_PARAM);
      if (keysList == null || keysList.trim().equals("none")) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "when numWorkers > 1 partitionKeys MUST be specified!");
      }
      partitionKeys = StrUtils.splitSmart(keysList, ',', true);
      // we have to use ALL parameters as a cache key to account for different queries
      partitionCacheKey = req.getParamString();
      tempPartitionCaches = new HashMap<>();
    } else {
      partitionKeys = null;
      partitionCacheKey = null;
      tempPartitionCaches = null;
    }
    this.fieldList = req.getParams().get(CommonParams.FL);
    this.batchSize = DEFAULT_BATCH_SIZE;
    this.partitionCaches = req.getSearcher().getCache(SOLR_CACHE_KEY);
  }

  @Override
  public String getContentType() {
    if ("javabin".equals(wt)) {
      return BinaryResponseParser.BINARY_CONTENT_TYPE;
    } else {
      return "json";
    }
  }

  @Override
  public void close() throws IOException {
    if (writer != null) {
      try {
        writer.close();
      } catch (Throwable t) {
        //We're going to sit on this.
      }
    }
    if (respWriter != null) {
      try {
        respWriter.flush();
        respWriter.close();
      } catch (Throwable t) {

      }
    }

  }

  protected void writeException(Exception e, PushWriter w, boolean logException) throws IOException {
    w.writeMap(mw -> {
      mw.put("responseHeader", singletonMap("status", 400))
          .put("response", makeMap(
              "numFound", 0,
              "docs", singletonList(singletonMap("EXCEPTION", e.getMessage()))));
    });
    if (logException) {
      SolrException.log(log, e);
    }
  }

  public void write(OutputStream os) throws IOException {
    try {
      _write(os);
    } finally {

    }
  }

  private void _write(OutputStream os) throws IOException {
    QueryResponseWriter rw = req.getCore().getResponseWriters().get(wt);
    if (rw instanceof BinaryResponseWriter) {
      //todo add support for other writers after testing
      writer = new JavaBinCodec(os, null);
    } else {
      respWriter = new OutputStreamWriter(os, StandardCharsets.UTF_8);
      writer = JSONResponseWriter.getPushWriter(respWriter, req, res);
    }
    Exception exception = res.getException();
    if (exception != null) {
      if (!(exception instanceof IgnoreException)) {
        writeException(exception, writer, false);
      }
      return;
    }

    SolrRequestInfo info = SolrRequestInfo.getRequestInfo();
    SortSpec sortSpec = info.getResponseBuilder().getSortSpec();

    if (sortSpec == null) {
      writeException((new IOException(new SyntaxError("No sort criteria was provided."))), writer, true);
      return;
    }

    SolrIndexSearcher searcher = req.getSearcher();
    Sort sort = searcher.weightSort(sortSpec.getSort());

    if (sort == null) {
      writeException((new IOException(new SyntaxError("No sort criteria was provided."))), writer, true);
      return;
    }

    if (sort != null && sort.needsScores()) {
      writeException((new IOException(new SyntaxError("Scoring is not currently supported with xsort."))), writer, true);
      return;
    }

    // There is a bailout in SolrIndexSearcher.getDocListNC when there are _no_ docs in the index at all.
    // if (lastDocRequested <= 0) {
    // That causes the totalHits and export entries in the context to _not_ get set.
    // The only time that really matters is when we search against an _empty_ set. That's too obscure
    // a condition to handle as part of this patch, if someone wants to pursue it it can be reproduced with:
    // ant test  -Dtestcase=StreamingTest -Dtests.method=testAllValidExportTypes -Dtests.seed=10F13879D0D1D6AD -Dtests.slow=true -Dtests.locale=es-PA -Dtests.timezone=America/Bahia_Banderas -Dtests.asserts=true -Dtests.file.encoding=ISO-8859-1
    // You'll have to uncomment the if below to hit the null pointer exception.
    // This is such an unusual case (i.e. an empty index) that catching this concdition here is probably OK.
    // This came to light in the very artifical case of indexing a single doc to Cloud.
    if (req.getContext().get("totalHits") != null) {
      totalHits = ((Integer) req.getContext().get("totalHits")).intValue();
      sets = (FixedBitSet[]) req.getContext().get("export");
      if (sets == null) {
        writeException((new IOException(new SyntaxError("xport RankQuery is required for xsort: rq={!xport}"))), writer, true);
        return;
      }
    }
    SolrParams params = req.getParams();

    String[] fields = null;

    if (fieldList == null) {
      writeException((new IOException(new SyntaxError("export field list (fl) must be specified."))), writer, true);
      return;
    } else {
      fields = fieldList.split(",");

      for (int i = 0; i < fields.length; i++) {

        fields[i] = fields[i].trim();

        if (fields[i].equals("score")) {
          writeException((new IOException(new SyntaxError("Scoring is not currently supported with xsort."))), writer, true);
          return;
        }
      }
    }

    try {
      fieldWriters = getFieldWriters(fields, req.getSearcher());
    } catch (Exception e) {
      writeException(e, writer, true);
      return;
    }

    outputDoc = new DoubleArrayMapWriter(fieldWriters.length);

    String expr = params.get(StreamParams.EXPR);
    if (expr != null) {
      StreamFactory streamFactory = initialStreamContext.getStreamFactory();
      streamFactory.withDefaultSort(params.get(CommonParams.SORT));
      try {
        StreamExpression expression = StreamExpressionParser.parse(expr);
        if (streamFactory.isEvaluator(expression)) {
          streamExpression = new StreamExpression(StreamParams.TUPLE);
          streamExpression.addParameter(new StreamExpressionNamedParameter(StreamParams.RETURN_VALUE, expression));
        } else {
          streamExpression = expression;
        }
      } catch (Exception e) {
        writeException(e, writer, true);
        return;
      }
      streamContext = new StreamContext();
      streamContext.setRequestParams(params);
      streamContext.setLocal(true);

      streamContext.workerID = 0;
      streamContext.numWorkers = 1;
      streamContext.setSolrClientCache(initialStreamContext.getSolrClientCache());
      streamContext.setModelCache(initialStreamContext.getModelCache());
      streamContext.setObjectCache(initialStreamContext.getObjectCache());
      streamContext.put("core", req.getCore().getName());
      streamContext.put("solr-core", req.getCore());
      streamContext.put(CommonParams.SORT, params.get(CommonParams.SORT));
    }

    try {
      writer.writeMap(m -> {
        m.put("responseHeader", singletonMap("status", 0));
        m.put("response", (MapWriter) mw -> {
          mw.put("numFound", totalHits);
          mw.put("docs", (IteratorWriter) iw -> writeDocs(req, os, iw, sort));
        });
      });
    } catch (java.io.EOFException e) {
      log.info("Caught Eof likely caused by early client disconnect");
    }

    if (streamContext != null) {
      streamContext = null;
    }
  }

  private TupleStream createTupleStream() throws IOException {
    StreamFactory streamFactory = (StreamFactory)initialStreamContext.getStreamFactory().clone();
    //Set the sort in the stream factory so it can be used during initialization.
    streamFactory.withDefaultSort(((String)streamContext.get(CommonParams.SORT)));
    TupleStream tupleStream = streamFactory.constructStream(streamExpression);
    tupleStream.setStreamContext(streamContext);
    return tupleStream;
  }

  protected void writeDocs(SolrQueryRequest req, OutputStream os, IteratorWriter.ItemWriter writer, Sort sort) throws IOException {
    List<LeafReaderContext> leaves = req.getSearcher().getTopReaderContext().leaves();
    final int queueSize = Math.min(batchSize, totalHits);
    if (tempPartitionCaches != null && partitionCaches != null) {
      initTempPartitionCaches(leaves);
    }

    ExportBuffers buffers = new ExportBuffers(this,
                                              leaves,
                                              req.getSearcher(),
                                              os,
                                              writer,
                                              sort,
                                              queueSize,
                                              totalHits,
                                              sets);


    if (streamExpression != null) {
      streamContext.put(ExportBuffers.EXPORT_BUFFERS_KEY, buffers);
      final TupleStream tupleStream;
      try {
        tupleStream = createTupleStream();
        tupleStream.open();
      } catch (Exception e) {
        buffers.getWriter().add((MapWriter) ew -> Tuple.EXCEPTION(e, true).writeMap(ew));
        return;
      }
      buffers.run(() -> {
        for (;;) {
          if (Thread.currentThread().isInterrupted()) {
            break;
          }
          final Tuple t;
          try {
            t = tupleStream.read();
          } catch (final Exception e) {
            buffers.getWriter().add((MapWriter) ew -> Tuple.EXCEPTION(e, true).writeMap(ew));
            break;
          }
          if (t == null) {
            break;
          }
          if (t.EOF && !t.EXCEPTION) {
            break;
          }
          // use decorated writer to monitor the number of output writes
          // and flush the output quickly in case of very few (reduced) output items
          buffers.getWriter().add((MapWriter) ew -> t.writeMap(ew));
          if (t.EXCEPTION && t.EOF) {
            break;
          }
        }
        return true;
      });
      tupleStream.close();
    } else {
      buffers.run(() -> {
        // get the initial buffer
        log.debug("--- writer init exchanging from empty");
        buffers.exchangeBuffers();
        ExportBuffers.Buffer buffer = buffers.getOutputBuffer();
        log.debug("--- writer init got {}", buffer);
        while (buffer.outDocsIndex != ExportBuffers.Buffer.NO_MORE_DOCS) {
          if (Thread.currentThread().isInterrupted()) {
            log.debug("--- writer interrupted");
            break;
          }
          try {
            for (int i = 0; i <= buffer.outDocsIndex; ++i) {
              // we're using the raw writer here because there's no potential
              // reduction in the number of output items, unlike when using
              // streaming expressions
              final SortDoc currentDoc = buffer.outDocs[i];
              MapWriter outputDoc = fillOutputDoc(currentDoc, leaves, fieldWriters);
              if (outputDoc != null) {
                writer.add(outputDoc);
              }
            }
          } finally {
          }
          //log.debug("--- writer exchanging from {}", buffer);
          try {
            long startExchangeBuffers = System.nanoTime();
            buffers.exchangeBuffers();
            long endExchangeBuffers = System.nanoTime();
            if (log.isDebugEnabled()) {
              log.debug("Waited for reader thread {}:", Long.toString(((endExchangeBuffers - startExchangeBuffers) / 1000000)));
            }
          } finally {
          }
          buffer = buffers.getOutputBuffer();
          //log.debug("--- writer got {}", buffer);
        }
        return true;
      });
    }
    transferTempPartitionCaches();
  }

  /**
   * This method transfers the newly built per-segment partitioning bitsets to the global cache,
   * keyed by the current query.
   */
  private void transferTempPartitionCaches() {
    if (tempPartitionCaches == null || partitionCaches == null) {
      return;
    }
    tempPartitionCaches.forEach((cacheKey, partitionSet) -> {
      SolrCache<String, FixedBitSet> perSegmentCache = partitionCaches.computeIfAbsent(cacheKey, k -> {
        CaffeineCache<String, FixedBitSet> cache = new CaffeineCache<>();
        cache.init(
                Map.of(
                        // 100 unique queries should be enough for anyone ;)
                        SolrCache.SIZE_PARAM, "100",
                        // evict entries after 600 sec
                        SolrCache.MAX_IDLE_TIME_PARAM, "600"),
                null, null);
        return cache;
      });
      // use our unique query+numWorkers+worker key
      perSegmentCache.put(partitionCacheKey, partitionSet);
    });
  }

  // this inits only those sets that are not already present in the global cache
  private void initTempPartitionCaches(List<LeafReaderContext> leaves) {
    tempPartitionCaches.clear();
    for (LeafReaderContext leaf : leaves) {
      IndexReader.CacheHelper cacheHelper = leaf.reader().getReaderCacheHelper();
      if (cacheHelper == null) {
        continue;
      }
      IndexReader.CacheKey cacheKey = cacheHelper.getKey();
      // check if bitset was computed earlier and can be skipped
      SolrCache<String, FixedBitSet> perSegmentCache = partitionCaches.get(cacheKey);
      if (perSegmentCache != null && perSegmentCache.get(partitionCacheKey) != null) {
        // already computed earlier
        continue;
      }
      tempPartitionCaches.put(cacheKey, new FixedBitSet(leaf.reader().maxDoc()));
    }
  }

  void fillNextBuffer(MergeIterator mergeIterator,
                      ExportBuffers.Buffer buffer) throws IOException {
    try {
      int outDocsIndex = -1;
      for (int i = 0; i < batchSize; i++) {
        SortDoc sortDoc = mergeIterator.next();
        if (sortDoc != null) {
          buffer.outDocs[++outDocsIndex].setValues(sortDoc);
        } else {
          break;
        }
      }
      buffer.outDocsIndex = outDocsIndex;
    } catch (Throwable t) {
      log.error("transfer", t);
      if (t instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      throw t;
    } finally {

    }
  }

  private static final class DoubleArrayMapWriter implements MapWriter, EntryWriter {
    final CharSequence[] keys;
    final Object[] values;
    int pos;

    DoubleArrayMapWriter(int size) {
      keys = new CharSequence[size];
      values = new Object[size];
      pos = 0;
    }

    @Override
    public EntryWriter put(CharSequence k, Object v) throws IOException {
      keys[pos] = k;
      values[pos] = v;
      pos++;
      return this;
    }

    public void clear() {
      for (int i = 0; i < pos; i++) {
        keys[i] = null;
        values[i] = null;
      }
      pos = 0;
    }

    @Override
    public void writeMap(EntryWriter ew) throws IOException {
      for (int i = 0; i < pos; i++) {
        ew.put(keys[i], values[i]);
      }
    }

    public Object get(CharSequence key) {
      for (int i = 0; i < pos; i++) {
        if (keys[i].equals(key)) {
          return values[i];
        }
      }
      return null;
    }
  }

  // we materialize this document so that we can potentially do hash partitioning
  private DoubleArrayMapWriter outputDoc;

  // WARNING: single-thread only! shared var tempDoc
  MapWriter fillOutputDoc(SortDoc sortDoc,
                          List<LeafReaderContext> leaves,
                          FieldWriter[] writers) throws IOException {
    int ord = sortDoc.ord;
    LeafReaderContext context = leaves.get(ord);
    // reuse
    outputDoc.clear();
    int fieldIndex = 0;
    for (FieldWriter fieldWriter : writers) {
      if (fieldWriter.write(sortDoc, context, outputDoc, fieldIndex)) {
        ++fieldIndex;
      }
    }
    if (partitionKeys != null) {
      return outputDoc;
    } else {
      // if we use partitioning then filter out unwanted docs
      return partitionFilter(sortDoc, context, outputDoc);
    }
  }

  MapWriter partitionFilter(SortDoc sortDoc, LeafReaderContext leaf, DoubleArrayMapWriter doc) {
    // calculate hash
    int hash = 0;
    for (String key : partitionKeys) {
      Object value = doc.get(key);
      if (value != null) {
        hash += value.hashCode();
      }
    }
    if ((hash & 0x7FFFFFFF) % numWorkers == workerId) {
      // our partition
      // check if we should mark it in the partitionSet
      IndexReader.CacheHelper cacheHelper = leaf.reader().getReaderCacheHelper();
      if (cacheHelper != null) {
        IndexReader.CacheKey cacheKey = cacheHelper.getKey();
        FixedBitSet partitionSet = tempPartitionCaches.get(cacheKey);
        if (partitionSet != null) {
          // not computed before - mark it
          partitionSet.set(sortDoc.docId);
        }
      }
      return doc;
    } else {
      // XXX update the cache
      return null;
    }
  }

  public FieldWriter[] getFieldWriters(String[] fields, SolrIndexSearcher searcher) throws IOException {
    IndexSchema schema = searcher.getSchema();
    FieldWriter[] writers = new FieldWriter[fields.length];
    for (int i = 0; i < fields.length; i++) {
      String field = fields[i];
      SchemaField schemaField = null;

      try {
        schemaField = schema.getField(field);
      } catch (Exception e) {
        throw new IOException(e);
      }

      if (!schemaField.hasDocValues()) {
        throw new IOException(schemaField + " must have DocValues to use this feature.");
      }
      boolean multiValued = schemaField.multiValued();
      FieldType fieldType = schemaField.getType();

      if (fieldType instanceof SortableTextField && schemaField.useDocValuesAsStored() == false) {
        throw new IOException(schemaField + " Must have useDocValuesAsStored='true' to be used with export writer");
      }

      if (fieldType instanceof IntValueFieldType) {
        if (multiValued) {
          writers[i] = new MultiFieldWriter(field, fieldType, schemaField, true);
        } else {
          writers[i] = new IntFieldWriter(field);
        }
      } else if (fieldType instanceof LongValueFieldType) {
        if (multiValued) {
          writers[i] = new MultiFieldWriter(field, fieldType, schemaField, true);
        } else {
          writers[i] = new LongFieldWriter(field);
        }
      } else if (fieldType instanceof FloatValueFieldType) {
        if (multiValued) {
          writers[i] = new MultiFieldWriter(field, fieldType, schemaField, true);
        } else {
          writers[i] = new FloatFieldWriter(field);
        }
      } else if (fieldType instanceof DoubleValueFieldType) {
        if (multiValued) {
          writers[i] = new MultiFieldWriter(field, fieldType, schemaField, true);
        } else {
          writers[i] = new DoubleFieldWriter(field);
        }
      } else if (fieldType instanceof StrField || fieldType instanceof SortableTextField) {
        if (multiValued) {
          writers[i] = new MultiFieldWriter(field, fieldType, schemaField, false);
        } else {
          writers[i] = new StringFieldWriter(field, fieldType);
        }
      } else if (fieldType instanceof DateValueFieldType) {
        if (multiValued) {
          writers[i] = new MultiFieldWriter(field, fieldType, schemaField, false);
        } else {
          writers[i] = new DateFieldWriter(field);
        }
      } else if (fieldType instanceof BoolField) {
        if (multiValued) {
          writers[i] = new MultiFieldWriter(field, fieldType, schemaField, true);
        } else {
          writers[i] = new BoolFieldWriter(field, fieldType);
        }
      } else {
        throw new IOException("Export fields must be one of the following types: int,float,long,double,string,date,boolean,SortableText");
      }
    }
    return writers;
  }

  SortDoc getSortDoc(SolrIndexSearcher searcher, SortField[] sortFields) throws IOException {
    SortValue[] sortValues = new SortValue[sortFields.length];
    IndexSchema schema = searcher.getSchema();
    for (int i = 0; i < sortFields.length; ++i) {
      SortField sf = sortFields[i];
      String field = sf.getField();
      boolean reverse = sf.getReverse();
      SchemaField schemaField = schema.getField(field);
      FieldType ft = schemaField.getType();

      if (!schemaField.hasDocValues()) {
        throw new IOException(field + " must have DocValues to use this feature.");
      }

      if (ft instanceof SortableTextField && schemaField.useDocValuesAsStored() == false) {
        throw new IOException(schemaField + " Must have useDocValuesAsStored='true' to be used with export writer");
      }

      if (ft instanceof IntValueFieldType) {
        if (reverse) {
          sortValues[i] = new IntValue(field, new IntComp.IntDesc());
        } else {
          sortValues[i] = new IntValue(field, new IntComp.IntAsc());
        }
      } else if (ft instanceof FloatValueFieldType) {
        if (reverse) {
          sortValues[i] = new FloatValue(field, new FloatComp.FloatDesc());
        } else {
          sortValues[i] = new FloatValue(field, new FloatComp.FloatAsc());
        }
      } else if (ft instanceof DoubleValueFieldType) {
        if (reverse) {
          sortValues[i] = new DoubleValue(field, new DoubleComp.DoubleDesc());
        } else {
          sortValues[i] = new DoubleValue(field, new DoubleComp.DoubleAsc());
        }
      } else if (ft instanceof LongValueFieldType) {
        if (reverse) {
          sortValues[i] = new LongValue(field, new LongComp.LongDesc());
        } else {
          sortValues[i] = new LongValue(field, new LongComp.LongAsc());
        }
      } else if (ft instanceof StrField || ft instanceof SortableTextField) {
        LeafReader reader = searcher.getSlowAtomicReader();
        SortedDocValues vals = reader.getSortedDocValues(field);
        if (reverse) {
          sortValues[i] = new StringValue(vals, field, new IntComp.IntDesc());
        } else {
          sortValues[i] = new StringValue(vals, field, new IntComp.IntAsc());
        }
      } else if (ft instanceof DateValueFieldType) {
        if (reverse) {
          sortValues[i] = new LongValue(field, new LongComp.LongDesc());
        } else {
          sortValues[i] = new LongValue(field, new LongComp.LongAsc());
        }
      } else if (ft instanceof BoolField) {
        // This is a bit of a hack, but since the boolean field stores ByteRefs, just like Strings
        // _and_ since "F" happens to sort before "T" (thus false sorts "less" than true)
        // we can just use the existing StringValue here.
        LeafReader reader = searcher.getSlowAtomicReader();
        SortedDocValues vals = reader.getSortedDocValues(field);
        if (reverse) {
          sortValues[i] = new StringValue(vals, field, new IntComp.IntDesc());
        } else {
          sortValues[i] = new StringValue(vals, field, new IntComp.IntAsc());
        }
      } else {
        throw new IOException("Sort fields must be one of the following types: int,float,long,double,string,date,boolean,SortableText");
      }
    }
    //SingleValueSortDoc etc are specialized classes which don't have array lookups. On benchmarking large datasets
    //This is faster than the using an array in SortDoc . So upto 4 sort fields we still want to keep specialized classes.
    //SOLR-12616 has more details
    if (sortValues.length == 1) {
      return new SingleValueSortDoc(sortValues[0]);
    } else if (sortValues.length == 2) {
      return new DoubleValueSortDoc(sortValues[0], sortValues[1]);
    } else if (sortValues.length == 3) {
      return new TripleValueSortDoc(sortValues[0], sortValues[1], sortValues[2]);
    } else if (sortValues.length == 4) {
      return new QuadValueSortDoc(sortValues[0], sortValues[1], sortValues[2], sortValues[3]);
    }
    return new SortDoc(sortValues);
  }


  static class MergeIterator {
    private TreeSet<SortDoc> set = new TreeSet<>();
    private SegmentIterator[] segmentIterators;
    private SortDoc outDoc;

    public MergeIterator(SegmentIterator[] segmentIterators, SortDoc proto) throws IOException {
      outDoc = proto.copy();
      this.segmentIterators = segmentIterators;
      for (int i = 0; i < segmentIterators.length; i++) {
        try {
          SortDoc sortDoc = segmentIterators[i].next();
          if (sortDoc != null) {
            set.add(sortDoc);
          }
        } catch (IOException e) {
          log.error("Error in MergeIterator: ", e);
          throw e;
        }
      }
    }

    /*
    * Merge sorts the SortDocs from Segment Iterators
    * Returns null when all docs are iterated.
    */

    public SortDoc next() throws IOException {
      SortDoc sortDoc = set.pollLast();
      //We've exhausted all documents
      if (sortDoc == null) {
        return null;
      } else {
        outDoc.setValues(sortDoc);
      }

      SortDoc nextDoc = segmentIterators[sortDoc.ord].next();
      if (nextDoc != null) {
        //The entire expense of the operation is here
        set.add(nextDoc);
      }
      return outDoc;
    }
  }

  public MergeIterator getMergeIterator(List<LeafReaderContext> leaves, FixedBitSet[] bits, SortDoc sortDoc) throws IOException {
    try {
      long totalDocs = 0;
      for (int i = 0; i < leaves.size(); i++) {
        totalDocs += leaves.get(i).reader().maxDoc();
      }

      //Resize the priorityQueueSize down for small result sets.
      this.priorityQueueSize = Math.min(this.priorityQueueSize, (int)(this.totalHits*1.2));

      if(log.isDebugEnabled()) {
        log.debug("Total priority queue size {}:", this.priorityQueueSize);
      }

      int[] sizes = new int[leaves.size()];

      int combineQueueSize = 0;
      for (int i = 0; i < leaves.size(); i++) {
        long maxDoc = leaves.get(i).reader().maxDoc();
        int sortQueueSize = Math.min((int) (((double) maxDoc / (double) totalDocs) * this.priorityQueueSize), batchSize);

        //Protect against too small a queue size as well
        if(sortQueueSize < 10) {
          sortQueueSize = 10;
        }

        if(log.isDebugEnabled()) {
          log.debug("Segment priority queue size {}:", sortQueueSize);
        }

        sizes[i] = sortQueueSize;
        combineQueueSize += sortQueueSize;

      }

      if(log.isDebugEnabled()) {
        log.debug("Combined priority queue size {}:", combineQueueSize);
      }

      SegmentIterator[] segmentIterators = new SegmentIterator[leaves.size()];
      for (int i = 0; i < segmentIterators.length; i++) {
        SortQueue sortQueue = new SortQueue(sizes[i], sortDoc.copy());
        // check if we have an existing partition filter and use it if present
        FixedBitSet myPartitionSet = partitionCacheKey != null ? getMyPartitionSet(leaves.get(i)) : null;
        segmentIterators[i] = new SegmentIterator(bits[i], myPartitionSet, leaves.get(i), sortQueue, sortDoc.copy());
      }

      return new MergeIterator(segmentIterators, sortDoc);
    } finally {
    }
  }

  private FixedBitSet getMyPartitionSet(LeafReaderContext leaf) {
    if (partitionCaches == null) {
      return null;
    }
    IndexReader.CacheHelper cacheHelper = leaf.reader().getReaderCacheHelper();
    if (cacheHelper == null) {
      return null;
    }
    IndexReader.CacheKey cacheKey = cacheHelper.getKey();

    SolrCache<String, FixedBitSet> perSegmentCaches = partitionCaches.get(cacheKey);
    if (perSegmentCaches == null) {
      // no queries yet for this segment
      return null;
    }
    return perSegmentCaches.get(partitionCacheKey);
  }

  private static class SegmentIterator {

    private final FixedBitSet bits;
    private final SortQueue queue;
    private final SortDoc sortDoc;
    private final LeafReaderContext context;
    private final SortDoc[] outDocs;

    private SortDoc nextDoc;
    private int index;


    /**
     * Construct per-segment iterator for matching docs.
     * @param bits matching document id-s in the segment
     * @param myPartitionSet filter to match only the docs in the current worker's partition, may be
     *                       null if not partitioning
     * @param context segment context
     * @param sortQueue sort queue
     * @param sortDoc proto sort document
     */
    public SegmentIterator(FixedBitSet bits, FixedBitSet myPartitionSet, LeafReaderContext context, SortQueue sortQueue, SortDoc sortDoc) throws IOException {
      this.bits = bits;
      if (myPartitionSet != null) {
        this.bits.and(myPartitionSet);
      }
      this.queue = sortQueue;
      this.sortDoc = sortDoc;
      this.nextDoc = sortDoc.copy();
      this.context = context;
      this.outDocs = new SortDoc[sortQueue.maxSize];
      topDocs();
    }

    public SortDoc next() throws IOException {
      SortDoc _sortDoc = null;
      if (index > -1) {
        _sortDoc = outDocs[index--];
      } else {
        topDocs();
        if (index > -1) {
          _sortDoc = outDocs[index--];
        }
      }

      if (_sortDoc != null) {
        //Clear the bit so it's not loaded again.
        bits.clear(_sortDoc.docId);

        //Load the global ordinal (only matters for strings)
        _sortDoc.setGlobalValues(nextDoc);

        nextDoc.setValues(_sortDoc);
        //We are now done with this doc.
        _sortDoc.reset();
      } else {
        nextDoc = null;
      }
      return nextDoc;
    }

    private void topDocs() throws IOException {
      try {
        queue.reset();
        SortDoc top = queue.top();
        this.sortDoc.setNextReader(context);
        DocIdSetIterator it = new BitSetIterator(bits, 0); // cost is not useful here
        int docId;
        while ((docId = it.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
          this.sortDoc.setValues(docId);
          if (top.lessThan(this.sortDoc)) {
            top.setValues(this.sortDoc);
            top = queue.updateTop();
          }
        }

        //Pop the queue and load up the array.
        index = -1;

        SortDoc _sortDoc;
        while ((_sortDoc = queue.pop()) != null) {
          if (_sortDoc.docId > -1) {
            outDocs[++index] = _sortDoc;
          }
        }
      } catch (Exception e) {
        log.error("Segment Iterator Error:", e);
        throw new IOException(e);
      } finally {

      }
    }
  }

  public static class IgnoreException extends IOException {
    public void printStackTrace(PrintWriter pw) {
      pw.print("Early Client Disconnect");
    }

    public String getMessage() {
      return "Early Client Disconnect";
    }
  }
}
