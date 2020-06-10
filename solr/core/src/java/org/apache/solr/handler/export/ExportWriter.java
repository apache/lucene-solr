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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Exchanger;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.FixedBitSet;
import org.apache.solr.client.solrj.impl.BinaryResponseParser;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.comp.ComparatorOrder;
import org.apache.solr.client.solrj.io.comp.FieldComparator;
import org.apache.solr.client.solrj.io.comp.MultipleFieldComparator;
import org.apache.solr.client.solrj.io.comp.StreamComparator;
import org.apache.solr.client.solrj.io.stream.StreamContext;
import org.apache.solr.client.solrj.io.stream.TupleStream;
import org.apache.solr.client.solrj.io.stream.expr.Explanation;
import org.apache.solr.client.solrj.io.stream.expr.Expressible;
import org.apache.solr.client.solrj.io.stream.expr.StreamExplanation;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionNamedParameter;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionParameter;
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
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.JavaBinCodec;
import org.apache.solr.common.util.SolrNamedThreadFactory;
import org.apache.solr.core.SolrCore;
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
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.search.SortSpec;
import org.apache.solr.search.SyntaxError;
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
 * bitmap identifies the smallest {@link #DOCUMENT_BATCH_SIZE} docs that haven't been sent yet and stores them in a
 * Priority Queue.  They are then exported (written across the wire) and marked as sent (unset in the bitmap).
 * This process repeats until all matching documents have been sent.
 * <p>
 * This streaming approach is light on memory (only {@link #DOCUMENT_BATCH_SIZE} documents are ever stored in memory at
 * once), and it allows {@link ExportWriter} to scale well with regard to numDocs.
 */
public class ExportWriter implements SolrCore.RawWriter, Closeable {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final int DOCUMENT_BATCH_SIZE = 30000;

  private static final String EXPORT_WRITER_KEY = "__ew__";
  private static final String EXPORT_BUFFERS_KEY = "__eb__";
  private static final String LEAF_READERS_KEY = "__leaves__";

  private OutputStreamWriter respWriter;
  final SolrQueryRequest req;
  final SolrQueryResponse res;
  final StreamContext initialStreamContext;
  StreamExpression streamExpression;
  StreamContext streamContext;
  FieldWriter[] fieldWriters;
  int totalHits = 0;
  FixedBitSet[] sets = null;
  PushWriter writer;
  private String wt;

  public static class ExportWriterStream extends TupleStream implements Expressible {
    StreamContext context;
    StreamComparator streamComparator;
    int pos = -1;
    ExportBuffers exportBuffers;
    Buffer buffer;

    public ExportWriterStream(StreamExpression expression, StreamFactory factory) throws IOException {
      streamComparator = parseComp(factory.getDefaultSort());
    }

    @Override
    public void setStreamContext(StreamContext context) {
      this.context = context;
    }

    @Override
    public List<TupleStream> children() {
      return null;
    }

    private StreamComparator parseComp(String sort) throws IOException {

      String[] sorts = sort.split(",");
      StreamComparator[] comps = new StreamComparator[sorts.length];
      for (int i = 0; i < sorts.length; i++) {
        String s = sorts[i];

        String[] spec = s.trim().split("\\s+"); //This should take into account spaces in the sort spec.

        if (spec.length != 2) {
          throw new IOException("Invalid sort spec:" + s);
        }

        String fieldName = spec[0].trim();
        String order = spec[1].trim();

        comps[i] = new FieldComparator(fieldName, order.equalsIgnoreCase("asc") ? ComparatorOrder.ASCENDING : ComparatorOrder.DESCENDING);
      }

      if (comps.length > 1) {
        return new MultipleFieldComparator(comps);
      } else {
        return comps[0];
      }
    }

    @Override
    public void open() throws IOException {
      exportBuffers = (ExportBuffers) context.get(EXPORT_BUFFERS_KEY);
      buffer = exportBuffers.getOutputBuffer();
    }

    @Override
    public void close() throws IOException {
      exportBuffers = null;
    }

    @Override
    public Tuple read() throws IOException {
      if (pos < 0) {
        try {
          buffer.outDocsIndex = Buffer.EMPTY;
          log.info("--- ews exchange empty buffer " + buffer);
          buffer = exportBuffers.exchanger.exchange(buffer);
          log.info("--- ews got new output buffer " + buffer);
        } catch (InterruptedException e) {
          log.info("--- ews interrupt");
          Thread.currentThread().interrupt();
          throw new IOException("interrupted");
        }
        if (buffer.outDocsIndex == Buffer.NO_MORE_DOCS) {
          log.info("--- ews EOF");
          return Tuple.EOF();
        } else {
          pos = buffer.outDocsIndex;
          log.info("--- ews new pos=" + pos);
        }
      }
      if (pos < 0) {
        log.info("--- ews EOF?");
        return Tuple.EOF();
      }
      Tuple tuple = new Tuple(buffer.outDocs[pos]);
      pos--;
      return tuple;
    }

    @Override
    public StreamComparator getStreamSort() {
      return streamComparator;
    }

    @Override
    public StreamExpressionParameter toExpression(StreamFactory factory) throws IOException {
      StreamExpression expression = new StreamExpression(factory.getFunctionName(this.getClass()));
      return expression;
    }

    @Override
    public Explanation toExplanation(StreamFactory factory) throws IOException {
      return new StreamExplanation(getStreamNodeId().toString())
          .withFunctionName("input")
          .withImplementingClass(this.getClass().getName())
          .withExpressionType(Explanation.ExpressionType.STREAM_SOURCE)
          .withExpression("--non-expressible--");
    }
  }


  public ExportWriter(SolrQueryRequest req, SolrQueryResponse res, String wt, StreamContext initialStreamContext) {
    this.req = req;
    this.res = res;
    this.wt = wt;
    this.initialStreamContext = initialStreamContext;
  }

  @Override
  public String getContentType() {
    if ("javabin".equals(wt)) {
      return BinaryResponseParser.BINARY_CONTENT_TYPE;
    } else return "json";
  }

  @Override
  public void close() throws IOException {
    if (writer != null) writer.close();
    if (respWriter != null) {
      respWriter.flush();
      respWriter.close();
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
    String fl = params.get("fl");

    String[] fields = null;

    if (fl == null) {
      writeException((new IOException(new SyntaxError("export field list (fl) must be specified."))), writer, true);
      return;
    } else {
      fields = fl.split(",");

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

    writer.writeMap(m -> {
      m.put("responseHeader", singletonMap("status", 0));
      m.put("response", (MapWriter) mw -> {
        mw.put("numFound", totalHits);
        mw.put("docs", (IteratorWriter) iw -> writeDocs(req, iw, sort));
      });
    });
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

  protected void identifyLowestSortingUnexportedDocs(List<LeafReaderContext> leaves, SortDoc sortDoc, SortQueue queue) throws IOException {
    queue.reset();
    SortDoc top = queue.top();
    for (int i = 0; i < leaves.size(); i++) {
      sortDoc.setNextReader(leaves.get(i));
      DocIdSetIterator it = new BitSetIterator(sets[i], 0); // cost is not useful here
      int docId;
      while ((docId = it.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
        sortDoc.setValues(docId);
        if (top.lessThan(sortDoc)) {
          top.setValues(sortDoc);
          top = queue.updateTop();
        }
      }
    }
  }

  protected void transferBatchToBufferForOutput(SortQueue queue, SortDoc[] outDocs,
                                                List<LeafReaderContext> leaves, Buffer destination) throws IOException {
    int outDocsIndex = -1;
    for (int i = 0; i < queue.maxSize; i++) {
      SortDoc s = queue.pop();
      if (s.docId > -1) {
        outDocs[++outDocsIndex] = s;
      }
    }
    destination.outDocsIndex = outDocsIndex;
    materializeDocs(leaves, outDocs, destination);
  }

  private static final class Buffer implements EntryWriter {
    static final int EMPTY = -1;
    static final int NO_MORE_DOCS = -2;

    int outDocsIndex = EMPTY;
    // use array-of-arrays instead of Map to conserve space
    Object[][] outDocs;
    int pos = EMPTY;

    EntryWriter getEntryWriter(int pos, int numFields) {
      if (outDocs == null) {
        outDocs = new Object[outDocsIndex + 1][];
      }
      this.pos = pos;
      Object[] fields = new Object[numFields << 1];
      outDocs[pos] = fields;
      return this;
    }

    @Override
    public EntryWriter put(CharSequence k, Object v) throws IOException {
      if (pos < 0) {
        throw new IOException("Invalid entry position");
      }
      Object[] fields = outDocs[pos];
      boolean putOk = false;
      for (int i = 0; i < fields.length; i += 2) {
        if (fields[i] == null || fields[i].equals(k)) {
          fields[i] = k;
          // convert everything complex into POJOs at this point
          if (v instanceof IteratorWriter) {
            List lst = new ArrayList();
            ((IteratorWriter)v).toList(lst);
            v = lst;
          } else if (v instanceof MapWriter) {
            Map<String, Object> map = new HashMap<>();
            ((MapWriter)v).toMap(map);
            v = map;
          }
          fields[i + 1] = v;
          putOk = true;
          break;
        }
      }
      if (!putOk) {
        throw new IOException("should not happen! pos=" + pos + " ran out of space for field " + k + "=" + v
            +  " - already full: " + Arrays.toString(fields));
      }
      return this;
    }

    public void writeItem(int pos, IteratorWriter.ItemWriter itemWriter) throws IOException {
      final Object[] fields = outDocs[pos];
      if (fields == null) {
        log.info("--- writeItem no fields at " + pos);
        return;
      }
      itemWriter.add((MapWriter) ew -> {
        for (int i = 0; i < fields.length; i += 2) {
          if (fields[i] == null) {
            log.info("--- writeItem empty field at " + pos + "/" + i);
            continue;
          }
          log.info("--- writeItem field " + pos + "/" + i);
          ew.put((CharSequence)fields[i], fields[i + 1]);
          log.info("--- writeItem done field " + pos + "/" + i);
        }
      });
    }

    public void reset() {
      outDocsIndex = EMPTY;
      pos = EMPTY;
      outDocs = null;
    }

    @Override
    public String toString() {
      return "Buffer@" + Integer.toHexString(hashCode()) + "{" +
          "outDocsIndex=" + outDocsIndex +
          '}';
    }
  }

  /* Helper class implementing a "double buffering" producer / consumer. */
  private static final class ExportBuffers {
    static final long EXCHANGE_TIMEOUT_SECONDS = 300;

    final Buffer bufferOne;
    final Buffer bufferTwo;
    final Exchanger<Buffer> exchanger = new Exchanger<>();
    final SortDoc[] outDocs;
    final List<LeafReaderContext> leaves;
    final ExportWriter exportWriter;
    final int totalHits;
    Buffer fillBuffer;
    Buffer outputBuffer;
    Runnable filler;
    ExecutorService service;

    ExportBuffers (ExportWriter exportWriter, List<LeafReaderContext> leaves, SolrIndexSearcher searcher, Sort sort, int queueSize, int totalHits) {
      this.exportWriter = exportWriter;
      this.leaves = leaves;
      this.outDocs = new SortDoc[queueSize];
      this.bufferOne = new Buffer();
      this.bufferTwo = new Buffer();
      this.totalHits = totalHits;
      fillBuffer = bufferOne;
      outputBuffer = bufferTwo;
      filler = () -> {
        try {
          SortDoc sortDoc = exportWriter.getSortDoc(searcher, sort.getSort());
          SortQueue queue = new SortQueue(queueSize, sortDoc);
          for (int count = 0; count < totalHits; ) {
            log.info("--- filler fillOutDocs in " + fillBuffer);
            exportWriter.fillOutDocs(leaves, sortDoc, queue, outDocs, fillBuffer);
            count += (fillBuffer.outDocsIndex + 1);
            log.info("--- filler count=" + count + ", exchange buffer from " + fillBuffer);
            fillBuffer = exchanger.exchange(fillBuffer, EXCHANGE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            log.info("--- filler got empty buffer " + fillBuffer);
          }
          fillBuffer.outDocsIndex = Buffer.NO_MORE_DOCS;
          log.info("--- filler final exchange buffer from " + fillBuffer);
          fillBuffer = exchanger.exchange(fillBuffer, EXCHANGE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
          log.info("--- filler final got buffer " + fillBuffer);
        } catch (Exception e) {
          log.error("filler", e);
          shutdownNow();
        }
      };
    }

    public Buffer getOutputBuffer() {
      return outputBuffer;
    }

    public Buffer exchange(Buffer buffer) throws InterruptedException, TimeoutException {
      return exchanger.exchange(buffer, EXCHANGE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    public void shutdownNow() {
      if (service != null) {
        log.info("--- shutting down buffers");
        service.shutdownNow();
      }
    }

    /**
     * Start processing and block until complete or Exception is thrown.
     * @param writer writer that exchanges and processes buffers received from a producer.
     * @throws IOException on errors
     */
    public void run(Callable<Boolean> writer) throws IOException {
      service = ExecutorUtil.newMDCAwareFixedThreadPool(2, new SolrNamedThreadFactory("ExportBuffers"));
      try {
        CompletableFuture.runAsync(filler, service);
        writer.call();

        // alternatively we could run the writer in a separate thread too:
//        CompletableFuture<Void> allDone = CompletableFuture.allOf(
//            CompletableFuture.runAsync(filler, service),
//            CompletableFuture.runAsync(() -> {
//              try {
//                writer.call();
//              } catch (Exception e) {
//                log.error("writer", e);
//                shutdownNow();
//              }
//            }, service)
//        );
//        allDone.join();
        log.info("-- finished.");
      } catch (Exception e) {
        log.error("Exception running filler / writer", e);
         //
      } finally {
        log.info("--- all done, shutting down buffers");
        service.shutdownNow();
      }
    }
  }

  protected void writeDocs(SolrQueryRequest req, IteratorWriter.ItemWriter writer, Sort sort) throws IOException {
    List<LeafReaderContext> leaves = req.getSearcher().getTopReaderContext().leaves();
    SortDoc sortDoc = getSortDoc(req.getSearcher(), sort.getSort());
    final int queueSize = Math.min(DOCUMENT_BATCH_SIZE, totalHits);

    ExportBuffers buffers = new ExportBuffers(this, leaves, req.getSearcher(), sort, queueSize, totalHits);

    if (streamExpression != null) {
      streamContext.put(EXPORT_WRITER_KEY, this);
      streamContext.put(EXPORT_BUFFERS_KEY, buffers);
      streamContext.put(LEAF_READERS_KEY, leaves);
      final TupleStream tupleStream = createTupleStream();
      tupleStream.open();
      buffers.run(() -> {
        for (;;) {
          if (Thread.currentThread().isInterrupted()) {
            break;
          }
          final Tuple t = tupleStream.read();
          if (t == null) {
            break;
          }
          if (t.EOF) {
            break;
          }
          writer.add((MapWriter) ew -> t.writeMap(ew));
        }
        return true;
      });
      tupleStream.close();
    } else {
      buffers.run(() -> {
        Buffer buffer = buffers.getOutputBuffer();
        log.info("--- writer init exchanging from " + buffer);
        buffer = buffers.exchange(buffer);
        log.info("--- writer init got " + buffer);
        while (buffer.outDocsIndex != Buffer.NO_MORE_DOCS) {
          if (Thread.currentThread().isInterrupted()) {
            log.info("--- writer interrupted");
            break;
          }
          for (int i = buffer.outDocsIndex; i >= 0; --i) {
            buffer.writeItem(i, writer);
          }
          log.info("--- writer exchanging from " + buffer);
          buffer = buffers.exchange(buffer);
          log.info("--- writer got " + buffer);
        }
        return true;
      });
    }
  }

  private void fillOutDocs(List<LeafReaderContext> leaves, SortDoc sortDoc,
                          SortQueue sortQueue, SortDoc[] outDocs, Buffer buffer) throws IOException {
    identifyLowestSortingUnexportedDocs(leaves, sortDoc, sortQueue);
    transferBatchToBufferForOutput(sortQueue, outDocs, leaves, buffer);
  }

  private void materializeDocs(List<LeafReaderContext> leaves, SortDoc[] outDocs, Buffer buffer) throws IOException {
    log.info("--- materialize docs in " + buffer);
    if (buffer.outDocsIndex < 0) {
      return;
    }
    for (int i = buffer.outDocsIndex; i >= 0; i--) {
      SortDoc sortDoc = outDocs[i];
      EntryWriter ew = buffer.getEntryWriter(i, fieldWriters.length);
      writeDoc(sortDoc, leaves, ew);
      sortDoc.reset();
    }
  }

  void writeDoc(SortDoc sortDoc,
                          List<LeafReaderContext> leaves,
                          EntryWriter ew) throws IOException {
    int ord = sortDoc.ord;
    FixedBitSet set = sets[ord];
    set.clear(sortDoc.docId);
    LeafReaderContext context = leaves.get(ord);
    int fieldIndex = 0;
    for (FieldWriter fieldWriter : fieldWriters) {
      if (fieldWriter.write(sortDoc, context.reader(), ew, fieldIndex)) {
        ++fieldIndex;
      }
    }
  }

  protected FieldWriter[] getFieldWriters(String[] fields, SolrIndexSearcher searcher) throws IOException {
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

  private SortDoc getSortDoc(SolrIndexSearcher searcher, SortField[] sortFields) throws IOException {
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

  public static class IgnoreException extends IOException {
    public void printStackTrace(PrintWriter pw) {
      pw.print("Early Client Disconnect");
    }

    public String getMessage() {
      return "Early Client Disconnect";
    }
  }

}
