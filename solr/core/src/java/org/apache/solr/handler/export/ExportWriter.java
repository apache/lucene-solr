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
import java.util.List;

import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.FixedBitSet;
import org.apache.solr.client.solrj.impl.BinaryResponseParser;
import org.apache.solr.common.IteratorWriter;
import org.apache.solr.common.MapWriter;
import org.apache.solr.common.MapWriter.EntryWriter;
import org.apache.solr.common.PushWriter;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.JavaBinCodec;
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
  private static final int DOCUMENT_BATCH_SIZE = 30000;
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private OutputStreamWriter respWriter;
  final SolrQueryRequest req;
  final SolrQueryResponse res;
  FieldWriter[] fieldWriters;
  int totalHits = 0;
  FixedBitSet[] sets = null;
  PushWriter writer;
  private String wt;


  public ExportWriter(SolrQueryRequest req, SolrQueryResponse res, String wt) {
    this.req = req;
    this.res = res;
    this.wt = wt;

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

    writer.writeMap(m -> {
      m.put("responseHeader", singletonMap("status", 0));
      m.put("response", (MapWriter) mw -> {
        mw.put("numFound", totalHits);
        mw.put("docs", (IteratorWriter) iw -> writeDocs(req, iw, sort));
      });
    });

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

  protected int transferBatchToArrayForOutput(SortQueue queue, SortDoc[] destinationArr) {
    int outDocsIndex = -1;
    for (int i = 0; i < queue.maxSize; i++) {
      SortDoc s = queue.pop();
      if (s.docId > -1) {
        destinationArr[++outDocsIndex] = s;
      }
    }

    return outDocsIndex;
  }

  protected void addDocsToItemWriter(List<LeafReaderContext> leaves, IteratorWriter.ItemWriter writer, SortDoc[] docsToExport, int outDocsIndex) throws IOException {
    try {
      for (int i = outDocsIndex; i >= 0; --i) {
        SortDoc s = docsToExport[i];
        writer.add((MapWriter) ew -> {
          writeDoc(s, leaves, ew);
          s.reset();
        });
      }
    } catch (Throwable e) {
      Throwable ex = e;
      while (ex != null) {
        String m = ex.getMessage();
        if (m != null && m.contains("Broken pipe")) {
          throw new IgnoreException();
        }
        ex = ex.getCause();
      }

      if (e instanceof IOException) {
        throw ((IOException) e);
      } else {
        throw new IOException(e);
      }
    }
  }

  protected void writeDocs(SolrQueryRequest req, IteratorWriter.ItemWriter writer, Sort sort) throws IOException {
    List<LeafReaderContext> leaves = req.getSearcher().getTopReaderContext().leaves();
    SortDoc sortDoc = getSortDoc(req.getSearcher(), sort.getSort());
    int count = 0;
    final int queueSize = Math.min(DOCUMENT_BATCH_SIZE, totalHits);

    SortQueue queue = new SortQueue(queueSize, sortDoc);
    SortDoc[] outDocs = new SortDoc[queueSize];

    while (count < totalHits) {
      identifyLowestSortingUnexportedDocs(leaves, sortDoc, queue);
      int outDocsIndex = transferBatchToArrayForOutput(queue, outDocs);

      count += (outDocsIndex + 1);
      addDocsToItemWriter(leaves, writer, outDocs, outDocsIndex);
    }
  }

  protected void writeDoc(SortDoc sortDoc,
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
          sortValues[i] = new IntValue(field, new IntDesc());
        } else {
          sortValues[i] = new IntValue(field, new IntAsc());
        }
      } else if (ft instanceof FloatValueFieldType) {
        if (reverse) {
          sortValues[i] = new FloatValue(field, new FloatDesc());
        } else {
          sortValues[i] = new FloatValue(field, new FloatAsc());
        }
      } else if (ft instanceof DoubleValueFieldType) {
        if (reverse) {
          sortValues[i] = new DoubleValue(field, new DoubleDesc());
        } else {
          sortValues[i] = new DoubleValue(field, new DoubleAsc());
        }
      } else if (ft instanceof LongValueFieldType) {
        if (reverse) {
          sortValues[i] = new LongValue(field, new LongDesc());
        } else {
          sortValues[i] = new LongValue(field, new LongAsc());
        }
      } else if (ft instanceof StrField || ft instanceof SortableTextField) {
        LeafReader reader = searcher.getSlowAtomicReader();
        SortedDocValues vals = reader.getSortedDocValues(field);
        if (reverse) {
          sortValues[i] = new StringValue(vals, field, new IntDesc());
        } else {
          sortValues[i] = new StringValue(vals, field, new IntAsc());
        }
      } else if (ft instanceof DateValueFieldType) {
        if (reverse) {
          sortValues[i] = new LongValue(field, new LongDesc());
        } else {
          sortValues[i] = new LongValue(field, new LongAsc());
        }
      } else if (ft instanceof BoolField) {
        // This is a bit of a hack, but since the boolean field stores ByteRefs, just like Strings
        // _and_ since "F" happens to sort before "T" (thus false sorts "less" than true)
        // we can just use the existing StringValue here.
        LeafReader reader = searcher.getSlowAtomicReader();
        SortedDocValues vals = reader.getSortedDocValues(field);
        if (reverse) {
          sortValues[i] = new StringValue(vals, field, new IntDesc());
        } else {
          sortValues[i] = new StringValue(vals, field, new IntAsc());
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
