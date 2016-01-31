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
    
package org.apache.solr.response;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.Writer;
import java.lang.invoke.MethodHandles;
import java.util.List;
import java.util.ArrayList;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.MultiDocValues;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CharsRefBuilder;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.LongValues;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestInfo;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.schema.StrField;
import org.apache.solr.schema.TrieDoubleField;
import org.apache.solr.schema.TrieFloatField;
import org.apache.solr.schema.TrieIntField;
import org.apache.solr.schema.TrieLongField;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.search.SortSpec;
import org.apache.solr.search.SyntaxError;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SortingResponseWriter implements QueryResponseWriter {

  private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public void init(NamedList args) {
    /* NOOP */
  }

  public String getContentType(SolrQueryRequest req, SolrQueryResponse res) {
    return "application/json";
  }

  public void write(Writer writer, SolrQueryRequest req, SolrQueryResponse res) throws IOException {
    Exception e1 = res.getException();
    if(e1 != null) {
      if(!(e1 instanceof IgnoreException)) {
        writeException(e1, writer, false);
      }
      return;
    }

    SolrRequestInfo info = SolrRequestInfo.getRequestInfo();
    SortSpec sortSpec = info.getResponseBuilder().getSortSpec();
    Exception exception = null;

    if(sortSpec == null) {
      exception = new IOException(new SyntaxError("No sort criteria was provided."));
    }

    SolrIndexSearcher searcher = req.getSearcher();
    Sort sort = searcher.weightSort(sortSpec.getSort());

    if(sort == null) {
      exception = new IOException(new SyntaxError("No sort criteria was provided."));
    }

    if(sort != null && sort.needsScores()) {
      exception = new IOException(new SyntaxError("Scoring is not currently supported with xsort."));
    }

    FixedBitSet[] sets = (FixedBitSet[])req.getContext().get("export");
    Integer th = (Integer)req.getContext().get("totalHits");

    if(sets == null) {
      exception = new IOException(new SyntaxError("xport RankQuery is required for xsort: rq={!xport}"));
    }

    int totalHits = th.intValue();
    SolrParams params = req.getParams();
    String fl = params.get("fl");

    String[] fields = null;

    if(fl == null) {
      exception = new IOException(new SyntaxError("export field list (fl) must be specified."));
    } else  {
      fields = fl.split(",");

      for(int i=0;i<fields.length; i++) {

        fields[i] = fields[i].trim();

        if(fields[i].equals("score")) {
          exception =  new IOException(new SyntaxError("Scoring is not currently supported with xsort."));
          break;
        }
      }
    }

    FieldWriter[] fieldWriters = null;

    try {
      fieldWriters = getFieldWriters(fields, req.getSearcher());
    }catch(Exception e) {
      exception = e;
    }


    if(exception != null) {
      writeException(exception, writer, true);
      return;
    }

    writer.write("{\"responseHeader\": {\"status\": 0}, \"response\":{\"numFound\":"+totalHits+", \"docs\":[");


    //Write the data.
    List<LeafReaderContext> leaves = req.getSearcher().getTopReaderContext().leaves();
    SortDoc sortDoc = getSortDoc(req.getSearcher(), sort.getSort());
    int count = 0;
    int queueSize = 30000;
    SortQueue queue = new SortQueue(queueSize, sortDoc);
    SortDoc[] outDocs = new SortDoc[queueSize];

    boolean commaNeeded = false;
    while(count < totalHits) {
      //long begin = System.nanoTime();
      queue.reset();
      SortDoc top = queue.top();
      for(int i=0; i<leaves.size(); i++) {
        sortDoc.setNextReader(leaves.get(i));
        DocIdSetIterator it = new BitSetIterator(sets[i], 0); // cost is not useful here
        int docId = -1;
        while((docId = it.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
          sortDoc.setValues(docId);
          if(top.lessThan(sortDoc)) {
            top.setValues(sortDoc);
            top = queue.updateTop();
          }
        }
      }

      int outDocsIndex = -1;

      for(int i=0; i<queueSize; i++) {
        SortDoc s = queue.pop();
        if(s.docId > -1) {
          outDocs[++outDocsIndex] = s;
        }
      }

     //long end = System.nanoTime();

      count += (outDocsIndex+1);

      try {
        for(int i=outDocsIndex; i>=0; --i) {
          SortDoc s = outDocs[i];
          if(commaNeeded){writer.write(',');}
          writer.write('{');
          writeDoc(s, leaves, fieldWriters, sets, writer);
          writer.write('}');
          commaNeeded = true;
          s.reset();
        }
      } catch(Throwable e) {
        Throwable ex = e;
        e.printStackTrace();
        while(ex != null) {
          String m = ex.getMessage();
          if(m != null && m.contains("Broken pipe")) {
            throw new IgnoreException();
          }
          ex = ex.getCause();
        }

        if(e instanceof IOException) {
          throw ((IOException)e);
        } else {
          throw new IOException(e);
        }
      }
    }

    //System.out.println("Sort Time 2:"+Long.toString(total/1000000));
    writer.write("]}}");
    writer.flush();
  }

  public static class IgnoreException extends IOException {
    public void printStackTrace(PrintWriter pw) {
      pw.print("Early Client Disconnect");

    }

    public String getMessage() {
      return "Early Client Disconnect";
    }
  }


  protected void writeDoc(SortDoc sortDoc,
                          List<LeafReaderContext> leaves,
                          FieldWriter[] fieldWriters,
                          FixedBitSet[] sets,
                          Writer out) throws IOException{

    int ord = sortDoc.ord;
    FixedBitSet set = sets[ord];
    set.clear(sortDoc.docId);
    LeafReaderContext context = leaves.get(ord);
    int fieldIndex = 0;
    for(FieldWriter fieldWriter : fieldWriters) {
      if(fieldWriter.write(sortDoc.docId, context.reader(), out, fieldIndex)){
        ++fieldIndex;
      }
    }
  }

  protected void writeException(Exception e, Writer out, boolean log) throws IOException{
    out.write("{\"responseHeader\": {\"status\": 400}, \"response\":{\"numFound\":0, \"docs\":[");
    out.write("{\"EXCEPTION\":\"");
    writeStr(e.getMessage(), out);
    out.write("\"}");
    out.write("]}}");
    out.flush();
    if(log) {
      SolrException.log(logger, e);
    }
  }

  protected FieldWriter[] getFieldWriters(String[] fields, SolrIndexSearcher searcher) throws IOException {
    IndexSchema schema = searcher.getSchema();
    FieldWriter[] writers = new FieldWriter[fields.length];
    for(int i=0; i<fields.length; i++) {
      String field = fields[i];
      SchemaField schemaField = null;

      try {
        schemaField = schema.getField(field);
      } catch (Exception e) {
        throw new IOException(e);
      }

      if(!schemaField.hasDocValues()) {
        throw new IOException(field+" must have DocValues to use this feature.");
      }

      boolean multiValued = schemaField.multiValued();
      FieldType fieldType = schemaField.getType();
      if(fieldType instanceof TrieIntField) {
        if(multiValued) {
          writers[i] = new MultiFieldWriter(field, fieldType, true);
        } else {
          writers[i] = new IntFieldWriter(field);
        }
      } else if (fieldType instanceof TrieLongField) {
        if(multiValued) {
          writers[i] = new MultiFieldWriter(field, fieldType, true);
        } else {
          writers[i] = new LongFieldWriter(field);
        }
      } else if (fieldType instanceof TrieFloatField) {
        if(multiValued) {
          writers[i] = new MultiFieldWriter(field, fieldType, true);
        } else {
          writers[i] = new FloatFieldWriter(field);
        }
      } else if(fieldType instanceof TrieDoubleField) {
        if(multiValued) {
          writers[i] = new MultiFieldWriter(field, fieldType, true);
        } else {
          writers[i] = new DoubleFieldWriter(field);
        }
      } else if(fieldType instanceof StrField) {
        if(multiValued) {
          writers[i] = new MultiFieldWriter(field, fieldType, false);
        } else {
          writers[i] = new StringFieldWriter(field, fieldType);
        }
      } else {
        throw new IOException("Export fields must either be one of the following types: int,float,long,double,string");
      }
    }
    return writers;
  }

  private SortDoc getSortDoc(SolrIndexSearcher searcher, SortField[] sortFields) throws IOException {
    SortValue[] sortValues = new SortValue[sortFields.length];
    IndexSchema schema = searcher.getSchema();
    for(int i=0; i<sortFields.length; ++i) {
      SortField sf = sortFields[i];
      String field = sf.getField();
      boolean reverse = sf.getReverse();
      SchemaField schemaField = schema.getField(field);
      FieldType ft = schemaField.getType();

      if(!schemaField.hasDocValues()) {
        throw new IOException(field+" must have DocValues to use this feature.");
      }

      if(ft instanceof TrieIntField) {
        if(reverse) {
          sortValues[i] = new IntValue(field, new IntDesc());
        } else {
          sortValues[i] = new IntValue(field, new IntAsc());
        }
      } else if(ft instanceof TrieFloatField) {
        if(reverse) {
          sortValues[i] = new FloatValue(field, new FloatDesc());
        } else {
          sortValues[i] = new FloatValue(field, new FloatAsc());
        }
      } else if(ft instanceof TrieDoubleField) {
        if(reverse) {
          sortValues[i] = new DoubleValue(field, new DoubleDesc());
        } else {
          sortValues[i] = new DoubleValue(field, new DoubleAsc());
        }
      } else if(ft instanceof TrieLongField) {
        if(reverse) {
          sortValues[i] = new LongValue(field, new LongDesc());
        } else {
          sortValues[i] = new LongValue(field, new LongAsc());
        }
      } else if(ft instanceof StrField) {
        LeafReader reader = searcher.getLeafReader();
        SortedDocValues vals =  reader.getSortedDocValues(field);
        if(reverse) {
          sortValues[i] = new StringValue(vals, field, new IntDesc());
        } else {
          sortValues[i] = new StringValue(vals, field, new IntAsc());
        }
      } else {
        throw new IOException("Sort fields must be one of the following types: int,float,long,double,string");
      }
    }

    if(sortValues.length == 1) {
      return new SingleValueSortDoc(sortValues[0]);
    } else if(sortValues.length == 2) {
      return new DoubleValueSortDoc(sortValues[0], sortValues[1]);
    } else if(sortValues.length == 3) {
      return new TripleValueSortDoc(sortValues[0], sortValues[1], sortValues[2]);
    } else if(sortValues.length == 4) {
      return new QuadValueSortDoc(sortValues[0], sortValues[1], sortValues[2], sortValues[3]);
    } else {
      throw new IOException("A max of 4 sorts can be specified");
    }
  }

  class SortQueue extends PriorityQueue<SortDoc> {

    private SortDoc proto;
    private Object[] cache;

    public SortQueue(int len, SortDoc proto) {
      super(len);
      this.proto = proto;
    }

    protected boolean lessThan(SortDoc t1, SortDoc t2) {
      return t1.lessThan(t2);
    }

    private void populate() {
      Object[] heap = getHeapArray();
      cache = new SortDoc[heap.length];
      for(int i=1; i<heap.length; i++) {
        cache[i] = heap[i]  = proto.copy();
      }
      size = maxSize;
    }

    private void reset() {
      Object[] heap = getHeapArray();
      if(cache != null) {
        System.arraycopy(cache, 1, heap, 1, heap.length-1);
        size = maxSize;
      } else {
        populate();
      }
    }
  }

  class SortDoc {

    protected int docId = -1;
    protected int ord = -1;
    protected int docBase = -1;

    private SortValue[] sortValues;

    public SortDoc() {

    }

    public void setNextReader(LeafReaderContext context) throws IOException {
      this.ord = context.ord;
      for(SortValue value : sortValues) {
        value.setNextReader(context);
      }
    }

    public void reset() {
      this.docId = -1;
    }

    public void setValues(int docId) throws IOException {
      this.docId = docId;
      for(SortValue sortValue : sortValues) {
        sortValue.setCurrentValue(docId);
      }
    }

    public void setValues(SortDoc sortDoc) throws IOException {
      this.docId = sortDoc.docId;
      this.ord = sortDoc.ord;
      SortValue[] vals = sortDoc.sortValues;
      for(int i=0; i<vals.length; i++) {
        sortValues[i].setCurrentValue(vals[i]);
      }
    }

    public SortDoc(SortValue[] sortValues) {
      this.sortValues = sortValues;
    }

    public SortDoc copy() {
      SortValue[] svs = new SortValue[sortValues.length];
      for(int i=0; i<sortValues.length; i++) {
        svs[i] = sortValues[i].copy();
      }

      return new SortDoc(svs);
    }

    public boolean lessThan(Object o) {
      if(docId == -1) {
        return true;
      }

      SortDoc sd = (SortDoc)o;
      SortValue[] sortValues1 = sd.sortValues;
      for(int i=0; i<sortValues.length; i++) {
        int comp = sortValues[i].compareTo(sortValues1[i]);
        if(comp < 0) {
          return true;
        } if(comp > 0) {
          return false;
        }
      }
      return docId+docBase < sd.docId+sd.docBase;
    }

    public String toString() {
      return "";
    }
  }

  class SingleValueSortDoc extends SortDoc {

    protected SortValue value1;

    public void setNextReader(LeafReaderContext context) throws IOException {
      this.ord = context.ord;
      value1.setNextReader(context);
    }

    public void reset() {
      this.docId = -1;
      this.value1.reset();
    }

    public void setValues(int docId) throws IOException {
      this.docId = docId;
      value1.setCurrentValue(docId);
    }

    public void setValues(SortDoc sortDoc) throws IOException {
      this.docId = sortDoc.docId;
      this.ord = sortDoc.ord;
      value1.setCurrentValue(((SingleValueSortDoc)sortDoc).value1);
    }

    public SingleValueSortDoc(SortValue value1) {
      super();
      this.value1 = value1;
    }

    public SortDoc copy() {
      return new SingleValueSortDoc(value1.copy());
    }

    public boolean lessThan(Object o) {
      SingleValueSortDoc sd = (SingleValueSortDoc)o;
      int comp = value1.compareTo(sd.value1);
      if(comp == -1) {
        return true;
      } else if (comp == 1) {
        return false;
      } else {
        return docId+docBase > sd.docId+sd.docBase;
      }
    }

    public int compareTo(Object o) {
      SingleValueSortDoc sd = (SingleValueSortDoc)o;
      return value1.compareTo(sd.value1);
    }

    public String toString() {
      return docId+":"+value1.toString();
    }
  }

  class DoubleValueSortDoc extends SingleValueSortDoc {

    protected SortValue value2;

    public void setNextReader(LeafReaderContext context) throws IOException {
      this.ord = context.ord;
      value1.setNextReader(context);
      value2.setNextReader(context);
    }

    public void reset() {
      this.docId = -1;
      value1.reset();
      value2.reset();
    }

    public void setValues(int docId) throws IOException {
      this.docId = docId;
      value1.setCurrentValue(docId);
      value2.setCurrentValue(docId);
    }

    public void setValues(SortDoc sortDoc) throws IOException {
      this.docId = sortDoc.docId;
      this.ord = sortDoc.ord;
      value1.setCurrentValue(((DoubleValueSortDoc)sortDoc).value1);
      value2.setCurrentValue(((DoubleValueSortDoc)sortDoc).value2);
    }

    public DoubleValueSortDoc(SortValue value1, SortValue value2) {
      super(value1);
      this.value2 = value2;
    }

    public SortDoc copy() {
      return new DoubleValueSortDoc(value1.copy(), value2.copy());
    }

    public boolean lessThan(Object o) {
      DoubleValueSortDoc sd = (DoubleValueSortDoc)o;
      int comp = value1.compareTo(sd.value1);
      if(comp == -1) {
        return true;
      } else if (comp == 1) {
        return false;
      } else {
        comp = value2.compareTo(sd.value2);
        if(comp == -1) {
          return true;
        } else if (comp == 1) {
          return false;
        } else {
          return docId+docBase > sd.docId+sd.docBase;
        }
      }
    }

    public int compareTo(Object o) {
      DoubleValueSortDoc sd = (DoubleValueSortDoc)o;
      int comp = value1.compareTo(sd.value1);
      if(comp == 0) {
        return value2.compareTo(sd.value2);
      } else {
        return comp;
      }
    }
  }

  class TripleValueSortDoc extends DoubleValueSortDoc {

    protected SortValue value3;

    public void setNextReader(LeafReaderContext context) throws IOException {
      this.ord = context.ord;
      value1.setNextReader(context);
      value2.setNextReader(context);
      value3.setNextReader(context);
    }

    public void reset() {
      this.docId = -1;
      value1.reset();
      value2.reset();
      value3.reset();
    }

    public void setValues(int docId) throws IOException {
      this.docId = docId;
      value1.setCurrentValue(docId);
      value2.setCurrentValue(docId);
      value3.setCurrentValue(docId);
    }

    public void setValues(SortDoc sortDoc) throws IOException {
      this.docId = sortDoc.docId;
      this.ord = sortDoc.ord;
      value1.setCurrentValue(((TripleValueSortDoc)sortDoc).value1);
      value2.setCurrentValue(((TripleValueSortDoc)sortDoc).value2);
      value3.setCurrentValue(((TripleValueSortDoc)sortDoc).value3);
    }

    public TripleValueSortDoc(SortValue value1, SortValue value2, SortValue value3) {
      super(value1, value2);
      this.value3 = value3;
    }

    public SortDoc copy() {
      return new TripleValueSortDoc(value1.copy(), value2.copy(), value3.copy());
    }

    public boolean lessThan(Object o) {

      TripleValueSortDoc sd = (TripleValueSortDoc)o;
      int comp = value1.compareTo(sd.value1);
      if(comp == -1) {
        return true;
      } else if (comp == 1) {
        return false;
      } else {
        comp = value2.compareTo(sd.value2);
        if(comp == -1) {
          return true;
        } else if (comp == 1) {
          return false;
        } else {
          comp = value3.compareTo(sd.value3);
          if(comp == -1) {
            return true;
          } else if (comp == 1) {
            return false;
          } else {
            return docId+docBase > sd.docId+sd.docBase;
          }
        }
      }
    }

    public int compareTo(Object o) {

      TripleValueSortDoc sd = (TripleValueSortDoc)o;
      int comp = value1.compareTo(sd.value1);
      if(comp == 0) {
        comp = value2.compareTo(sd.value2);
        if(comp == 0) {
          return value3.compareTo(sd.value3);
        } else {
          return comp;
        }
      } else {
        return comp;
      }
    }
  }

  class QuadValueSortDoc extends TripleValueSortDoc {

    protected SortValue value4;

    public void setNextReader(LeafReaderContext context) throws IOException {
      this.ord = context.ord;
      value1.setNextReader(context);
      value2.setNextReader(context);
      value3.setNextReader(context);
      value4.setNextReader(context);
    }

    public void reset() {
      this.docId = -1;
      value1.reset();
      value2.reset();
      value3.reset();
      value4.reset();
    }

    public void setValues(int docId) throws IOException {
      this.docId = docId;
      value1.setCurrentValue(docId);
      value2.setCurrentValue(docId);
      value3.setCurrentValue(docId);
      value4.setCurrentValue(docId);
    }

    public void setValues(SortDoc sortDoc) throws IOException {
      this.docId = sortDoc.docId;
      this.ord = sortDoc.ord;
      value1.setCurrentValue(((QuadValueSortDoc)sortDoc).value1);
      value2.setCurrentValue(((QuadValueSortDoc)sortDoc).value2);
      value3.setCurrentValue(((QuadValueSortDoc)sortDoc).value3);
      value4.setCurrentValue(((QuadValueSortDoc)sortDoc).value4);
    }

    public QuadValueSortDoc(SortValue value1, SortValue value2, SortValue value3, SortValue value4) {
      super(value1, value2, value3);
      this.value4 = value4;
    }

    public SortDoc copy() {
      return new QuadValueSortDoc(value1.copy(), value2.copy(), value3.copy(), value4.copy());
    }

    public boolean lessThan(Object o) {

      QuadValueSortDoc sd = (QuadValueSortDoc)o;
      int comp = value1.compareTo(sd.value1);
      if(comp == -1) {
        return true;
      } else if (comp == 1) {
        return false;
      } else {
        comp = value2.compareTo(sd.value2);
        if(comp == -1) {
          return true;
        } else if (comp == 1) {
          return false;
        } else {
          comp = value3.compareTo(sd.value3);
          if(comp == -1) {
            return true;
          } else if (comp == 1) {
            return false;
          } else {
            comp = value4.compareTo(sd.value4);
            if(comp == -1) {
              return true;
            } else if (comp == 1) {
              return false;
            } else {
              return docId+docBase > sd.docId+sd.docBase;
            }
          }
        }
      }
    }

    public int compareTo(Object o) {
      QuadValueSortDoc sd = (QuadValueSortDoc)o;
      int comp = value1.compareTo(sd.value1);
      if(comp == 0) {
        comp = value2.compareTo(sd.value2);
        if(comp == 0) {
          comp = value3.compareTo(sd.value3);
          if(comp == 0) {
            return value4.compareTo(sd.value4);
          } else {
            return comp;
          }
        } else {
          return comp;
        }
      } else {
        return comp;
      }
    }
  }

  public interface SortValue extends Comparable<SortValue> {
    public void setCurrentValue(int docId) throws IOException;
    public void setNextReader(LeafReaderContext context) throws IOException;
    public void setCurrentValue(SortValue value);
    public void reset();
    public SortValue copy();
  }

  class IntValue implements SortValue {

    protected NumericDocValues vals;
    protected String field;
    protected int currentValue;
    protected IntComp comp;

    public IntValue copy() {
      return new IntValue(field, comp);
    }

    public IntValue(String field, IntComp comp) {
      this.field = field;
      this.comp = comp;
      this.currentValue = comp.resetValue();
    }

    public void setNextReader(LeafReaderContext context) throws IOException {
      this.vals = DocValues.getNumeric(context.reader(), field);
    }

    public void setCurrentValue(int docId) {
      currentValue = (int)vals.get(docId);
    }

    public int compareTo(SortValue o) {
      IntValue iv = (IntValue)o;
      return comp.compare(currentValue, iv.currentValue);
    }

    public void setCurrentValue (SortValue value) {
      currentValue = ((IntValue)value).currentValue;
    }

    public void reset() {
      currentValue = comp.resetValue();
    }
  }

  public interface IntComp {
    public int compare(int a, int b);
    public int resetValue();
  }

  class IntDesc implements IntComp {

    public int resetValue() {
      return Integer.MIN_VALUE;
    }

    public int compare(int a, int b) {
      if(a < b) {
        return -1;
      } else if (a > b) {
        return 1;
      } else {
        return 0;
      }
    }
  }

  class IntAsc implements IntComp {

    public int resetValue() {
      return Integer.MAX_VALUE;
    }

    public int compare(int a, int b) {
      if(a < b) {
        return 1;
      } else if (a > b) {
        return -1;
      } else {
        return 0;
      }
    }
  }

  class LongValue implements SortValue {

    protected NumericDocValues vals;
    protected String field;
    protected long currentValue;
    protected LongComp comp;

    public LongValue(String field, LongComp comp) {
      this.field = field;
      this.comp = comp;
      this.currentValue = comp.resetValue();
    }

    public LongValue copy() {
      return new LongValue(field, comp);
    }

    public void setNextReader(LeafReaderContext context) throws IOException {
      this.vals = DocValues.getNumeric(context.reader(), field);
    }

    public void setCurrentValue(int docId) {
      currentValue = vals.get(docId);
    }

    public void setCurrentValue(SortValue sv) {
      LongValue lv = (LongValue)sv;
      this.currentValue = lv.currentValue;
    }

    public int compareTo(SortValue o) {
      LongValue l = (LongValue)o;
      return comp.compare(currentValue, l.currentValue);
    }

    public void reset() {
      this.currentValue = comp.resetValue();
    }
  }

  interface LongComp {
    public int compare(long a, long b);
    public long resetValue();
  }

  class LongDesc implements LongComp {

    public long resetValue() {
      return Long.MIN_VALUE;
    }

    public int compare(long a, long b) {
      if(a < b) {
        return -1;
      } else if (a > b) {
        return 1;
      } else {
        return 0;
      }
    }
  }

  class LongAsc implements LongComp {

    public long resetValue() {
      return Long.MAX_VALUE;
    }

    public int compare(long a, long b) {
      if(a < b) {
        return 1;
      } else if (a > b) {
        return -1;
      } else {
        return 0;
      }
    }
  }

  class FloatValue implements SortValue {

    protected NumericDocValues vals;
    protected String field;
    protected float currentValue;
    protected FloatComp comp;

    public FloatValue(String field, FloatComp comp) {
      this.field = field;
      this.comp = comp;
      this.currentValue = comp.resetValue();
    }

    public FloatValue copy() {
      return new FloatValue(field, comp);
    }

    public void setNextReader(LeafReaderContext context) throws IOException {
      this.vals = DocValues.getNumeric(context.reader(), field);
    }

    public void setCurrentValue(int docId) {
      currentValue = Float.intBitsToFloat((int)vals.get(docId));
    }

    public void setCurrentValue(SortValue sv) {
      FloatValue fv = (FloatValue)sv;
      this.currentValue = fv.currentValue;
    }

    public void reset() {
      this.currentValue = comp.resetValue();
    }

    public int compareTo(SortValue o) {
      FloatValue fv = (FloatValue)o;
      return comp.compare(currentValue, fv.currentValue);
    }
  }

  interface FloatComp {
    public int compare(float a, float b);
    public float resetValue();
  }

  public class FloatDesc implements FloatComp {
    public float resetValue() {
      return -Float.MAX_VALUE;
    }

    public int compare(float a, float b) {
      if(a < b) {
        return -1;
      } else if (a > b) {
        return 1;
      } else {
        return 0;
      }
    }
  }

  public class FloatAsc implements FloatComp {
    public float resetValue() {
      return Float.MAX_VALUE;
    }

    public int compare(float a, float b) {
      if(a < b) {
        return 1;
      } else if (a > b) {
        return -1;
      } else {
        return 0;
      }
    }
  }

  class DoubleValue implements SortValue {

    protected NumericDocValues vals;
    protected String field;
    protected double currentValue;
    protected DoubleComp comp;

    public DoubleValue(String field, DoubleComp comp) {
      this.field = field;
      this.comp = comp;
      this.currentValue = comp.resetValue();
    }

    public DoubleValue copy() {
      return new DoubleValue(field, comp);
    }

    public void setNextReader(LeafReaderContext context) throws IOException {
      this.vals = DocValues.getNumeric(context.reader(), field);
    }

    public void setCurrentValue(int docId) {
      currentValue = Double.longBitsToDouble(vals.get(docId));
    }

    public void setCurrentValue(SortValue sv) {
      DoubleValue dv = (DoubleValue)sv;
      this.currentValue = dv.currentValue;
    }

    public void reset() {
      this.currentValue = comp.resetValue();
    }

    public int compareTo(SortValue o) {
      DoubleValue dv = (DoubleValue)o;
      return comp.compare(currentValue, dv.currentValue);
    }
  }

  interface DoubleComp {
    public int compare(double a, double b);
    public double resetValue();
  }

  public class DoubleDesc implements DoubleComp {
    public double resetValue() {
      return -Double.MAX_VALUE;
    }

    public int compare(double a, double b) {
      if(a < b) {
        return -1;
      } else if (a > b) {
        return 1;
      } else {
        return 0;
      }
    }
  }

  public class DoubleAsc implements DoubleComp {
    public double resetValue() {
      return Double.MAX_VALUE;
    }

    public int compare(double a, double b) {
      if(a < b) {
        return 1;
      } else if (a > b) {
        return -1;
      } else {
        return 0;
      }
    }
  }

  class StringValue implements SortValue {

    protected SortedDocValues vals;
    protected SortedDocValues segmentVals[];

    protected MultiDocValues.OrdinalMap ordinalMap;
    protected LongValues globalOrds;
    protected SortedDocValues currentVals;

    protected String field;
    protected int segment;
    protected int currentOrd;
    protected IntComp comp;

    public StringValue(SortedDocValues vals, String field, IntComp comp)  {
      this.vals = vals;
      if(vals instanceof  MultiDocValues.MultiSortedDocValues) {
        this.segmentVals = ((MultiDocValues.MultiSortedDocValues) vals).values;
        this.ordinalMap = ((MultiDocValues.MultiSortedDocValues) vals).mapping;
      }
      this.field = field;
      this.comp = comp;
      this.currentOrd = comp.resetValue();
    }

    public StringValue copy() {
      return new StringValue(vals, field, comp);
    }

    public void setCurrentValue(int docId) {
      int ord = currentVals.getOrd(docId);

      if(ord < 0) {
        currentOrd = -1;
      } else {
        if(globalOrds != null) {
          currentOrd = (int)globalOrds.get(ord);
        } else {
          currentOrd = ord;
        }
      }
    }

    public void setCurrentValue(SortValue sv) {
      StringValue v = (StringValue)sv;
      this.currentOrd = v.currentOrd;
    }

    public void setNextReader(LeafReaderContext context) {
      segment = context.ord;
      if(ordinalMap != null) {
        globalOrds = ordinalMap.getGlobalOrds(segment);
        currentVals = segmentVals[segment];
      } else {
        currentVals = vals;
      }
    }

    public void reset() {
      this.currentOrd = comp.resetValue();
    }

    public int compareTo(SortValue o) {
      StringValue sv = (StringValue)o;
      return comp.compare(currentOrd, sv.currentOrd);
    }

    public String toString() {
      return Integer.toString(this.currentOrd);
    }
  }

  protected abstract class FieldWriter {
    public abstract boolean write(int docId, LeafReader reader, Writer out, int fieldIndex) throws IOException;
  }

  class IntFieldWriter extends FieldWriter {
    private String field;

    public IntFieldWriter(String field) {
      this.field = field;
    }

    public boolean write(int docId, LeafReader reader, Writer out, int fieldIndex) throws IOException {
      NumericDocValues vals = DocValues.getNumeric(reader, this.field);
      int val = (int)vals.get(docId);
      if(fieldIndex>0) {
        out.write(',');
      }
      out.write('"');
      out.write(this.field);
      out.write('"');
      out.write(':');
      out.write(Integer.toString(val));
      return true;
    }
  }

  class MultiFieldWriter extends FieldWriter {
    private String field;
    private FieldType fieldType;
    private boolean numeric;
    private CharsRefBuilder cref = new CharsRefBuilder();

    public MultiFieldWriter(String field, FieldType fieldType, boolean numeric) {
      this.field = field;
      this.fieldType = fieldType;
      this.numeric = numeric;
    }
    public boolean write(int docId, LeafReader reader, Writer out, int fieldIndex) throws IOException {
      SortedSetDocValues vals = DocValues.getSortedSet(reader, this.field);
      vals.setDocument(docId);
      List<Long> ords = new ArrayList();
      long o = -1;
      while((o = vals.nextOrd()) != SortedSetDocValues.NO_MORE_ORDS) {
        ords.add(o);
      }

      if(ords.size()== 0) {
        return false;
      }


      if(fieldIndex>0) {
        out.write(',');
      }
      out.write('"');
      out.write(this.field);
      out.write('"');
      out.write(':');
      out.write('[');
      int v = 0;
      for(long ord : ords) {
        BytesRef ref = vals.lookupOrd(ord);
        fieldType.indexedToReadable(ref, cref);
        if(v > 0) {
          out.write(',');
        }

        if(!numeric) {
          out.write('"');
        }

        writeStr(cref.toString(), out);

        if(!numeric) {
          out.write('"');
        }
        ++v;
      }
      out.write("]");
      return true;
    }
  }

  class LongFieldWriter extends FieldWriter {
    private String field;

    public LongFieldWriter(String field) {
      this.field = field;
    }

    public boolean write(int docId, LeafReader reader, Writer out, int fieldIndex) throws IOException {
      NumericDocValues vals = DocValues.getNumeric(reader, this.field);
      long val = vals.get(docId);
      if(fieldIndex > 0) {
        out.write(',');
      }
      out.write('"');
      out.write(this.field);
      out.write('"');
      out.write(':');
      out.write(Long.toString(val));
      return true;
    }
  }

  class FloatFieldWriter extends FieldWriter {
    private String field;

    public FloatFieldWriter(String field) {
      this.field = field;
    }

    public boolean write(int docId, LeafReader reader, Writer out, int fieldIndex) throws IOException {
      NumericDocValues vals = DocValues.getNumeric(reader, this.field);
      int val = (int)vals.get(docId);
      if(fieldIndex > 0) {
        out.write(',');
      }
      out.write('"');
      out.write(this.field);
      out.write('"');
      out.write(':');
      out.write(Float.toString(Float.intBitsToFloat(val)));
      return true;
    }
  }

  class DoubleFieldWriter extends FieldWriter {
    private String field;

    public DoubleFieldWriter(String field) {
      this.field = field;
    }

    public boolean write(int docId, LeafReader reader, Writer out, int fieldIndex) throws IOException {
      NumericDocValues vals = DocValues.getNumeric(reader, this.field);
      if(fieldIndex > 0) {
        out.write(',');
      }
      long val = vals.get(docId);
      out.write('"');
      out.write(this.field);
      out.write('"');
      out.write(':');
      out.write(Double.toString(Double.longBitsToDouble(val)));
      return true;
    }
  }

  class StringFieldWriter extends FieldWriter {
    private String field;
    private FieldType fieldType;
    private CharsRefBuilder cref = new CharsRefBuilder();

    public StringFieldWriter(String field, FieldType fieldType) {
      this.field = field;
      this.fieldType = fieldType;
    }

    public boolean write(int docId, LeafReader reader, Writer out, int fieldIndex) throws IOException {
      SortedDocValues vals = DocValues.getSorted(reader, this.field);
      int ord = vals.getOrd(docId);
      if(ord == -1) {
        return false;
      }

      BytesRef ref = vals.lookupOrd(ord);
      fieldType.indexedToReadable(ref, cref);
      if(fieldIndex > 0) {
        out.write(',');
      }
      out.write('"');
      out.write(this.field);
      out.write('"');
      out.write(":");
      out.write('"');
      writeStr(cref.toString(), out);
      out.write('"');
      return true;
    }
  }

  private void writeStr(String val, Writer writer) throws IOException {
    for (int i=0; i<val.length(); i++) {
      char ch = val.charAt(i);
      if ((ch > '#' && ch != '\\' && ch < '\u2028') || ch == ' ') { // fast path
        writer.write(ch);
        continue;
      }
      switch(ch) {
        case '"':
        case '\\':
          writer.write('\\');
          writer.write(ch);
          break;
        case '\r': writer.write('\\'); writer.write('r'); break;
        case '\n': writer.write('\\'); writer.write('n'); break;
        case '\t': writer.write('\\'); writer.write('t'); break;
        case '\b': writer.write('\\'); writer.write('b'); break;
        case '\f': writer.write('\\'); writer.write('f'); break;
        case '\u2028': // fallthrough
        case '\u2029':
          unicodeEscape(writer,ch);
          break;
        // case '/':
        default: {
          if (ch <= 0x1F) {
            unicodeEscape(writer,ch);
          } else {
            writer.write(ch);
          }
        }
      }
    }
  }

  private static char[] hexdigits = {'0','1','2','3','4','5','6','7','8','9','a','b','c','d','e','f'};
  protected static void unicodeEscape(Appendable out, int ch) throws IOException {
    out.append('\\');
    out.append('u');
    out.append(hexdigits[(ch>>>12)     ]);
    out.append(hexdigits[(ch>>>8) & 0xf]);
    out.append(hexdigits[(ch>>>4) & 0xf]);
    out.append(hexdigits[(ch)     & 0xf]);
  }

  public abstract class PriorityQueue<T> {
    protected int size = 0;
    protected final int maxSize;
    private final T[] heap;

    public PriorityQueue(int maxSize) {
      this(maxSize, true);
    }

    public PriorityQueue(int maxSize, boolean prepopulate) {
      final int heapSize;
      if (0 == maxSize) {
        // We allocate 1 extra to avoid if statement in top()
        heapSize = 2;
      } else {
        if (maxSize > ArrayUtil.MAX_ARRAY_LENGTH) {
          // Don't wrap heapSize to -1, in this case, which
          // causes a confusing NegativeArraySizeException.
          // Note that very likely this will simply then hit
          // an OOME, but at least that's more indicative to
          // caller that this values is too big.  We don't +1
          // in this case, but it's very unlikely in practice
          // one will actually insert this many objects into
          // the PQ:
          // Throw exception to prevent confusing OOME:
          throw new IllegalArgumentException("maxSize must be <= " + ArrayUtil.MAX_ARRAY_LENGTH + "; got: " + maxSize);
        } else {
          // NOTE: we add +1 because all access to heap is
          // 1-based not 0-based.  heap[0] is unused.
          heapSize = maxSize + 1;
        }
      }
      // T is unbounded type, so this unchecked cast works always:
      @SuppressWarnings("unchecked") final T[] h = (T[]) new Object[heapSize];
      this.heap = h;
      this.maxSize = maxSize;

      if (prepopulate) {
        // If sentinel objects are supported, populate the queue with them
        T sentinel = getSentinelObject();
        if (sentinel != null) {
          heap[1] = sentinel;
          for (int i = 2; i < heap.length; i++) {
            heap[i] = getSentinelObject();
          }
          size = maxSize;
        }
      }
    }

    /** Determines the ordering of objects in this priority queue.  Subclasses
     *  must define this one method.
     *  @return <code>true</code> iff parameter <tt>a</tt> is less than parameter <tt>b</tt>.
     */
    protected abstract boolean lessThan(T a, T b);


    protected T getSentinelObject() {
      return null;
    }

    /**
     * Adds an Object to a PriorityQueue in log(size) time. If one tries to add
     * more objects than maxSize from initialize an
     *
     * @return the new 'top' element in the queue.
     */
    public final T add(T element) {
      size++;
      heap[size] = element;
      upHeap();
      return heap[1];
    }

    /**
     * Adds an Object to a PriorityQueue in log(size) time.
     * It returns the object (if any) that was
     * dropped off the heap because it was full. This can be
     * the given parameter (in case it is smaller than the
     * full heap's minimum, and couldn't be added), or another
     * object that was previously the smallest value in the
     * heap and now has been replaced by a larger one, or null
     * if the queue wasn't yet full with maxSize elements.
     */
    public T insertWithOverflow(T element) {
      if (size < maxSize) {
        add(element);
        return null;
      } else if (size > 0 && !lessThan(element, heap[1])) {
        T ret = heap[1];
        heap[1] = element;
        updateTop();
        return ret;
      } else {
        return element;
      }
    }

    /** Returns the least element of the PriorityQueue in constant time. */
    public final T top() {
      // We don't need to check size here: if maxSize is 0,
      // then heap is length 2 array with both entries null.
      // If size is 0 then heap[1] is already null.
      return heap[1];
    }

    /** Removes and returns the least element of the PriorityQueue in log(size)
     time. */
    public final T pop() {
      if (size > 0) {
        T result = heap[1];       // save first value
        heap[1] = heap[size];     // move last to first
        heap[size] = null;        // permit GC of objects
        size--;
        downHeap();               // adjust heap
        return result;
      } else {
        return null;
      }
    }

    /**
     * Should be called when the Object at top changes values. Still log(n) worst
     * case, but it's at least twice as fast to
     *
     * <pre class="prettyprint">
     * pq.top().change();
     * pq.updateTop();
     * </pre>
     *
     * instead of
     *
     * <pre class="prettyprint">
     * o = pq.pop();
     * o.change();
     * pq.push(o);
     * </pre>
     *
     * @return the new 'top' element.
     */
    public final T updateTop() {
      downHeap();
      return heap[1];
    }

    /** Returns the number of elements currently stored in the PriorityQueue. */
    public final int size() {
      return size;
    }

    /** Removes all entries from the PriorityQueue. */
    public final void clear() {
      for (int i = 0; i <= size; i++) {
        heap[i] = null;
      }
      size = 0;
    }

    private final void upHeap() {
      int i = size;
      T node = heap[i];          // save bottom node
      int j = i >>> 1;
      while (j > 0 && lessThan(node, heap[j])) {
        heap[i] = heap[j];       // shift parents down
        i = j;
        j = j >>> 1;
      }
      heap[i] = node;            // install saved node
    }

    private final void downHeap() {
      int i = 1;
      T node = heap[i];          // save top node
      int j = i << 1;            // find smaller child
      int k = j + 1;
      if (k <= size && lessThan(heap[k], heap[j])) {
        j = k;
      }
      while (j <= size && lessThan(heap[j], node)) {
        heap[i] = heap[j];       // shift up child
        i = j;
        j = i << 1;
        k = j + 1;
        if (k <= size && lessThan(heap[k], heap[j])) {
          j = k;
        }
      }
      heap[i] = node;            // install saved node
    }

    /** This method returns the internal heap array as Object[].
     * @lucene.internal
     */
    public final Object[] getHeapArray() {
      return (Object[]) heap;
    }
  }
}