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
import java.io.Writer;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.common.EnumFieldValue;
import org.apache.solr.common.IteratorWriter;
import org.apache.solr.common.MapSerializable;
import org.apache.solr.common.PushWriter;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.MapWriter;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.util.Base64;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.DocList;
import org.apache.solr.search.ReturnFields;
import org.apache.solr.util.FastWriter;

/** Base class for text-oriented response writers.
 *
 *
 */
public abstract class TextResponseWriter implements PushWriter {

  // indent up to 40 spaces
  static final char[] indentChars = new char[81];
  static {
    Arrays.fill(indentChars,' ');
    indentChars[0] = '\n';  // start with a newline
  }

  
  protected final FastWriter writer;
  protected final IndexSchema schema;
  protected final SolrQueryRequest req;
  protected final SolrQueryResponse rsp;

  // the default set of fields to return for each document
  protected ReturnFields returnFields;

  protected int level;
  protected boolean doIndent;

  protected Calendar cal;  // reusable calendar instance


  public TextResponseWriter(Writer writer, SolrQueryRequest req, SolrQueryResponse rsp) {
    this.writer = writer == null ? null: FastWriter.wrap(writer);
    this.schema = req.getSchema();
    this.req = req;
    this.rsp = rsp;
    String indent = req.getParams().get("indent");
    if (indent != null && !"".equals(indent) && !"off".equals(indent)) {
      doIndent=true;
    }
    returnFields = rsp.getReturnFields();
    if (req.getParams().getBool(CommonParams.OMIT_HEADER, false)) rsp.removeResponseHeader();
  }

  /** done with this ResponseWriter... make sure any buffers are flushed to writer */
  public void close() throws IOException {
    if(writer != null) writer.flushBuffer();
  }

  /** returns the Writer that the response is being written to */
  public Writer getWriter() { return writer; }


  public void indent() throws IOException {
     if (doIndent) indent(level);
  }

  public void indent(int lev) throws IOException {
    writer.write(indentChars, 0, Math.min((lev<<1)+1, indentChars.length));
  }

  //
  // Functions to manipulate the current logical nesting level.
  // Any indentation will be partially based on level.
  //
  public void setLevel(int level) { this.level = level; }
  public int level() { return level; }
  public int incLevel() { return ++level; }
  public int decLevel() { return --level; }
  public void setIndent(boolean doIndent) {
    this.doIndent = doIndent;
  }


  public abstract void writeNamedList(String name, NamedList val) throws IOException;

  public final void writeVal(String name, Object val) throws IOException {

    // if there get to be enough types, perhaps hashing on the type
    // to get a handler might be faster (but types must be exact to do that...)

    // go in order of most common to least common
    if (val == null) {
      writeNull(name);
    } else if (val instanceof String) {
      writeStr(name, val.toString(), true);
      // micro-optimization... using toString() avoids a cast first
    } else if (val instanceof IndexableField) {
      IndexableField f = (IndexableField)val;
      SchemaField sf = schema.getFieldOrNull( f.name() );
      if( sf != null ) {
        sf.getType().write(this, name, f);
      }
      else {
        writeStr(name, f.stringValue(), true);
      }
    } else if (val instanceof Number) {
      writeNumber(name, (Number) val);
    } else if (val instanceof Boolean) {
      writeBool(name, (Boolean) val);
    } else if (val instanceof Date) {
      writeDate(name, (Date) val);
    } else if (val instanceof Document) {
      SolrDocument doc = DocsStreamer.getDoc((Document) val, schema);
      writeSolrDocument(name, doc, returnFields, 0);
    } else if (val instanceof SolrDocument) {
      writeSolrDocument(name, (SolrDocument) val, returnFields, 0);
    } else if (val instanceof ResultContext) {
      // requires access to IndexReader
      writeDocuments(name, (ResultContext) val);
    } else if (val instanceof DocList) {
      // Should not happen normally
      ResultContext ctx = new BasicResultContext((DocList)val, returnFields, null, null, req);
      writeDocuments(name, ctx);
    // }
    // else if (val instanceof DocSet) {
    // how do we know what fields to read?
    // todo: have a DocList/DocSet wrapper that
    // restricts the fields to write...?
    } else if (val instanceof SolrDocumentList) {
      writeSolrDocumentList(name, (SolrDocumentList)val, returnFields);
    } else if (val instanceof Map) {
      writeMap(name, (Map)val, false, true);
    } else if (val instanceof NamedList) {
      writeNamedList(name, (NamedList)val);
    } else if (val instanceof Path) {
      writeStr(name, ((Path) val).toAbsolutePath().toString(), true);
    } else if (val instanceof IteratorWriter) {
      writeIterator((IteratorWriter) val);
    } else if (val instanceof Iterable) {
      writeArray(name,((Iterable)val).iterator());
    } else if (val instanceof Object[]) {
      writeArray(name,(Object[])val);
    } else if (val instanceof Iterator) {
      writeArray(name, (Iterator) val);
    } else if (val instanceof byte[]) {
      byte[] arr = (byte[])val;
      writeByteArr(name, arr, 0, arr.length);
    } else if (val instanceof BytesRef) {
      BytesRef arr = (BytesRef)val;
      writeByteArr(name, arr.bytes, arr.offset, arr.length);
    } else if (val instanceof EnumFieldValue) {
      writeStr(name, val.toString(), true);
    } else if (val instanceof WriteableValue) {
      ((WriteableValue)val).write(name, this);
    } else if (val instanceof MapWriter) {
      writeMap((MapWriter) val);
    } else if (val instanceof MapSerializable) {
      //todo find a better way to reuse the map more efficiently
      writeMap(name, ((MapSerializable) val).toMap(new LinkedHashMap<>()), false, true);
    } else {
      // default... for debugging only
      writeStr(name, val.getClass().getName() + ':' + val.toString(), true);
    }
  }
  @Override
  public void writeMap(MapWriter mw) throws IOException {
    //todo
  }

  @Override
  public void writeIterator(IteratorWriter iw) throws IOException {
    /*todo*/
  }

  protected void writeBool(String name , Boolean val) throws IOException {
    writeBool(name, val.toString());
  }

  protected void writeNumber(String name, Number val) throws IOException {
    if (val instanceof Integer) {
      writeInt(name, val.toString());
    } else if (val instanceof Long) {
      writeLong(name, val.toString());
    } else if (val instanceof Float) {
      // we pass the float instead of using toString() because
      // it may need special formatting. same for double.
      writeFloat(name, ((Float)val).floatValue());
    } else if (val instanceof Double) {
      writeDouble(name, ((Double) val).doubleValue());
    } else if (val instanceof Short) {
      writeInt(name, val.toString());
    } else if (val instanceof Byte) {
      writeInt(name, val.toString());
    } else {
      // default... for debugging only
      writeStr(name, val.getClass().getName() + ':' + val.toString(), true);
    }
  }

  // names are passed when writing primitives like writeInt to allow many different
  // types of formats, including those where the name may come after the value (like
  // some XML formats).

  public abstract void writeStartDocumentList(String name, long start, int size, long numFound, Float maxScore) throws IOException;  

  public abstract void writeSolrDocument(String name, SolrDocument doc, ReturnFields fields, int idx) throws IOException;
  
  public abstract void writeEndDocumentList() throws IOException;
  
  // Assume each SolrDocument is already transformed
  public final void writeSolrDocumentList(String name, SolrDocumentList docs, ReturnFields fields) throws IOException
  {
    writeStartDocumentList(name, docs.getStart(), docs.size(), docs.getNumFound(), docs.getMaxScore() );
    for( int i=0; i<docs.size(); i++ ) {
      writeSolrDocument( null, docs.get(i), fields, i );
    }
    writeEndDocumentList();
  }


  public final void writeDocuments(String name, ResultContext res) throws IOException {
    DocList ids = res.getDocList();
    Iterator<SolrDocument> docsStreamer = res.getProcessedDocuments();
    writeStartDocumentList(name, ids.offset(), ids.size(), ids.matches(),
        res.wantsScores() ? new Float(ids.maxScore()) : null);

    int idx = 0;
    while (docsStreamer.hasNext()) {
      writeSolrDocument(null, docsStreamer.next(), res.getReturnFields(), idx);
      idx++;
    }
    writeEndDocumentList();
  }
  
  
  public abstract void writeStr(String name, String val, boolean needsEscaping) throws IOException;

  public abstract void writeMap(String name, Map val, boolean excludeOuter, boolean isFirstVal) throws IOException;

  public void writeArray(String name, Object[] val) throws IOException {
    writeArray(name, Arrays.asList(val));
  }

  public void writeArray(String name, List l) throws IOException {
    writeArray(name, l.iterator());
  }
  
  public abstract void writeArray(String name, Iterator val) throws IOException;

  public abstract void writeNull(String name) throws IOException;

  /** if this form of the method is called, val is the Java string form of an int */
  public abstract void writeInt(String name, String val) throws IOException;

  public void writeInt(String name, int val) throws IOException {
    writeInt(name,Integer.toString(val));
  }

  /** if this form of the method is called, val is the Java string form of a long */
  public abstract void writeLong(String name, String val) throws IOException;

  public  void writeLong(String name, long val) throws IOException {
    writeLong(name,Long.toString(val));
  }

  /** if this form of the method is called, val is the Java string form of a boolean */
  public abstract void writeBool(String name, String val) throws IOException;

  public void writeBool(String name, boolean val) throws IOException {
    writeBool(name,Boolean.toString(val));
  }

  /** if this form of the method is called, val is the Java string form of a float */
  public abstract void writeFloat(String name, String val) throws IOException;

  public void writeFloat(String name, float val) throws IOException {
    String s = Float.toString(val);
    // If it's not a normal number, write the value as a string instead.
    // The following test also handles NaN since comparisons are always false.
    if (val > Float.NEGATIVE_INFINITY && val < Float.POSITIVE_INFINITY) {
      writeFloat(name,s);
    } else {
      writeStr(name,s,false);
    }
  }


  /** if this form of the method is called, val is the Java string form of a double */
  public abstract void writeDouble(String name, String val) throws IOException;

  public void writeDouble(String name, double val) throws IOException {
    String s = Double.toString(val);
    // If it's not a normal number, write the value as a string instead.
    // The following test also handles NaN since comparisons are always false.
    if (val > Double.NEGATIVE_INFINITY && val < Double.POSITIVE_INFINITY) {
      writeDouble(name,s);
    } else {
      writeStr(name,s,false);
    }
  }


  public void writeDate(String name, Date val) throws IOException {
    writeDate(name, val.toInstant().toString());
  }
  

  /** if this form of the method is called, val is the Solr ISO8601 based date format */
  public abstract void writeDate(String name, String val) throws IOException;

  public void writeByteArr(String name, byte[] buf, int offset, int len) throws IOException {
    writeStr(name, Base64.byteArrayToBase64(buf, offset, len), false);
  }
}
