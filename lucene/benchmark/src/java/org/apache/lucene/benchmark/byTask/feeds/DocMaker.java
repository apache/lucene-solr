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
package org.apache.lucene.benchmark.byTask.feeds;


import java.io.Closeable;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.lucene.benchmark.byTask.utils.Config;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FloatPoint;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexOptions;

/**
 * Creates {@link Document} objects. Uses a {@link ContentSource} to generate
 * {@link DocData} objects. Supports the following parameters:
 * <ul>
 * <li><b>content.source</b> - specifies the {@link ContentSource} class to use
 * (default <b>SingleDocSource</b>).
 * <li><b>doc.stored</b> - specifies whether fields should be stored (default
 * <b>false</b>).
 * <li><b>doc.body.stored</b> - specifies whether the body field should be stored (default
 * = <b>doc.stored</b>).
 * <li><b>doc.tokenized</b> - specifies whether fields should be tokenized
 * (default <b>true</b>).
 * <li><b>doc.body.tokenized</b> - specifies whether the
 * body field should be tokenized (default = <b>doc.tokenized</b>).
 * <li><b>doc.body.offsets</b> - specifies whether to add offsets into the postings index
 *  for the body field.  It is useful for highlighting.  (default <b>false</b>)
 * <li><b>doc.tokenized.norms</b> - specifies whether norms should be stored in
 * the index or not. (default <b>false</b>).
 * <li><b>doc.body.tokenized.norms</b> - specifies whether norms should be
 * stored in the index for the body field. This can be set to true, while
 * <code>doc.tokenized.norms</code> is set to false, to allow norms storing just
 * for the body field. (default <b>true</b>).
 * <li><b>doc.term.vector</b> - specifies whether term vectors should be stored
 * for fields (default <b>false</b>).
 * <li><b>doc.term.vector.positions</b> - specifies whether term vectors should
 * be stored with positions (default <b>false</b>).
 * <li><b>doc.term.vector.offsets</b> - specifies whether term vectors should be
 * stored with offsets (default <b>false</b>).
 * <li><b>doc.store.body.bytes</b> - specifies whether to store the raw bytes of
 * the document's content in the document (default <b>false</b>).
 * <li><b>doc.reuse.fields</b> - specifies whether Field and Document objects
 * should be reused (default <b>true</b>).
 * <li><b>doc.index.props</b> - specifies whether the properties returned by
 * <li><b>doc.random.id.limit</b> - if specified, docs will be assigned random
 * IDs from 0 to this limit.  This is useful with UpdateDoc
 * for testing performance of IndexWriter.updateDocument.
 * {@link DocData#getProps()} will be indexed. (default <b>false</b>).
 * </ul>
 */
public class DocMaker implements Closeable {

  private static class LeftOver {
    private DocData docdata;
    private int cnt;
  }

  private Random r;
  private int updateDocIDLimit;

  /**
   * Document state, supports reuse of field instances
   * across documents (see <code>reuseFields</code> parameter).
   */
  protected static class DocState {
    
    private final Map<String,Field> fields;
    private final Map<String,Field> numericFields;
    private final boolean reuseFields;
    final Document doc;
    DocData docData = new DocData();
    
    public DocState(boolean reuseFields, FieldType ft, FieldType bodyFt) {

      this.reuseFields = reuseFields;
      
      if (reuseFields) {
        fields =  new HashMap<>();
        numericFields = new HashMap<>();
        
        // Initialize the map with the default fields.
        fields.put(BODY_FIELD, new Field(BODY_FIELD, "", bodyFt));
        fields.put(TITLE_FIELD, new Field(TITLE_FIELD, "", ft));
        fields.put(DATE_FIELD, new Field(DATE_FIELD, "", ft));
        fields.put(ID_FIELD, new StringField(ID_FIELD, "", Field.Store.YES));
        fields.put(NAME_FIELD, new Field(NAME_FIELD, "", ft));

        numericFields.put(DATE_MSEC_FIELD, new LongPoint(DATE_MSEC_FIELD, 0L));
        numericFields.put(TIME_SEC_FIELD, new IntPoint(TIME_SEC_FIELD, 0));
        
        doc = new Document();
      } else {
        numericFields = null;
        fields = null;
        doc = null;
      }
    }

    /**
     * Returns a field corresponding to the field name. If
     * <code>reuseFields</code> was set to true, then it attempts to reuse a
     * Field instance. If such a field does not exist, it creates a new one.
     */
    Field getField(String name, FieldType ft) {
      if (!reuseFields) {
        return new Field(name, "", ft);
      }
      
      Field f = fields.get(name);
      if (f == null) {
        f = new Field(name, "", ft);
        fields.put(name, f);
      }
      return f;
    }

    Field getNumericField(String name, Class<? extends Number> numericType) {
      Field f;
      if (reuseFields) {
        f = numericFields.get(name);
      } else {
        f = null;
      }
      
      if (f == null) {
        if (numericType.equals(Integer.class)) {
          f = new IntPoint(name, 0);
        } else if (numericType.equals(Long.class)) {
          f = new LongPoint(name, 0L);
        } else if (numericType.equals(Float.class)) {
          f = new FloatPoint(name, 0.0F);
        } else if (numericType.equals(Double.class)) {
          f = new DoublePoint(name, 0.0);
        } else {
          throw new UnsupportedOperationException("Unsupported numeric type: " + numericType);
        }
        if (reuseFields) {
          numericFields.put(name, f);
        }
      }
      return f;
    }
  }
  
  private boolean storeBytes = false;

  private static class DateUtil {
    public SimpleDateFormat parser = new SimpleDateFormat("dd-MMM-yyyy HH:mm:ss", Locale.ENGLISH);
    public Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("GMT"), Locale.ROOT);
    public ParsePosition pos = new ParsePosition(0);
    public DateUtil() {
      parser.setLenient(true);
    }
  }

  // leftovers are thread local, because it is unsafe to share residues between threads
  private ThreadLocal<LeftOver> leftovr = new ThreadLocal<>();
  private ThreadLocal<DocState> docState = new ThreadLocal<>();
  private ThreadLocal<DateUtil> dateParsers = new ThreadLocal<>();

  public static final String BODY_FIELD = "body";
  public static final String TITLE_FIELD = "doctitle";
  public static final String DATE_FIELD = "docdate";
  public static final String DATE_MSEC_FIELD = "docdatenum";
  public static final String TIME_SEC_FIELD = "doctimesecnum";
  public static final String ID_FIELD = "docid";
  public static final String BYTES_FIELD = "bytes";
  public static final String NAME_FIELD = "docname";

  protected Config config;

  protected FieldType valType;
  protected FieldType bodyValType;
    
  protected ContentSource source;
  protected boolean reuseFields;
  protected boolean indexProperties;
  
  private final AtomicInteger numDocsCreated = new AtomicInteger();

  public DocMaker() {
  }
  
  // create a doc
  // use only part of the body, modify it to keep the rest (or use all if size==0).
  // reset the docdata properties so they are not added more than once.
  private Document createDocument(DocData docData, int size, int cnt) throws UnsupportedEncodingException {

    final DocState ds = getDocState();
    final Document doc = reuseFields ? ds.doc : new Document();
    doc.clear();
    
    // Set ID_FIELD
    FieldType ft = new FieldType(valType);
    ft.setStored(true);

    Field idField = ds.getField(ID_FIELD, ft);
    int id;
    if (r != null) {
      id = r.nextInt(updateDocIDLimit);
    } else {
      id = docData.getID();
      if (id == -1) {
        id = numDocsCreated.getAndIncrement();
      }
    }
    idField.setStringValue(Integer.toString(id));
    doc.add(idField);
    
    // Set NAME_FIELD
    String name = docData.getName();
    if (name == null) name = "";
    name = cnt < 0 ? name : name + "_" + cnt;
    Field nameField = ds.getField(NAME_FIELD, valType);
    nameField.setStringValue(name);
    doc.add(nameField);
    
    // Set DATE_FIELD
    DateUtil util = dateParsers.get();
    if (util == null) {
      util = new DateUtil();
      dateParsers.set(util);
    }
    Date date = null;
    String dateString = docData.getDate();
    if (dateString != null) {
      util.pos.setIndex(0);
      date = util.parser.parse(dateString, util.pos);
      //System.out.println(dateString + " parsed to " + date);
    } else {
      dateString = "";
    }
    Field dateStringField = ds.getField(DATE_FIELD, valType);
    dateStringField.setStringValue(dateString);
    doc.add(dateStringField);

    if (date == null) {
      // just set to right now
      date = new Date();
    }

    Field dateField = ds.getNumericField(DATE_MSEC_FIELD, Long.class);
    dateField.setLongValue(date.getTime());
    doc.add(dateField);

    util.cal.setTime(date);
    final int sec = util.cal.get(Calendar.HOUR_OF_DAY)*3600 + util.cal.get(Calendar.MINUTE)*60 + util.cal.get(Calendar.SECOND);

    Field timeSecField = ds.getNumericField(TIME_SEC_FIELD, Integer.class);
    timeSecField.setIntValue(sec);
    doc.add(timeSecField);
    
    // Set TITLE_FIELD
    String title = docData.getTitle();
    Field titleField = ds.getField(TITLE_FIELD, valType);
    titleField.setStringValue(title == null ? "" : title);
    doc.add(titleField);
    
    String body = docData.getBody();
    if (body != null && body.length() > 0) {
      String bdy;
      if (size <= 0 || size >= body.length()) {
        bdy = body; // use all
        docData.setBody(""); // nothing left
      } else {
        // attempt not to break words - if whitespace found within next 20 chars...
        for (int n = size - 1; n < size + 20 && n < body.length(); n++) {
          if (Character.isWhitespace(body.charAt(n))) {
            size = n;
            break;
          }
        }
        bdy = body.substring(0, size); // use part
        docData.setBody(body.substring(size)); // some left
      }
      Field bodyField = ds.getField(BODY_FIELD, bodyValType);
      bodyField.setStringValue(bdy);
      doc.add(bodyField);
      
      if (storeBytes) {
        Field bytesField = ds.getField(BYTES_FIELD, StringField.TYPE_STORED);
        bytesField.setBytesValue(bdy.getBytes(StandardCharsets.UTF_8));
        doc.add(bytesField);
      }
    }

    if (indexProperties) {
      Properties props = docData.getProps();
      if (props != null) {
        for (final Map.Entry<Object,Object> entry : props.entrySet()) {
          Field f = ds.getField((String) entry.getKey(), valType);
          f.setStringValue((String) entry.getValue());
          doc.add(f);
        }
        docData.setProps(null);
      }
    }
    
    //System.out.println("============== Created doc "+numDocsCreated+" :\n"+doc+"\n==========");
    return doc;
  }

  private void resetLeftovers() {
    leftovr.set(null);
  }

  protected DocState getDocState() {
    DocState ds = docState.get();
    if (ds == null) {
      ds = new DocState(reuseFields, valType, bodyValType);
      docState.set(ds);
    }
    return ds;
  }

  /**
   * Closes the {@link DocMaker}. The base implementation closes the
   * {@link ContentSource}, and it can be overridden to do more work (but make
   * sure to call super.close()).
   */
  @Override
  public void close() throws IOException {
    source.close();
  }
  
  /**
   * Creates a {@link Document} object ready for indexing. This method uses the
   * {@link ContentSource} to get the next document from the source, and creates
   * a {@link Document} object from the returned fields. If
   * <code>reuseFields</code> was set to true, it will reuse {@link Document}
   * and {@link Field} instances.
   */
  public Document makeDocument() throws Exception {
    resetLeftovers();
    DocData docData = source.getNextDocData(getDocState().docData);
    Document doc = createDocument(docData, 0, -1);
    return doc;
  }

  /**
   * Same as {@link #makeDocument()}, only this method creates a document of the
   * given size input by <code>size</code>.
   */
  public Document makeDocument(int size) throws Exception {
    LeftOver lvr = leftovr.get();
    if (lvr == null || lvr.docdata == null || lvr.docdata.getBody() == null
        || lvr.docdata.getBody().length() == 0) {
      resetLeftovers();
    }
    DocData docData = getDocState().docData;
    DocData dd = (lvr == null ? source.getNextDocData(docData) : lvr.docdata);
    int cnt = (lvr == null ? 0 : lvr.cnt);
    while (dd.getBody() == null || dd.getBody().length() < size) {
      DocData dd2 = dd;
      dd = source.getNextDocData(new DocData());
      cnt = 0;
      dd.setBody(dd2.getBody() + dd.getBody());
    }
    Document doc = createDocument(dd, size, cnt);
    if (dd.getBody() == null || dd.getBody().length() == 0) {
      resetLeftovers();
    } else {
      if (lvr == null) {
        lvr = new LeftOver();
        leftovr.set(lvr);
      }
      lvr.docdata = dd;
      lvr.cnt = ++cnt;
    }
    return doc;
  }
  
  /** Reset inputs so that the test run would behave, input wise, as if it just started. */
  public synchronized void resetInputs() throws IOException {
    source.printStatistics("docs");
    // re-initiate since properties by round may have changed.
    setConfig(config, source);
    source.resetInputs();
    numDocsCreated.set(0);
    resetLeftovers();
  }
  
  /** Set the configuration parameters of this doc maker. */
  public void setConfig(Config config, ContentSource source) {
    this.config = config;
    this.source = source;

    boolean stored = config.get("doc.stored", false);
    boolean bodyStored = config.get("doc.body.stored", stored);
    boolean tokenized = config.get("doc.tokenized", true);
    boolean bodyTokenized = config.get("doc.body.tokenized", tokenized);
    boolean norms = config.get("doc.tokenized.norms", false);
    boolean bodyNorms = config.get("doc.body.tokenized.norms", true);
    boolean bodyOffsets = config.get("doc.body.offsets", false);
    boolean termVec = config.get("doc.term.vector", false);
    boolean termVecPositions = config.get("doc.term.vector.positions", false);
    boolean termVecOffsets = config.get("doc.term.vector.offsets", false);
    
    valType = new FieldType(TextField.TYPE_NOT_STORED);
    valType.setStored(stored);
    valType.setTokenized(tokenized);
    valType.setOmitNorms(!norms);
    valType.setStoreTermVectors(termVec);
    valType.setStoreTermVectorPositions(termVecPositions);
    valType.setStoreTermVectorOffsets(termVecOffsets);
    valType.freeze();

    bodyValType = new FieldType(TextField.TYPE_NOT_STORED);
    bodyValType.setStored(bodyStored);
    bodyValType.setTokenized(bodyTokenized);
    bodyValType.setOmitNorms(!bodyNorms);
    if (bodyTokenized && bodyOffsets) {
      bodyValType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
    }
    bodyValType.setStoreTermVectors(termVec);
    bodyValType.setStoreTermVectorPositions(termVecPositions);
    bodyValType.setStoreTermVectorOffsets(termVecOffsets);
    bodyValType.freeze();

    storeBytes = config.get("doc.store.body.bytes", false);
    
    reuseFields = config.get("doc.reuse.fields", true);

    // In a multi-rounds run, it is important to reset DocState since settings
    // of fields may change between rounds, and this is the only way to reset
    // the cache of all threads.
    docState = new ThreadLocal<>();
    
    indexProperties = config.get("doc.index.props", false);

    updateDocIDLimit = config.get("doc.random.id.limit", -1);
    if (updateDocIDLimit != -1) {
      r = new Random(179);
    }
  }

}
