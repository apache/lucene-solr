package org.apache.lucene.benchmark.byTask.feeds;

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

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

import org.apache.lucene.benchmark.byTask.utils.Config;
import org.apache.lucene.benchmark.byTask.utils.Format;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.ValuesField;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.Field.TermVector;
import org.apache.lucene.index.values.Values;

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
public class DocMaker {

  private static class LeftOver {
    private DocData docdata;
    private int cnt;
  }

  private Random r;
  private int updateDocIDLimit;

  static class DocState {
    
    private final Map<String,Field> fields;
    private final boolean reuseFields;
    final Document doc;
    DocData docData = new DocData();
    
    public DocState(boolean reuseFields, Store store, Store bodyStore, Index index, Index bodyIndex, TermVector termVector) {

      this.reuseFields = reuseFields;
      
      if (reuseFields) {
        fields =  new HashMap<String,Field>();
        
        // Initialize the map with the default fields.
        fields.put(BODY_FIELD, new Field(BODY_FIELD, "", bodyStore, bodyIndex, termVector));
        fields.put(TITLE_FIELD, new Field(TITLE_FIELD, "", store, index, termVector));
        fields.put(DATE_FIELD, new Field(DATE_FIELD, "", store, index, termVector));
        fields.put(ID_FIELD, new Field(ID_FIELD, "", Field.Store.YES, Field.Index.NOT_ANALYZED_NO_NORMS));
        fields.put(NAME_FIELD, new Field(NAME_FIELD, "", store, index, termVector));
        
        doc = new Document();
      } else {
        fields = null;
        doc = null;
      }
    }

    /**
     * Returns a field corresponding to the field name. If
     * <code>reuseFields</code> was set to true, then it attempts to reuse a
     * Field instance. If such a field does not exist, it creates a new one.
     */
    Field getField(String name, Store store, Index index, TermVector termVector) {
      if (!reuseFields) {
        return new Field(name, "", store, index, termVector);
      }
      
      Field f = fields.get(name);
      if (f == null) {
        f = new Field(name, "", store, index, termVector);
        fields.put(name, f);
      }
      return f;
    }
  }
  
  private int numDocsCreated = 0;
  private boolean storeBytes = false;

  // leftovers are thread local, because it is unsafe to share residues between threads
  private ThreadLocal<LeftOver> leftovr = new ThreadLocal<LeftOver>();
  private ThreadLocal<DocState> docState = new ThreadLocal<DocState>();

  public static final String BODY_FIELD = "body";
  public static final String TITLE_FIELD = "doctitle";
  public static final String DATE_FIELD = "docdate";
  public static final String ID_FIELD = "docid";
  public static final String BYTES_FIELD = "bytes";
  public static final String NAME_FIELD = "docname";

  protected Config config;

  protected Store storeVal = Store.NO;
  protected Store bodyStoreVal = Store.NO;
  protected Index indexVal = Index.ANALYZED_NO_NORMS;
  protected Index bodyIndexVal = Index.ANALYZED;
  protected TermVector termVecVal = TermVector.NO;
  
  protected ContentSource source;
  protected boolean reuseFields;
  protected boolean indexProperties;
  
  private int lastPrintedNumUniqueTexts = 0;

  private long lastPrintedNumUniqueBytes = 0;

  private int printNum = 0;
  private Map<String, Values> fieldVauleMap;

  // create a doc
  // use only part of the body, modify it to keep the rest (or use all if size==0).
  // reset the docdata properties so they are not added more than once.
  private Document createDocument(DocData docData, int size, int cnt) throws UnsupportedEncodingException {
    Values valueType;
    final DocState ds = getDocState();
    final Document doc = reuseFields ? ds.doc : new Document();
    doc.getFields().clear();
    
    // Set ID_FIELD
    Field idField = ds.getField(ID_FIELD, storeVal, Index.NOT_ANALYZED_NO_NORMS, termVecVal);
    idField.setValue("doc" + (r != null ? r.nextInt(updateDocIDLimit) : incrNumDocsCreated()));
    doc.add(idField);
    
    // Set NAME_FIELD
    String name = docData.getName();
    if (name == null) name = "";
    name = cnt < 0 ? name : name + "_" + cnt;
    Field nameField = ds.getField(NAME_FIELD, storeVal, indexVal, termVecVal);
    nameField.setValue(name);
    trySetIndexValues(nameField);
    doc.add(nameField);
    
    // Set DATE_FIELD
    String date = docData.getDate();
    if (date == null) {
      date = "";
    }
    Field dateField = ds.getField(DATE_FIELD, storeVal, indexVal, termVecVal);
    dateField.setValue(date);
    trySetIndexValues(dateField);
    doc.add(dateField);
    
    // Set TITLE_FIELD
    String title = docData.getTitle();
    Field titleField = ds.getField(TITLE_FIELD, storeVal, indexVal, termVecVal);
    titleField.setValue(title == null ? "" : title);
    trySetIndexValues(titleField);
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
      Field bodyField = ds.getField(BODY_FIELD, bodyStoreVal, bodyIndexVal, termVecVal);
      bodyField.setValue(bdy);
      trySetIndexValues(bodyField);
      doc.add(bodyField);
      
      if (storeBytes) {
        Field bytesField = ds.getField(BYTES_FIELD, Store.YES, Index.NOT_ANALYZED_NO_NORMS, TermVector.NO);
        bytesField.setValue(bdy.getBytes("UTF-8"));
        trySetIndexValues(bytesField);
        doc.add(bytesField);
        
      }
    }

    if (indexProperties) {
      Properties props = docData.getProps();
      if (props != null) {
        for (final Map.Entry<Object,Object> entry : props.entrySet()) {
          Field f = ds.getField((String) entry.getKey(), storeVal, indexVal, termVecVal);
          f.setValue((String) entry.getValue());
          trySetIndexValues(f);
          doc.add(f);
        }
        docData.setProps(null);
      }
    }
    
    //System.out.println("============== Created doc "+numDocsCreated+" :\n"+doc+"\n==========");
    return doc;
  }
  
  private void trySetIndexValues(Field field) {
    final Values valueType;
    if((valueType = fieldVauleMap.get(field.name())) != null)
      ValuesField.set(field, valueType);
  }

  private void resetLeftovers() {
    leftovr.set(null);
  }

  protected DocState getDocState() {
    DocState ds = docState.get();
    if (ds == null) {
      ds = new DocState(reuseFields, storeVal, bodyStoreVal, indexVal, bodyIndexVal, termVecVal);
      docState.set(ds);
    }
    return ds;
  }

  protected synchronized int incrNumDocsCreated() {
    return numDocsCreated++;
  }

  /**
   * Closes the {@link DocMaker}. The base implementation closes the
   * {@link ContentSource}, and it can be overridden to do more work (but make
   * sure to call super.close()).
   */
  public void close() throws IOException {
    source.close();
  }
  
  /**
   * Returns the number of bytes generated by the content source since last
   * reset.
   */
  public synchronized long getBytesCount() {
    return source.getBytesCount();
  }

  /**
   * Returns the total number of bytes that were generated by the content source
   * defined to that doc maker.
   */ 
  public long getTotalBytesCount() {
    return source.getTotalBytesCount();
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
  
  public void printDocStatistics() {
    boolean print = false;
    String col = "                  ";
    StringBuffer sb = new StringBuffer();
    String newline = System.getProperty("line.separator");
    sb.append("------------> ").append(getClass().getSimpleName()).append(" statistics (").append(printNum).append("): ").append(newline);
    int nut = source.getTotalDocsCount();
    if (nut > lastPrintedNumUniqueTexts) {
      print = true;
      sb.append("total count of unique texts: ").append(Format.format(0,nut,col)).append(newline);
      lastPrintedNumUniqueTexts = nut;
    }
    long nub = getTotalBytesCount();
    if (nub > lastPrintedNumUniqueBytes) {
      print = true;
      sb.append("total bytes of unique texts: ").append(Format.format(0,nub,col)).append(newline);
      lastPrintedNumUniqueBytes = nub;
    }
    if (source.getDocsCount() > 0) {
      print = true;
      sb.append("num docs added since last inputs reset:   ").append(Format.format(0,source.getDocsCount(),col)).append(newline);
      sb.append("total bytes added since last inputs reset: ").append(Format.format(0,getBytesCount(),col)).append(newline);
    }
    if (print) {
      System.out.println(sb.append(newline).toString());
      printNum++;
    }
  }
  
  /** Reset inputs so that the test run would behave, input wise, as if it just started. */
  public synchronized void resetInputs() throws IOException {
    printDocStatistics();
    // re-initiate since properties by round may have changed.
    setConfig(config);
    source.resetInputs();
    numDocsCreated = 0;
    resetLeftovers();
  }
  
  private static final Map<String, Values> parseValueFields(String fields) {
    if(fields == null)
      return Collections.emptyMap();
    String[] split = fields.split(";");
    Map<String, Values> result = new HashMap<String, Values>();
    for (String tuple : split) {
      final String[] nameValue = tuple.split(":");
      if (nameValue.length != 2) {
        throw new IllegalArgumentException("illegal doc.stored.values format: "
            + fields + " expected fieldname:ValuesType;...;...;");
      }
      result.put(nameValue[0].trim(), Values.valueOf(nameValue[1]));
    }
    return result;
  }
  
  /** Set the configuration parameters of this doc maker. */
  public void setConfig(Config config) {
    this.config = config;
    try {
      String sourceClass = config.get("content.source", "org.apache.lucene.benchmark.byTask.feeds.SingleDocSource");
      source = Class.forName(sourceClass).asSubclass(ContentSource.class).newInstance();
      source.setConfig(config);
    } catch (Exception e) {
      // Should not get here. Throw runtime exception.
      throw new RuntimeException(e);
    }

    boolean stored = config.get("doc.stored", false);
    boolean bodyStored = config.get("doc.body.stored", stored);
    boolean tokenized = config.get("doc.tokenized", true);
    boolean bodyTokenized = config.get("doc.body.tokenized", tokenized);
    boolean norms = config.get("doc.tokenized.norms", false);
    boolean bodyNorms = config.get("doc.body.tokenized.norms", true);
    boolean termVec = config.get("doc.term.vector", false);
    fieldVauleMap = parseValueFields(config.get("doc.stored.values", null));
    storeVal = (stored ? Field.Store.YES : Field.Store.NO);
    bodyStoreVal = (bodyStored ? Field.Store.YES : Field.Store.NO);
    if (tokenized) {
      indexVal = norms ? Index.ANALYZED : Index.ANALYZED_NO_NORMS;
    } else {
      indexVal = norms ? Index.NOT_ANALYZED : Index.NOT_ANALYZED_NO_NORMS;
    }

    if (bodyTokenized) {
      bodyIndexVal = bodyNorms ? Index.ANALYZED : Index.ANALYZED_NO_NORMS;
    } else {
      bodyIndexVal = bodyNorms ? Index.NOT_ANALYZED : Index.NOT_ANALYZED_NO_NORMS;
    }

    boolean termVecPositions = config.get("doc.term.vector.positions", false);
    boolean termVecOffsets = config.get("doc.term.vector.offsets", false);
    if (termVecPositions && termVecOffsets) {
      termVecVal = TermVector.WITH_POSITIONS_OFFSETS;
    } else if (termVecPositions) {
      termVecVal = TermVector.WITH_POSITIONS;
    } else if (termVecOffsets) {
      termVecVal = TermVector.WITH_OFFSETS;
    } else if (termVec) {
      termVecVal = TermVector.YES;
    } else {
      termVecVal = TermVector.NO;
    }
    storeBytes = config.get("doc.store.body.bytes", false);
    
    reuseFields = config.get("doc.reuse.fields", true);

    // In a multi-rounds run, it is important to reset DocState since settings
    // of fields may change between rounds, and this is the only way to reset
    // the cache of all threads.
    docState = new ThreadLocal<DocState>();
    
    indexProperties = config.get("doc.index.props", false);
    updateDocIDLimit = config.get("doc.random.id.limit", -1);
    if (updateDocIDLimit != -1) {
      r = new Random(179);
    }
  }

}
