package org.apache.lucene.document;

/**
 * Copyright 2004 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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
import java.io.Reader;
import java.util.List;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.HashSet;
import java.util.Set;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.IndexableFieldType;
import org.apache.lucene.util.BytesRef;

/** Defers actually loading a field's value until you ask
 *  for it.  You must not use the returned Field instances
 *  after the provided reader has been closed. 
 * @see #getField
 */
public class LazyDocument {
  private final IndexReader reader;
  private final int docID;

  // null until first field is loaded
  private Document doc;

  private Map<Integer,List<LazyField>> fields = new HashMap<Integer,List<LazyField>>();
  private Set<String> fieldNames = new HashSet<String>();

  public LazyDocument(IndexReader reader, int docID) {
    this.reader = reader;
    this.docID = docID;
  }

  /**
   * Creates an IndexableField whose value will be lazy loaded if and 
   * when it is used. 
   * <p>
   * <b>NOTE:</b> This method must be called once for each value of the field 
   * name specified in sequence that the values exist.  This method may not be 
   * used to generate multiple, lazy, IndexableField instances refering to 
   * the same underlying IndexableField instance.
   * </p>
   * <p>
   * The lazy loading of field values from all instances of IndexableField 
   * objects returned by this method are all backed by a single Document 
   * per LazyDocument instance.
   * </p>
   */
  public IndexableField getField(FieldInfo fieldInfo) {  

    fieldNames.add(fieldInfo.name);
    List<LazyField> values = fields.get(fieldInfo.number);
    if (null == values) {
      values = new ArrayList<LazyField>();
      fields.put(fieldInfo.number, values);
    } 

    LazyField value = new LazyField(fieldInfo.name, fieldInfo.number);
    values.add(value);

    synchronized (this) {
      // edge case: if someone asks this LazyDoc for more LazyFields
      // after other LazyFields from the same LazyDoc have been
      // actuallized, we need to force the doc to be re-fetched
      // so the new LazyFields are also populated.
      doc = null;
    }
    return value;
  }

  /** 
   * non-private for test only access
   * @lucene.internal 
   */
  synchronized Document getDocument() {
    if (doc == null) {
      try {
        doc = reader.document(docID, fieldNames);
      } catch (IOException ioe) {
        throw new IllegalStateException("unable to load document", ioe);
      }
    }
    return doc;
  }

  // :TODO: synchronize to prevent redundent copying? (sync per field name?)
  private void fetchRealValues(String name, int fieldNum) {
    Document d = getDocument();

    List<LazyField> lazyValues = fields.get(fieldNum);
    IndexableField[] realValues = d.getFields(name);
    
    assert realValues.length <= lazyValues.size() 
      : "More lazy values then real values for field: " + name;
    
    for (int i = 0; i < lazyValues.size(); i++) {
      LazyField f = lazyValues.get(i);
      if (null != f) {
        f.realValue = realValues[i];
      }
    }
  }


  /** 
   * @lucene.internal 
   */
  public class LazyField implements IndexableField {
    private String name;
    private int fieldNum;
    volatile IndexableField realValue = null;

    private LazyField(String name, int fieldNum) {
      this.name = name;
      this.fieldNum = fieldNum;
    }

    /** 
     * non-private for test only access
     * @lucene.internal 
     */
    public boolean hasBeenLoaded() {
      return null != realValue;
    }

    private IndexableField getRealValue() {
      if (null == realValue) {
        fetchRealValues(name, fieldNum);
      }
      assert hasBeenLoaded() : "field value was not lazy loaded";
      assert realValue.name().equals(name()) : 
        "realvalue name != name: " + realValue.name() + " != " + name();

      return realValue;
    }

    @Override
    public String name() {
      return name;
    }

    @Override
    public float boost() {
      return 1.0f;
    }

    @Override
    public BytesRef binaryValue() {
      return getRealValue().binaryValue();
    }

    @Override
    public String stringValue() {
      return getRealValue().stringValue();
    }

    @Override
    public Reader readerValue() {
      return getRealValue().readerValue();
    }

    @Override
    public Number numericValue() {
      return getRealValue().numericValue();
    }

    @Override
    public IndexableFieldType fieldType() {
      return getRealValue().fieldType();
    }

    @Override
    public TokenStream tokenStream(Analyzer analyzer) throws IOException {
      return getRealValue().tokenStream(analyzer);
    }
  }
}
