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
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.IndexableFieldType;
import org.apache.lucene.util.BytesRef;

/** Defers actually loading a field's value until you ask
 *  for it.  You must not use the returned Field instances
 *  after the provided reader has been closed. */

public class LazyDocument {
  private IndexReader reader;
  private final int docID;

  // null until first field is loaded
  private Document doc;

  private Map<Integer,Integer> fields = new HashMap<Integer,Integer>();

  public LazyDocument(IndexReader reader, int docID) {
    this.reader = reader;
    this.docID = docID;
  }

  public IndexableField getField(FieldInfo fieldInfo) {  
    Integer num = fields.get(fieldInfo.number);
    if (num == null) {
      num = 0;
    } else {
      num++;
    }
    fields.put(fieldInfo.number, num);

    return new LazyField(fieldInfo.name, num);
  }

  private synchronized Document getDocument() {
    if (doc == null) {
      try {
        doc = reader.document(docID);
      } catch (IOException ioe) {
        throw new IllegalStateException("unable to load document", ioe);
      }
      reader = null;
    }
    return doc;
  }

  private class LazyField implements IndexableField {
    private String name;
    private int num;
    
    public LazyField(String name, int num) {
      this.name = name;
      this.num = num;
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
      if (num == 0) {
        return getDocument().getField(name).binaryValue();
      } else {
        return getDocument().getFields(name)[num].binaryValue();
      }
    }

    @Override
    public String stringValue() {
      if (num == 0) {
        return getDocument().getField(name).stringValue();
      } else {
        return getDocument().getFields(name)[num].stringValue();
      }
    }

    @Override
    public Reader readerValue() {
      if (num == 0) {
        return getDocument().getField(name).readerValue();
      } else {
        return getDocument().getFields(name)[num].readerValue();
      }
    }

    @Override
    public Number numericValue() {
      if (num == 0) {
        return getDocument().getField(name).numericValue();
      } else {
        return getDocument().getFields(name)[num].numericValue();
      }
    }

    @Override
    public IndexableFieldType fieldType() {
      if (num == 0) {
        return getDocument().getField(name).fieldType();
      } else {
        return getDocument().getFields(name)[num].fieldType();
      }
    }

    @Override
    public TokenStream tokenStream(Analyzer analyzer) throws IOException {
      if (num == 0) {
        return getDocument().getField(name).tokenStream(analyzer);
      } else {
        return getDocument().getFields(name)[num].tokenStream(analyzer);
      }
    }
  }
}
