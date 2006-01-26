/**
 * Copyright 2006 The Apache Software Foundation
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

package org.apache.solr.schema;

import org.apache.lucene.search.SortField;
import org.apache.lucene.search.function.ValueSource;
import org.apache.lucene.search.function.OrdFieldSource;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.document.Field;
import org.apache.solr.request.XMLWriter;

import java.util.Map;
import java.io.Reader;
import java.io.IOException;
/**
 * @author yonik
 * @version $Id$
 */
public class BoolField extends FieldType {
  protected void init(IndexSchema schema, Map<String,String> args) {
  }

  public SortField getSortField(SchemaField field,boolean reverse) {
    return getStringSort(field,reverse);
  }

  public ValueSource getValueSource(SchemaField field) {
    return new OrdFieldSource(field.name);
  }

  // avoid instantiating every time...
  protected final static Token TRUE_TOKEN = new Token("T",0,1);
  protected final static Token FALSE_TOKEN = new Token("F",0,1);

  ////////////////////////////////////////////////////////////////////////
  // TODO: look into creating my own queryParser that can more efficiently
  // handle single valued non-text fields (int,bool,etc) if needed.


  protected final static Analyzer boolAnalyzer = new Analyzer() {
      public TokenStream tokenStream(String fieldName, Reader reader) {
        return new Tokenizer(reader) {
          boolean done=false;
          public Token next() throws IOException {
            if (done) return null;
            done=true;
            int ch = input.read();
            if (ch==-1) return null;
            return (ch=='t' || ch=='T' || ch=='1') ? TRUE_TOKEN : FALSE_TOKEN;
          }
        };
      }
    };

  public Analyzer getAnalyzer() {
    return boolAnalyzer;
  }

  public Analyzer getQueryAnalyzer() {
    return boolAnalyzer;
  }

  public String toInternal(String val) {
    char ch = (val!=null && val.length()>0) ? val.charAt(0) : 0;
    return (ch=='1' || ch=='t' || ch=='T') ? "T" : "F";
  }

  public String toExternal(Field f) {
    return indexedToReadable(f.stringValue());
  }

  public String indexedToReadable(String indexedForm) {
    char ch = indexedForm.charAt(0);
    return ch=='T' ? "true" : "false";
  }

  public void write(XMLWriter xmlWriter, String name, Field f) throws IOException {
    xmlWriter.writeBool(name, f.stringValue().charAt(0) =='T');
  }
}
