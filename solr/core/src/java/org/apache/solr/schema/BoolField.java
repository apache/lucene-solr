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
package org.apache.solr.schema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.docvalues.BoolDocValues;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.CharsRefBuilder;
import org.apache.lucene.util.mutable.MutableValue;
import org.apache.lucene.util.mutable.MutableValueBool;
import org.apache.solr.analysis.SolrAnalyzer;
import org.apache.solr.response.TextResponseWriter;
import org.apache.solr.search.QParser;
import org.apache.solr.search.function.OrdFieldSource;
import org.apache.solr.uninverting.UninvertingReader.Type;
/**
 *
 */
public class BoolField extends PrimitiveFieldType {
  @Override
  public SortField getSortField(SchemaField field,boolean reverse) {
    field.checkSortability();
    return getStringSort(field,reverse);
  }

  @Override
  public Type getUninversionType(SchemaField sf) {
    if (sf.multiValued()) {
      return Type.SORTED_SET_BINARY;
    } else {
      return Type.SORTED;
    }
  }

  @Override
  public ValueSource getValueSource(SchemaField field, QParser qparser) {
    field.checkFieldCacheSource();
    return new BoolFieldSource(field.name);
  }

  // avoid instantiating every time...
  public final static char[] TRUE_TOKEN = {'T'};
  public final static char[] FALSE_TOKEN = {'F'};

  ////////////////////////////////////////////////////////////////////////
  // TODO: look into creating my own queryParser that can more efficiently
  // handle single valued non-text fields (int,bool,etc) if needed.

  protected final static Analyzer boolAnalyzer = new SolrAnalyzer() {
    @Override
    public TokenStreamComponents createComponents(String fieldName) {
      Tokenizer tokenizer = new Tokenizer() {
        final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
        boolean done = false;

        @Override
        public void reset() throws IOException {
          super.reset();
          done = false;
        }

        @Override
        public boolean incrementToken() throws IOException {
          clearAttributes();
          if (done) return false;
          done = true;
          int ch = input.read();
          if (ch==-1) return false;
          termAtt.copyBuffer(
                  ((ch=='t' || ch=='T' || ch=='1') ? TRUE_TOKEN : FALSE_TOKEN)
                  ,0,1);
          return true;
        }
      };

      return new TokenStreamComponents(tokenizer);
    }
  };


  @Override
  public Analyzer getIndexAnalyzer() {
    return boolAnalyzer;
  }

  @Override
  public Analyzer getQueryAnalyzer() {
    return boolAnalyzer;
  }

  @Override
  public String toInternal(String val) {
    char ch = (val!=null && val.length()>0) ? val.charAt(0) : 0;
    return (ch=='1' || ch=='t' || ch=='T') ? "T" : "F";
  }

  @Override
  public String toExternal(IndexableField f) {
    if (null != f.binaryValue()) {
      return indexedToReadable(f.binaryValue().utf8ToString());
    }
    if (null != f.stringValue()) {
      return indexedToReadable(f.stringValue());
    }
    return null;
  }

  @Override
  public Boolean toObject(IndexableField f) {
    return Boolean.valueOf( toExternal(f) );
  }

  @Override
  public Object toObject(SchemaField sf, BytesRef term) {
    return term.bytes[term.offset] == 'T';
  }

  @Override
  public String indexedToReadable(String indexedForm) {
    char ch = indexedForm.charAt(0);
    return ch=='T' ? "true" : "false";
  }

  private static final CharsRef TRUE = new CharsRef("true");
  private static final CharsRef FALSE = new CharsRef("false");

  @Override
  public CharsRef indexedToReadable(BytesRef input, CharsRefBuilder charsRef) {
    if (input.length > 0 && input.bytes[input.offset] == 'T') {
      charsRef.copyChars(TRUE);
    } else {
      charsRef.copyChars(FALSE);
    }
    return charsRef.get();
  }

  @Override
  public void write(TextResponseWriter writer, String name, IndexableField f) throws IOException {
    writer.writeBool(name, f.stringValue().charAt(0) == 'T');
  }

  @Override
  public Object marshalSortValue(Object value) {
    return marshalStringSortValue(value);
  }

  @Override
  public Object unmarshalSortValue(Object value) {
    return unmarshalStringSortValue(value);
  }

  @Override
  public List<IndexableField> createFields(SchemaField field, Object value, float boost) {
    IndexableField fval = createField(field, value, boost);

    if (field.hasDocValues()) {
      IndexableField docval;
      final BytesRef bytes = new BytesRef(toInternal(value.toString()));
      if (field.multiValued()) {
        docval = new SortedSetDocValuesField(field.getName(), bytes);
      } else {
        docval = new SortedDocValuesField(field.getName(), bytes);
      }

      // Only create a list of we have 2 values...
      if (fval != null) {
        List<IndexableField> fields = new ArrayList<>(2);
        fields.add(fval);
        fields.add(docval);
        return fields;
      }

      fval = docval;
    }
    return Collections.singletonList(fval);
  }

  @Override
  public void checkSchemaField(final SchemaField field) {
  }
}

// TODO - this can be much more efficient - use FixedBitSet or Bits
class BoolFieldSource extends ValueSource {
  protected String field;

  public BoolFieldSource(String field) {
    this.field = field;
  }

  @Override
  public String description() {
    return "bool(" + field + ')';
  }


  @Override
  public FunctionValues getValues(Map context, LeafReaderContext readerContext) throws IOException {
    final SortedDocValues sindex = DocValues.getSorted(readerContext.reader(), field);

    // figure out what ord maps to true
    int nord = sindex.getValueCount();
    // if no values in the segment, default trueOrd to something other then -1 (missing)
    int tord = -2;
    for (int i=0; i<nord; i++) {
      final BytesRef br = sindex.lookupOrd(i);
      if (br.length==1 && br.bytes[br.offset]=='T') {
        tord = i;
        break;
      }
    }

    final int trueOrd = tord;

    return new BoolDocValues(this) {

      private int getOrdForDoc(int doc) throws IOException {
        if (doc > sindex.docID()) {
          sindex.advance(doc);
        }
        if (doc == sindex.docID()) {
          return sindex.ordValue();
        } else {
          return -1;
        }
      }
      @Override
      
      public boolean boolVal(int doc) throws IOException {
        return getOrdForDoc(doc) == trueOrd;
      }

      @Override
      public boolean exists(int doc) throws IOException {
        return getOrdForDoc(doc) != -1;
      }

      @Override
      public ValueFiller getValueFiller() {
        return new ValueFiller() {
          private final MutableValueBool mval = new MutableValueBool();

          @Override
          public MutableValue getValue() {
            return mval;
          }

          @Override
          public void fillValue(int doc) throws IOException {
            int ord = getOrdForDoc(doc);
            mval.value = (ord == trueOrd);
            mval.exists = (ord != -1);
          }
        };
      }
    };
  }

  @Override
  public boolean equals(Object o) {
    return o.getClass() == BoolFieldSource.class && this.field.equals(((BoolFieldSource)o).field);
  }

  private static final int hcode = OrdFieldSource.class.hashCode();
  @Override
  public int hashCode() {
    return hcode + field.hashCode();
  };

}
