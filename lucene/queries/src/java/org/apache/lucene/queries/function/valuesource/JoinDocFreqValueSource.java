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
package org.apache.lucene.queries.function.valuesource;

import java.io.IOException;
import java.util.Map;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.MultiTerms;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.docvalues.IntDocValues;
import org.apache.lucene.util.BytesRef;

/**
 * Use a field value and find the Document Frequency within another field.
 * 
 * @since solr 4.0
 */
public class JoinDocFreqValueSource extends FieldCacheSource {

  public static final String NAME = "joindf";
  
  protected final String qfield;
  
  public JoinDocFreqValueSource(String field, String qfield) {
    super(field);
    this.qfield = qfield;
  }

  @Override
  public String description() {
    return NAME + "(" + field +":("+qfield+"))";
  }

  @Override
  public FunctionValues getValues(Map context, LeafReaderContext readerContext) throws IOException
  {
    final BinaryDocValues terms = DocValues.getBinary(readerContext.reader(), field);
    final IndexReader top = ReaderUtil.getTopLevelContext(readerContext).reader();
    Terms t = MultiTerms.getTerms(top, qfield);
    final TermsEnum termsEnum = t == null ? TermsEnum.EMPTY : t.iterator();
    
    return new IntDocValues(this) {

      int lastDocID = -1;

      @Override
      public int intVal(int doc) throws IOException {
        if (doc < lastDocID) {
          throw new IllegalArgumentException("docs were sent out-of-order: lastDocID=" + lastDocID + " vs docID=" + doc);
        }
        lastDocID = doc;
        int curDocID = terms.docID();
        if (doc > curDocID) {
          curDocID = terms.advance(doc);
        }
        if (doc == curDocID) {
          BytesRef term = terms.binaryValue();
          if (termsEnum.seekExact(term)) {
            return termsEnum.docFreq();
          }
        }
        return 0;
      }
    };
  }
  
  @Override
  public boolean equals(Object o) {
    if (o.getClass() != JoinDocFreqValueSource.class) return false;
    JoinDocFreqValueSource other = (JoinDocFreqValueSource)o;
    if( !qfield.equals( other.qfield ) ) return false;
    return super.equals(other);
  }

  @Override
  public int hashCode() {
    return qfield.hashCode() + super.hashCode();
  }
}
