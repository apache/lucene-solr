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

package org.apache.solr.search.function;

import java.io.IOException;
import java.util.Map;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexReader.AtomicReaderContext;
import org.apache.lucene.search.FieldCache.DocTerms;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.ReaderUtil;
import org.apache.solr.common.SolrException;

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
  public DocValues getValues(Map context, AtomicReaderContext readerContext) throws IOException 
  {
    final DocTerms terms = cache.getTerms(readerContext.reader, field, true );
    final IndexReader top = ReaderUtil.getTopLevelContext(readerContext).reader;
    
    return new IntDocValues(this) {
      BytesRef ref = new BytesRef();

      @Override
      public int intVal(int doc) 
      {
        try {
          terms.getTerm(doc, ref);
          int v = top.docFreq( qfield, ref ); 
          //System.out.println( NAME+"["+field+"="+ref.utf8ToString()+"=("+qfield+":"+v+")]" );
          return v;
        } 
        catch (IOException e) {
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "caught exception in function "+description()+" : doc="+doc, e);
        }
      }
    };
  }
  
  @Override
  public boolean equals(Object o) {
    if (o.getClass() !=  JoinDocFreqValueSource.class) return false;
    JoinDocFreqValueSource other = (JoinDocFreqValueSource)o;
    if( !qfield.equals( other.qfield ) ) return false;
    return super.equals(other);
  }

  @Override
  public int hashCode() {
    return qfield.hashCode() + super.hashCode();
  };
}
