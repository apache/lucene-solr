package org.apache.lucene.search.cache;

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

import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.FieldCache;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.FieldCache.IntParser;
import org.apache.lucene.search.FieldCache.Parser;
import org.apache.lucene.search.cache.CachedArray.IntValues;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.OpenBitSet;

public class IntValuesCreator extends CachedArrayCreator<IntValues>
{
  protected IntParser parser;

  public IntValuesCreator( String field, IntParser parser, int options )
  {
    super( field, options );
    this.parser = parser;
  }

  public IntValuesCreator( String field, IntParser parser )
  {
    super( field );
    this.parser = parser;
  }

  @Override
  public Class getArrayType() {
    return Integer.class;
  }

  @Override
  public Parser getParser() {
    return parser;
  }
  
  @Override
  public int getSortTypeID() {
    return SortField.INT;
  }


  //--------------------------------------------------------------------------------
  //--------------------------------------------------------------------------------

  @Override
  public IntValues create(IndexReader reader) throws IOException {
    return validate( new IntValues(), reader );
  }

  @Override
  public synchronized IntValues validate(IntValues entry, IndexReader reader) throws IOException {
    boolean ok = false;
    
    if( hasOption(OPTION_CACHE_VALUES) ) {
      ok = true;
      if( entry.values == null ) {
        fillIntValues(entry, reader, field);
      }
      else {
        assertSameParser( entry, parser );
      }
    }
    if( hasOption(OPTION_CACHE_BITS) ) {
      ok = true;
      if( entry.valid == null ) {
        fillValidBits(entry, reader, field);
      }
    }
    if( !ok ) {
      throw new RuntimeException( "the config must cache values and/or bits" );
    }
    return entry;
  }

  protected void fillIntValues( IntValues vals, IndexReader reader, String field ) throws IOException
  {
    if( parser == null ) {
      try {
        parser = FieldCache.DEFAULT_INT_PARSER;
        fillIntValues( vals, reader, field );
        return;
      }
      catch (NumberFormatException ne) {
        vals.parserHashCode = null;
        parser = FieldCache.NUMERIC_UTILS_INT_PARSER;
        fillIntValues( vals, reader, field );
        return;
      }
    }
    setParserAndResetCounts(vals, parser);
    
    Terms terms = MultiFields.getTerms(reader, field);
    int maxDoc = reader.maxDoc();
    vals.values = null;
    if (terms != null) {
      final TermsEnum termsEnum = terms.iterator();
      OpenBitSet validBits = (hasOption(OPTION_CACHE_BITS)) ? new OpenBitSet( maxDoc ) : null;
      DocsEnum docs = null;
      try {
        while(true) {
          final BytesRef term = termsEnum.next();
          if (term == null) {
            break;
          }
          final int termval = parser.parseInt(term);
          docs = termsEnum.docs(null, docs);
          while (true) {
            final int docID = docs.nextDoc();
            if (docID == DocIdSetIterator.NO_MORE_DOCS) {
              break;
            }
            if(vals.values == null) {
              vals.values = new int[maxDoc];
            }
            vals.values[docID] = termval;
            vals.numDocs++;
            if( validBits != null ) {
              validBits.set( docID );
            }
          }
          vals.numTerms++;
        }
      } catch (FieldCache.StopFillCacheException stop) {}

      if( vals.valid == null ) {
        vals.valid = checkMatchAllBits( validBits, vals.numDocs, maxDoc );
      }
    }

    if(vals.values == null) {
      vals.values = new int[maxDoc];
    }

    if( vals.valid == null && vals.numDocs < 1 ) {
      vals.valid = new Bits.MatchNoBits( maxDoc );
    }
  }
}