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
import org.apache.lucene.search.FieldCache.DocTerms;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.PagedBytes;
import org.apache.lucene.util.packed.GrowableWriter;
import org.apache.lucene.util.packed.PackedInts;

// TODO: this if DocTermsIndex was already created, we should share it...
public class DocTermsCreator extends EntryCreatorWithOptions<DocTerms>
{
  public static final int FASTER_BUT_MORE_RAM = 2;

  public String field;

  public DocTermsCreator( String field )
  {
    super( FASTER_BUT_MORE_RAM ); // By default turn on FASTER_BUT_MORE_RAM
    if( field == null ) {
      throw new IllegalArgumentException( "field can not be null" );
    }
    this.field = field;
  }

  public DocTermsCreator( String field, int flags )
  {
    super( flags );
    if( field == null ) {
      throw new IllegalArgumentException( "field can not be null" );
    }
    this.field = field;
  }

  @Override
  public SimpleEntryKey getCacheKey() {
    return new SimpleEntryKey( DocTermsCreator.class, field );
  }

  @Override
  public DocTerms create(IndexReader reader) throws IOException {

    Terms terms = MultiFields.getTerms(reader, field);

    final boolean fasterButMoreRAM = hasOption( FASTER_BUT_MORE_RAM );
    final int termCountHardLimit = reader.maxDoc();

    // Holds the actual term data, expanded.
    final PagedBytes bytes = new PagedBytes(15);

    int startBPV;

    if (terms != null) {
      // Try for coarse estimate for number of bits; this
      // should be an underestimate most of the time, which
      // is fine -- GrowableWriter will reallocate as needed
      long numUniqueTerms = 0;
      try {
        numUniqueTerms = terms.getUniqueTermCount();
      } catch (UnsupportedOperationException uoe) {
        numUniqueTerms = -1;
      }
      if (numUniqueTerms != -1) {
        if (numUniqueTerms > termCountHardLimit) {
          numUniqueTerms = termCountHardLimit;
        }
        startBPV = PackedInts.bitsRequired(numUniqueTerms*4);
      } else {
        startBPV = 1;
      }
    } else {
      startBPV = 1;
    }

    final GrowableWriter docToOffset = new GrowableWriter(startBPV, reader.maxDoc(), fasterButMoreRAM);

    // pointer==0 means not set
    bytes.copyUsingLengthPrefix(new BytesRef());

    if (terms != null) {
      int termCount = 0;
      final TermsEnum termsEnum = terms.iterator();
      final Bits liveDocs = MultiFields.getLiveDocs(reader);
      DocsEnum docs = null;
      while(true) {
        if (termCount++ == termCountHardLimit) {
          // app is misusing the API (there is more than
          // one term per doc); in this case we make best
          // effort to load what we can (see LUCENE-2142)
          break;
        }

        final BytesRef term = termsEnum.next();
        if (term == null) {
          break;
        }
        final long pointer = bytes.copyUsingLengthPrefix(term);
        docs = termsEnum.docs(liveDocs, docs);
        while (true) {
          final int docID = docs.nextDoc();
          if (docID == DocIdSetIterator.NO_MORE_DOCS) {
            break;
          }
          docToOffset.set(docID, pointer);
        }
      }
    }

    // maybe an int-only impl?
    return new DocTermsImpl(bytes.freeze(true), docToOffset.getMutable());
  }

  @Override
  public DocTerms validate(DocTerms entry, IndexReader reader) throws IOException {
    // TODO? nothing? perhaps subsequent call with FASTER_BUT_MORE_RAM?
    return entry;
  }

  private static class DocTermsImpl extends DocTerms {
    private final PagedBytes.Reader bytes;
    private final PackedInts.Reader docToOffset;

    public DocTermsImpl(PagedBytes.Reader bytes, PackedInts.Reader docToOffset) {
      this.bytes = bytes;
      this.docToOffset = docToOffset;
    }

    @Override
    public int size() {
      return docToOffset.size();
    }

    @Override
    public boolean exists(int docID) {
      return docToOffset.get(docID) == 0;
    }

    @Override
    public BytesRef getTerm(int docID, BytesRef ret) {
      final long pointer = docToOffset.get(docID);
      return bytes.fill(ret, pointer);
    }
  }
}
