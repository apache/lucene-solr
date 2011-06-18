package org.apache.lucene.index;

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

import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.OpenBitSet;
import org.apache.lucene.util.Version;

/**
 * Split an index based on a given primary key term 
 * and a 'middle' term.  If the middle term is present, it's
 * sent to dir2.
 */
public class PKIndexSplitter {
  private Term midTerm;
  Directory input;
  Directory dir1;
  Directory dir2; 
  
  public PKIndexSplitter(Term midTerm, Directory input, 
      Directory dir1, Directory dir2) {
    this.midTerm = midTerm;
    this.input = input;
    this.dir1 = dir1;
    this.dir2 = dir2;
  }
  
  public void split() throws IOException {
    IndexReader reader = IndexReader.open(input);
    OpenBitSet lowDels = setDeletes(reader, null, midTerm.bytes());
    OpenBitSet hiDels = setDeletes(reader, midTerm.bytes(), null);
    
    createIndex(dir1, reader, lowDels);
    createIndex(dir2, reader, hiDels);
    reader.close();
  }
  
  private void createIndex(Directory target, IndexReader reader, OpenBitSet bv) throws IOException {
    IndexWriter w = new IndexWriter(target, new IndexWriterConfig(
        Version.LUCENE_CURRENT,
        new WhitespaceAnalyzer(Version.LUCENE_CURRENT))
        .setOpenMode(OpenMode.CREATE));
    w.addIndexes(new DeletesIndexReader(reader, bv));
    w.close();
  }
  
  private OpenBitSet setDeletes(IndexReader reader, BytesRef startTerm, 
      BytesRef endTermExcl) throws IOException {
    OpenBitSet incl = new OpenBitSet(reader.maxDoc());
    Terms terms = MultiFields.getTerms(reader, midTerm.field());
    TermsEnum te = terms.iterator();
    if (startTerm != null) {
      te.seek(startTerm);
    }
    while (true) {
      final BytesRef term = te.next();
      if (term == null) {
        break;
      }
      if (endTermExcl != null && term.compareTo(endTermExcl) >= 0) {
        break;
      }
      DocsEnum docs = MultiFields.getTermDocsEnum(reader, 
          MultiFields.getDeletedDocs(reader), midTerm.field(), term);
      while (true) {
        final int doc = docs.nextDoc();
        if (doc != DocsEnum.NO_MORE_DOCS) {
          incl.set(doc);
        } else break;
      }
    }
    OpenBitSet dels = new OpenBitSet(reader.maxDoc());
    for (int x=0; x < reader.maxDoc(); x++) {
      if (!incl.get(x)) {
        dels.set(x);
      }
    }
    return dels;
  }
  
  public static class DeletesIndexReader extends FilterIndexReader {
    OpenBitSet readerDels;
    
    public DeletesIndexReader(IndexReader reader, OpenBitSet deletes) {
      super(new SlowMultiReaderWrapper(reader));
      readerDels = new OpenBitSet(reader.maxDoc());
      if (in.hasDeletions()) {
        final Bits oldDelBits = MultiFields.getDeletedDocs(in);
        assert oldDelBits != null;
        for (int i = 0; i < in.maxDoc(); i++) {
          if (oldDelBits.get(i) || deletes.get(i)) {
            readerDels.set(i);
          }
        }
      } else {
        readerDels = deletes;
      }
    }
    
    @Override
    public int numDocs() {
      return in.maxDoc() - (int)readerDels.cardinality();
    }
    
    @Override
    public boolean hasDeletions() {
      return (int)readerDels.cardinality() > 0;
    }
    
    @Override
    public Bits getDeletedDocs() {
      return readerDels;
    }
  }
}
