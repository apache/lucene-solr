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

package org.apache.lucene.index;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.logging.Logger;

import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.index.IndexWriter;       // javadocs
import org.apache.lucene.store.*;
import org.apache.lucene.util.Version;

/** Sort an index by document importance factor. Higher scoring documents are
 * assigned smaller document numbers. Document weights are obtained from a
 * specified field, which has to be single-valued and stored, with string value
 * that represents a float number. Stored fields in the output index remain
 * consistent, i.e. both stored fields and postings are renumbered in sync.
 *
 * <p><b>NOTE</b>: this tool is unaware of documents added
 * atomically via {@link IndexWriter#addDocuments} or {@link
 * IndexWriter#updateDocuments}, which means it can easily
 * break up such document groups.
 */
public class IndexSorter {
  private static final Logger LOG = Logger.getLogger(IndexSorter.class.getName());
  
  private static class PostingMap implements Comparable<PostingMap> {
    private int newDoc;
    private long offset;

    public int compareTo(PostingMap pm) {              // order by newDoc id
      return this.newDoc - pm.newDoc;
    }
  }

  private static class SortedTermPositions implements TermPositions {
    private TermPositions original;
    private int[] oldToNew;

    private int docFreq;

    private PostingMap[] postingMaps = new PostingMap[0];
    private int pointer;

    private int freq;
    private int position;

    private static final String TEMP_FILE = "temp";
    private final RAMDirectory tempDir = new RAMDirectory();
    private RAMOutputStream out;
    private IndexInput in;

    public SortedTermPositions(TermPositions original, int[] oldToNew) {
      this.original = original;
      this.oldToNew = oldToNew;
      try {
        out = (RAMOutputStream)tempDir.createOutput(TEMP_FILE);
      } catch (IOException ioe) {
        LOG.warning("Error creating temporary output: " + ioe);
      }
    }

    public void seek(Term term) throws IOException {
      throw new UnsupportedOperationException();
    }

    public void seek(TermEnum terms) throws IOException {
      original.seek(terms);

      docFreq = terms.docFreq();
      pointer = -1;

      if (docFreq > postingMaps.length) {         // grow postingsMap
        PostingMap[] newMap = new PostingMap[docFreq];
        System.arraycopy(postingMaps, 0, newMap, 0, postingMaps.length);
        for (int i = postingMaps.length; i < docFreq; i++) {
          newMap[i] = new PostingMap();
        }
        postingMaps = newMap;
      }

      out.reset();

      int i = 0;
      while (original.next()) {
        PostingMap map = postingMaps[i++];
        map.newDoc = oldToNew[original.doc()];    // remap the newDoc id
        map.offset = out.getFilePointer();        // save pointer to buffer

        final int tf = original.freq();           // buffer tf & positions
        out.writeVInt(tf);
        int prevPosition = 0;
        for (int j = tf; j > 0; j--) {            // delta encode positions
          int p = original.nextPosition();
          out.writeVInt(p - prevPosition);
          prevPosition = p;
        }
      }
      out.flush();
      docFreq = i;                                // allow for deletions
      
      Arrays.sort(postingMaps, 0, docFreq);       // resort by mapped doc ids

      // NOTE: this might be substantially faster if RAMInputStream were public
      // and supported a reset() operation.
      in = tempDir.openInput(TEMP_FILE);
    }
        
    public boolean next() throws IOException {
      pointer++;
      if (pointer < docFreq) {
        in.seek(postingMaps[pointer].offset);
        freq = in.readVInt();
        position = 0;
        return true;
      }
      return false;
    }
      
    public int doc() { return postingMaps[pointer].newDoc; }
    public int freq() { return freq; }

    public int nextPosition() throws IOException {
      int positionIncrement = in.readVInt();
      position += positionIncrement;
      return position;
    }

    public int read(int[] docs, int[] freqs) {
      throw new UnsupportedOperationException();
    }
    public boolean skipTo(int target) {
      throw new UnsupportedOperationException();
    }

    public byte[] getPayload(byte[] data, int offset) throws IOException {
      return null;
    }

    public int getPayloadLength() {
      return 0;
    }

    public boolean isPayloadAvailable() {
      return false;
    }

    public void close() throws IOException {
      original.close();
    }

  }

  private static class SortingReader extends FilterIndexReader {
    
    private int[] oldToNew;
    private int[] newToOld;

    public SortingReader(IndexReader oldReader, int[] oldToNew) {
      super(oldReader);
      this.oldToNew = oldToNew;
      
      this.newToOld = new int[oldReader.maxDoc()];
      int oldDoc = 0;
      while (oldDoc < oldToNew.length) {
        int newDoc = oldToNew[oldDoc];
        if (newDoc != -1) {
          newToOld[newDoc] = oldDoc;
        }
        oldDoc++;
      }
    }

    @Override
    public IndexReader[] getSequentialSubReaders() {
      return null;
    }

    @Override
    public Document document(int n) throws IOException {
      return document(n, null);
    }

    @Override
    public Document document(int n, FieldSelector fieldSelector)
        throws CorruptIndexException, IOException {
      return super.document(newToOld[n], fieldSelector);
    }

    @Override
    public boolean isDeleted(int n) {
      return false;
    }

    @Override
    public byte[] norms(String f) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void norms(String f, byte[] norms, int offset) throws IOException {
      byte[] oldNorms = super.norms(f);
      int oldDoc = 0;
      while (oldDoc < oldNorms.length) {
        int newDoc = oldToNew[oldDoc];
        if (newDoc != -1) {
          norms[newDoc] = oldNorms[oldDoc];
        }
        oldDoc++;
      }
    }

    @Override
    protected void doSetNorm(int d, String f, byte b) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public TermDocs termDocs() throws IOException {
      throw new UnsupportedOperationException();
    }
    
    @Override
    public TermPositions termPositions() throws IOException {
      return new SortedTermPositions(super.termPositions(), oldToNew);
    }

    @Override
    public TermFreqVector[] getTermFreqVectors(int docNumber)
            throws IOException {
      return super.getTermFreqVectors(newToOld[docNumber]);
    }

    @Override
    protected void doDelete(int n) throws IOException { 
      throw new UnsupportedOperationException();
    }

  }

  private static class DocScore implements Comparable<DocScore> {
    private int oldDoc;
    private float score;

    public int compareTo(DocScore that) {            // order by score, oldDoc
      if (this.score == that.score) {
        return this.oldDoc - that.oldDoc;
      } else {
        return this.score < that.score ? 1 : -1 ;
      }
    }
    
    @Override
    public String toString() {
      return "oldDoc=" + oldDoc + ",score=" + score;
    }
  }

  public IndexSorter() {
    
  }
  
  public void sort(Directory input, Directory output, String field) throws IOException {
    LOG.info("IndexSorter: starting.");
    long start = System.currentTimeMillis();
    IndexReader reader = IndexReader.open(input, true);

    SortingReader sorter = new SortingReader(reader, oldToNew(reader, field));
    IndexWriterConfig cfg = new IndexWriterConfig(Version.LUCENE_31, new WhitespaceAnalyzer(Version.LUCENE_31));
    IndexWriter writer = new IndexWriter(output, cfg);
    writer.addIndexes(new IndexReader[] { sorter });
    writer.close();
    long end = System.currentTimeMillis();
    LOG.info("IndexSorter: done, " + (end - start)
        + " total milliseconds");
  }

  private static int[] oldToNew(IndexReader reader, String field) throws IOException {
    int readerMax = reader.maxDoc();
    DocScore[] newToOld = new DocScore[readerMax];
    FieldSelector fSel = new MapFieldSelector(field);

    for (int oldDoc = 0; oldDoc < readerMax; oldDoc++) {
      float score;
      if (reader.isDeleted(oldDoc)) {
        score = 0.0f;
      } else {
        Document d = reader.document(oldDoc, fSel);
        try {
          score = Float.parseFloat(d.get(field));
        } catch (Exception e) {
          score = 0.0f;
        }
      }
      DocScore docScore = new DocScore();
      docScore.oldDoc = oldDoc;
      docScore.score = score;
      newToOld[oldDoc] = docScore;
    }
    Arrays.sort(newToOld);

    int[] oldToNew = new int[readerMax];
    for (int newDoc = 0; newDoc < readerMax; newDoc++) {
      DocScore docScore = newToOld[newDoc];
      oldToNew[docScore.oldDoc] = newDoc;
    }    
    return oldToNew;
  }

  /** */
  public static void main(String[] args) throws Exception {
    Directory input, output;
    String field;
      
    String usage = "IndexSorter <input> <output> <field>";

    if (args.length < 3) {
      System.err.println("Usage: " + usage);
      System.exit(-1);
    }

    input = FSDirectory.open(new File(args[0]));
    File out = new File(args[1]);
    if (!out.exists()) out.mkdirs();
    output = FSDirectory.open(out);
    field = args[2];
    IndexSorter sorter = new IndexSorter();
    try {
      sorter.sort(input, output, field);
    } catch (Exception e) {
      LOG.warning("IndexSorter: " + e);
    }
  }
}
