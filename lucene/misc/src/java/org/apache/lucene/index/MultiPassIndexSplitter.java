package org.apache.lucene.index;

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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.SuppressForbidden;
import org.apache.lucene.util.Version;

/**
 * This tool splits input index into multiple equal parts. The method employed
 * here uses {@link IndexWriter#addIndexes(CodecReader[])} where the input data
 * comes from the input index with artificially applied deletes to the document
 * id-s that fall outside the selected partition.
 * <p>Note 1: Deletes are only applied to a buffered list of deleted docs and
 * don't affect the source index - this tool works also with read-only indexes.
 * <p>Note 2: the disadvantage of this tool is that source index needs to be
 * read as many times as there are parts to be created, hence the name of this
 * tool.
 *
 * <p><b>NOTE</b>: this tool is unaware of documents added
 * atomically via {@link IndexWriter#addDocuments} or {@link
 * IndexWriter#updateDocuments}, which means it can easily
 * break up such document groups.
 */
@SuppressForbidden(reason = "System.out required: command line tool")
public class MultiPassIndexSplitter {
  
  /**
   * Split source index into multiple parts.
   * @param in source index, can have deletions, can have
   * multiple segments (or multiple readers).
   * @param outputs list of directories where the output parts will be stored.
   * @param seq if true, then the source index will be split into equal
   * increasing ranges of document id-s. If false, source document id-s will be
   * assigned in a deterministic round-robin fashion to one of the output splits.
   * @throws IOException If there is a low-level I/O error
   */
  public void split(IndexReader in, Directory[] outputs, boolean seq) throws IOException {
    if (outputs == null || outputs.length < 2) {
      throw new IOException("Invalid number of outputs.");
    }
    if (in == null || in.numDocs() < 2) {
      throw new IOException("Not enough documents for splitting");
    }
    int numParts = outputs.length;
    // wrap a potentially read-only input
    // this way we don't have to preserve original deletions because neither
    // deleteDocument(int) or undeleteAll() is applied to the wrapped input index.
    FakeDeleteIndexReader input = new FakeDeleteIndexReader(in);
    int maxDoc = input.maxDoc();
    int partLen = maxDoc / numParts;
    for (int i = 0; i < numParts; i++) {
      input.undeleteAll();
      if (seq) { // sequential range
        int lo = partLen * i;
        int hi = lo + partLen;
        // below range
        for (int j = 0; j < lo; j++) {
          input.deleteDocument(j);
        }
        // above range - last part collects all id-s that remained due to
        // integer rounding errors
        if (i < numParts - 1) {
          for (int j = hi; j < maxDoc; j++) {
            input.deleteDocument(j);
          }
        }
      } else {
        // round-robin
        for (int j = 0; j < maxDoc; j++) {
          if ((j + numParts - i) % numParts != 0) {
            input.deleteDocument(j);
          }
        }
      }
      IndexWriter w = new IndexWriter(outputs[i], new IndexWriterConfig(null)
          .setOpenMode(OpenMode.CREATE));
      System.err.println("Writing part " + (i + 1) + " ...");
      // pass the subreaders directly, as our wrapper's numDocs/hasDeletetions are not up-to-date
      final List<? extends FakeDeleteLeafIndexReader> sr = input.getSequentialSubReaders();
      w.addIndexes(sr.toArray(new CodecReader[sr.size()])); // TODO: maybe take List<IR> here?
      w.close();
    }
    System.err.println("Done.");
  }
  
  @SuppressWarnings("deprecation")
  public static void main(String[] args) throws Exception {
    if (args.length < 5) {
      System.err.println("Usage: MultiPassIndexSplitter -out <outputDir> -num <numParts> [-seq] <inputIndex1> [<inputIndex2 ...]");
      System.err.println("\tinputIndex\tpath to input index, multiple values are ok");
      System.err.println("\t-out ouputDir\tpath to output directory to contain partial indexes");
      System.err.println("\t-num numParts\tnumber of parts to produce");
      System.err.println("\t-seq\tsequential docid-range split (default is round-robin)");
      System.exit(-1);
    }
    ArrayList<IndexReader> indexes = new ArrayList<>();
    String outDir = null;
    int numParts = -1;
    boolean seq = false;
    for (int i = 0; i < args.length; i++) {
      if (args[i].equals("-out")) {
        outDir = args[++i];
      } else if (args[i].equals("-num")) {
        numParts = Integer.parseInt(args[++i]);
      } else if (args[i].equals("-seq")) {
        seq = true;
      } else {
        Path file = Paths.get(args[i]);
        if (!Files.isDirectory(file)) {
          System.err.println("Invalid input path - skipping: " + file);
          continue;
        }
        Directory dir = FSDirectory.open(file);
        try {
          if (!DirectoryReader.indexExists(dir)) {
            System.err.println("Invalid input index - skipping: " + file);
            continue;
          }
        } catch (Exception e) {
          System.err.println("Invalid input index - skipping: " + file);
          continue;
        }
        indexes.add(DirectoryReader.open(dir));
      }
    }
    if (outDir == null) {
      throw new Exception("Required argument missing: -out outputDir");
    }
    if (numParts < 2) {
      throw new Exception("Invalid value of required argument: -num numParts");
    }
    if (indexes.size() == 0) {
      throw new Exception("No input indexes to process");
    }
    Path out = Paths.get(outDir);
    Files.createDirectories(out);
    Directory[] dirs = new Directory[numParts];
    for (int i = 0; i < numParts; i++) {
      dirs[i] = FSDirectory.open(out.resolve("part-" + i));
    }
    MultiPassIndexSplitter splitter = new MultiPassIndexSplitter();
    IndexReader input;
    if (indexes.size() == 1) {
      input = indexes.get(0);
    } else {
      input = new MultiReader(indexes.toArray(new IndexReader[indexes.size()]));
    }
    splitter.split(input, dirs, seq);
  }
  
  /**
   * This class emulates deletions on the underlying index.
   */
  private static final class FakeDeleteIndexReader extends BaseCompositeReader<FakeDeleteLeafIndexReader> {

    public FakeDeleteIndexReader(IndexReader reader) throws IOException {
      super(initSubReaders(reader));
    }
    
    private static FakeDeleteLeafIndexReader[] initSubReaders(IndexReader reader) throws IOException {
      final List<LeafReaderContext> leaves = reader.leaves();
      final FakeDeleteLeafIndexReader[] subs = new FakeDeleteLeafIndexReader[leaves.size()];
      int i = 0;
      for (final LeafReaderContext ctx : leaves) {
        subs[i++] = new FakeDeleteLeafIndexReader(SlowCodecReaderWrapper.wrap(ctx.reader()));
      }
      return subs;
    }
        
    public void deleteDocument(int docID) {
      final int i = readerIndex(docID);
      getSequentialSubReaders().get(i).deleteDocument(docID - readerBase(i));
    }

    public void undeleteAll()  {
      for (FakeDeleteLeafIndexReader r : getSequentialSubReaders()) {
        r.undeleteAll();
      }
    }

    @Override
    protected void doClose() {}

    // no need to override numDocs/hasDeletions,
    // as we pass the subreaders directly to IW.addIndexes().
  }
  
  private static final class FakeDeleteLeafIndexReader extends FilterCodecReader {
    FixedBitSet liveDocs;

    public FakeDeleteLeafIndexReader(CodecReader reader) {
      super(reader);
      undeleteAll(); // initialize main bitset
    }

    @Override
    public int numDocs() {
      return liveDocs.cardinality();
    }

    public void undeleteAll()  {
      final int maxDoc = in.maxDoc();
      liveDocs = new FixedBitSet(in.maxDoc());
      if (in.hasDeletions()) {
        final Bits oldLiveDocs = in.getLiveDocs();
        assert oldLiveDocs != null;
        // this loop is a little bit ineffective, as Bits has no nextSetBit():
        for (int i = 0; i < maxDoc; i++) {
          if (oldLiveDocs.get(i)) liveDocs.set(i);
        }
      } else {
        // mark all docs as valid
        liveDocs.set(0, maxDoc);
      }
    }

    public void deleteDocument(int n) {
      liveDocs.clear(n);
    }

    @Override
    public Bits getLiveDocs() {
      return liveDocs;
    }
  }
}
