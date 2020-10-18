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

package org.apache.lucene.util.hnsw;

import org.apache.lucene.codecs.lucene90.Lucene90VectorReader;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.VectorField;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.KnnGraphValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.VectorValues;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.PrintStreamInfoStream;
import org.apache.lucene.util.SuppressForbidden;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.FloatBuffer;
import java.nio.IntBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Locale;
import java.util.Random;
import java.util.Set;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

/** For testing indexing and search performance of a knn-graph
 * 
 * java -cp .../lib/*.jar org.apache.lucene.util.hnsw.KnnGraphTester -ndoc 1000000 -search .../vectors.bin
*/
public class KnnGraphTester {

  private final static String KNN_FIELD = "knn";
  private final static String ID_FIELD = "id";
  private final static VectorValues.ScoreFunction scoreFunction = VectorValues.ScoreFunction.DOT_PRODUCT;

  private Random random;
  private int numDocs;
  private int dim;
  private int topK;
  private int fanout;
  private int numIters;
  private Path indexPath;
  private boolean reindex;

  @SuppressForbidden(reason="uses Random()")
  private KnnGraphTester() {
    // set defaults
    numDocs = 1000;
    numIters = 1000;
    dim = 256;
    topK = 100;
    fanout = 50;
    random = new Random();
    indexPath = Paths.get("knn_test_index");
  }

  public static void main(String... args) throws Exception {
    new KnnGraphTester().run(args);
  }

  private void run(String... args) throws Exception {
    String operation = null, docVectorsPath = null, queryPath = null;
    for (int iarg = 0; iarg < args.length; iarg++) {
      String arg = args[iarg];
      switch(arg) {
        case "-generate":
        case "-search":
        case "-check":
        case "-stats":
          if (operation != null) {
            throw new IllegalArgumentException("Specify only one operation, not both " + arg + " and " + operation);
          }
          if (iarg == args.length - 1) {
            throw new IllegalArgumentException("Operation " + arg + " requires a following pathname");
          }
          operation = arg;
          docVectorsPath = args[++iarg];
          if (operation.equals("-search")) {
            queryPath = args[++iarg];
          }
          break;
        case "-fanout":
          if (iarg == args.length - 1) {
            throw new IllegalArgumentException("-fanout requires a following number");
          }
          fanout = Integer.parseInt(args[++iarg]);
          break;
        case "-ndoc":
          if (iarg == args.length - 1) {
            throw new IllegalArgumentException("-ndoc requires a following number");
          }
          numDocs = Integer.parseInt(args[++iarg]);
          break;
        case "-niter":
          if (iarg == args.length - 1) {
            throw new IllegalArgumentException("-niter requires a following number");
          }
          numIters = Integer.parseInt(args[++iarg]);
          break;
        case "-reindex":
          reindex = true;
          break;
        case "-forceMerge":
          operation = "-forceMerge";
          break;
        default:
          throw new IllegalArgumentException("unknown operation " + operation);
          //usage();
      }
    }
    if (operation == null) {
      usage();
    }
    if (reindex) {
      createIndex(Paths.get(docVectorsPath), indexPath);
    }
    switch (operation) {
      case "-search":
        testSearch(indexPath, Paths.get(queryPath), getNN(Paths.get(docVectorsPath), Paths.get(queryPath)));
        break;
      case "-forceMerge":
        forceMerge();
        break;
      case "-check":
        throw new UnsupportedOperationException("check TBD");
        //checkIndex(indexPath);
        //break;
      case "-stats":
        printFanoutHist(indexPath);
        break;
    }
  }

  @SuppressForbidden(reason="Prints stuff")
  private void printFanoutHist(Path indexPath) throws IOException {
    try (Directory dir = FSDirectory.open(indexPath);
         DirectoryReader reader = DirectoryReader.open(dir)) {
      // int[] globalHist = new int[reader.maxDoc()];
      for (LeafReaderContext context : reader.leaves()) {
        LeafReader leafReader = context.reader();
        KnnGraphValues knnValues = ((Lucene90VectorReader) ((CodecReader) leafReader).getVectorReader()).getGraphValues(KNN_FIELD);
        System.out.printf("Leaf %d has %d documents\n", context.ord, leafReader.maxDoc());
        printGraphFanout(knnValues, leafReader.maxDoc());
      }
    }
  }

  @SuppressForbidden(reason="Prints stuff")
  private void forceMerge() throws IOException {
    IndexWriterConfig iwc = new IndexWriterConfig()
      .setOpenMode(IndexWriterConfig.OpenMode.APPEND);
    iwc.setInfoStream(new PrintStreamInfoStream(System.out));
    System.out.println("Force merge index in " + indexPath);
    try (IndexWriter iw = new IndexWriter(FSDirectory.open(indexPath), iwc)) {
      iw.forceMerge(1);
    }
  }

  @SuppressForbidden(reason="Prints stuff")
  private void printGraphFanout(KnnGraphValues knnValues, int numDocs) throws IOException {
    int min = Integer.MAX_VALUE, max = 0, total = 0;
    int count = 0;
    int[] leafHist = new int[numDocs];
    for (int node = 0; node < numDocs; node++) {
      knnValues.seek(node);
      int n = 0, arc;
      while ((arc = knnValues.nextArc()) != NO_MORE_DOCS) {
        ++n;
      }
      ++leafHist[n];
      max = Math.max(max, n);
      min = Math.min(min, n);
      if (n > 0) {
        ++count;
        total += n;
      }
    }
    System.out.printf("Graph size=%d, Fanout min=%d, mean=%.2f, max=%d\n", count, min, total / (float) count, max);
    printHist(leafHist, max, count, 10);
  }

  @SuppressForbidden(reason="Prints stuff")
  private void printHist(int[] hist, int max, int count, int nbuckets) {
    System.out.print("%");
    for (int i=0; i <= nbuckets; i ++) {
      System.out.printf("%4d", i * 100 / nbuckets);
    }
    System.out.printf("\n %4d", hist[0]);
    int total = 0, ibucket = 1;
    for (int i = 1; i <= max && ibucket <= nbuckets; i++) {
      total += hist[i];
      if (total >= count * ibucket / nbuckets) {
        System.out.printf("%4d", i);
        ++ibucket;
      }
    }
    System.out.println("");
  }

  /*
  private void checkIndex(Path indexPath, int numIters) throws IOException {
    float[][] targets = new float[numIters][];
    TopDocs[] results = new TopDocs[numIters];
    System.out.println("checking " + numIters + " documents");
    try (Directory dir = FSDirectory.open(indexPath);
         DirectoryReader reader = DirectoryReader.open(dir)) {
      for (int i = 0; i < numIters; i++) {
        targets[i] = new float[dim];
        int docid = random.nextInt(reader.maxDoc());
        int readerOrd = ReaderUtil.subIndex(docid, reader.leaves());
        LeafReaderContext ctx = reader.leaves().get(readerOrd);
        int leafDocid = docid - ctx.docBase;
        KnnGraphValues graphValues = ((Lucene90KnnGraphReader) ((CodecReader) ctx.reader()).getKnnGraphReader()).getGraphValues(KNN_FIELD);
        VectorValues vectorValues = ctx.reader().getVectorValues(KNN_FIELD);
        boolean ok = graphValues.advanceExact(leafDocid);
        if (ok == false) {
          throw new IllegalStateException("KnnGraphValues failed to advance to docid=" + docid);
        }
        int[] friends = graphValues.getFriends(0);
        // check all the supposed friends for overlap with the true list of closest documents
        int advanced = vectorValues.advance(leafDocid);
        if (advanced != leafDocid) {
          throw new IllegalStateException("VectorValues failed to advance to docid=" + docid
                                          + "; in leaf=" + readerOrd + " having maxDoc=" + ctx.reader().maxDoc()
                                          + ", advance(" + leafDocid + ") returned " + advanced);
        }
        System.arraycopy(vectorValues.vectorValue(), 0, targets[i], 0, dim);
        results[i] = new TopDocs(null, new ScoreDoc[friends.length]);
        for (int j = 0; j < friends.length; j++) {
          results[i].scoreDocs[j] = new ScoreDoc(ctx.docBase + friends[j], 0);
        }
      }
    }
    checkResults(targets, results);
  }
  */

  @SuppressForbidden(reason="Prints stuff")
  private void testSearch(Path indexPath, Path queryPath, int[][] nn) throws IOException {
    TopDocs[] results = new TopDocs[numIters];
    try (FileChannel q = FileChannel.open(queryPath)) {
      FloatBuffer targets = q.map(FileChannel.MapMode.READ_ONLY, 0, numIters * dim * Float.BYTES)
        .order(ByteOrder.LITTLE_ENDIAN)
        .asFloatBuffer();
      float[] target = new float[dim];
      System.out.println("running " + numIters + " targets; topK=" + topK + ", fanout=" + fanout);
      long start;
      ThreadMXBean bean = ManagementFactory.getThreadMXBean();
      long cpuTimeStartNs;
      long elapsed, totalCpuTime;
      try (Directory dir = FSDirectory.open(indexPath);
           DirectoryReader reader = DirectoryReader.open(dir)) {

        for (int i = 0; i < 1000; i++) {
          // warm up
          targets.get(target);
          results[i] = doKnnSearch(reader, KNN_FIELD, target, topK, fanout);
        }
        targets.position(0);
        start = System.nanoTime();
        cpuTimeStartNs = bean.getCurrentThreadCpuTime();
        for (int i = 0; i < numIters; i++) {
          targets.get(target);
          results[i] = doKnnSearch(reader, KNN_FIELD, target, topK, fanout);
        }
        totalCpuTime = (bean.getCurrentThreadCpuTime() - cpuTimeStartNs) / 1_000_000;
        elapsed = (System.nanoTime() - start) / 1_000_000; // ns -> ms
          for (int i = 0; i < numIters; i++) {
              for (ScoreDoc doc : results[i].scoreDocs) {
                  doc.doc = Integer.parseInt(reader.document(doc.doc).get("id"));
              }
          }
      }
      System.out.println("completed " + numIters + " searches in " + elapsed + " ms: " + ((1000 * numIters) / elapsed) + " QPS "
      + "CPU time=" + totalCpuTime + "ms");
    }
    System.out.println("checking results");
    checkResults(results, nn);
  }

  private static TopDocs doKnnSearch(IndexReader reader, String field, float[] vector, int k, int fanout) throws IOException {
    TopDocs[] results = new TopDocs[reader.leaves().size()];
    for (LeafReaderContext ctx: reader.leaves()) {
      results[ctx.ord] = ctx.reader().getVectorValues(field).randomAccess().search(vector, k, fanout);
      int docBase = ctx.docBase;
      for (ScoreDoc scoreDoc : results[ctx.ord].scoreDocs) {
        scoreDoc.doc += docBase;
      }
    }
    return TopDocs.merge(k, results);
  }

  private void checkResults(TopDocs[] results, int[][] nn) {
    int totalMatches = 0;
    int totalResults = 0;
    for (int i = 0; i < results.length; i++) {
      int n = results[i].scoreDocs.length;
      totalResults += n;
      //System.out.println(Arrays.toString(nn[i]));
      //System.out.println(Arrays.toString(results[i].scoreDocs));
      totalMatches += compareNN(nn[i], results[i]);
    }
    System.out.println("total matches = " + totalMatches + " out of " + totalResults);
    System.out.printf(Locale.ROOT, "Average overlap = %.2f%%\n", ((100.0 * totalMatches) / totalResults));
  }

  int compareNN(int[] expected, TopDocs results) {
    int matched = 0;
    /*
    System.out.print("expected=");
    for (int j = 0; j < expected.length; j++) {
      System.out.print(expected[j]);
      System.out.print(", ");
    }
    System.out.print('\n');
    System.out.println("results=");
    for (int j = 0; j < results.scoreDocs.length; j++) {
      System.out.print("" + results.scoreDocs[j].doc + ":" + results.scoreDocs[j].score + ", ");
    }
    System.out.print('\n');
    */
    Set<Integer> expectedSet = new HashSet<>();
    for (int i = 0; i < results.scoreDocs.length; i++) {
      expectedSet.add(expected[i]);
    }
    for (ScoreDoc scoreDoc : results.scoreDocs) {
      if (expectedSet.contains(scoreDoc.doc)) {
        ++matched;
      }
    }
    return matched;
  }

  int[][] getNN(Path docPath, Path queryPath) throws IOException {
    // look in working directory for cached nn file
    String nnFileName = "nn-" + numDocs + "-" + numIters + "-" + topK + ".bin";
    Path nnPath = Paths.get(nnFileName);
    if (Files.exists(nnPath)) {
      return readNN(nnPath);
    } else {
      int[][] nn = computeNN(docPath, queryPath);
      writeNN(nn, nnPath);
      return nn;
    }
  }

  int[][] readNN(Path nnPath) throws IOException {
    int[][] result = new int[numIters][];
    try (FileChannel in = FileChannel.open(nnPath)) {
      IntBuffer intBuffer = in.map(FileChannel.MapMode.READ_ONLY, 0, numIters * topK * Integer.BYTES)
              .order(ByteOrder.LITTLE_ENDIAN)
              .asIntBuffer();
      for (int i = 0; i < numIters; i++) {
        result[i] = new int[topK];
        intBuffer.get(result[i]);
      }
    }
    return result;
  }

  void writeNN(int[][] nn, Path nnPath) throws IOException {
    System.out.println("writing true nearest neighbors to " + nnPath);
    ByteBuffer tmp = ByteBuffer.allocate(nn[0].length * Integer.BYTES).order(ByteOrder.LITTLE_ENDIAN);
    try (OutputStream out = Files.newOutputStream(nnPath)) {
      for (int i = 0; i < numIters; i++) {
        tmp.asIntBuffer().put(nn[i]);
        out.write(tmp.array());
      }
    }
  }

  int[][] computeNN(Path docPath, Path queryPath) throws IOException {
    int[][] result = new int[numIters][];
    System.out.println("computing true nearest neighbors of " + numIters + " target vectors");
    try (FileChannel in = FileChannel.open(docPath);
         FileChannel qIn = FileChannel.open(queryPath)) {
      FloatBuffer queries = qIn.map(FileChannel.MapMode.READ_ONLY, 0, numIters * dim * Float.BYTES)
        .order(ByteOrder.LITTLE_ENDIAN)
        .asFloatBuffer();
      float[] vector = new float[dim];
      float[] query = new float[dim];
      for (int i = 0; i < numIters; i++) {
        queries.get(query);
        long totalBytes = (long) numDocs * dim * Float.BYTES;
        int blockSize = (int) Math.min(totalBytes, (Integer.MAX_VALUE / (dim * Float.BYTES)) * (dim * Float.BYTES)), offset = 0;
        int j = 0;
        //System.out.println("totalBytes=" + totalBytes);
        while (j < numDocs) {
          FloatBuffer vectors = in.map(FileChannel.MapMode.READ_ONLY, offset, blockSize)
            .order(ByteOrder.LITTLE_ENDIAN)
            .asFloatBuffer();
          offset += blockSize;
          Neighbors queue = Neighbors.create(topK, scoreFunction.reversed);
          for (; j < numDocs && vectors.hasRemaining(); j++) {
            vectors.get(vector);
            float d = VectorValues.compare(query, vector, scoreFunction);
            queue.insertWithOverflow(new Neighbor(j, d));
          }
          result[i] = new int[topK];
          for (int k = topK - 1; k >= 0; k--) {
            Neighbor n = queue.pop();
            result[i][k] = n.node;
            //System.out.print(" " + n);
          }
          if ((i + 1) % 10 == 0) {
            System.out.print(" " + (i + 1));
            System.out.flush();
          }
        }
      }
    }
    return result;
  }

  private void createIndex(Path docsPath, Path indexPath) throws IOException {
    IndexWriterConfig iwc = new IndexWriterConfig()
      .setOpenMode(IndexWriterConfig.OpenMode.CREATE);
    // iwc.setMergePolicy(NoMergePolicy.INSTANCE);
    iwc.setRAMBufferSizeMB(1994d);
    iwc.setInfoStream(new PrintStreamInfoStream(System.out));
    System.out.println("creating index in " + indexPath);
    long start = System.nanoTime();
    long totalBytes = (long) numDocs * dim * Float.BYTES, offset = 0;
    try (FSDirectory dir = FSDirectory.open(indexPath); IndexWriter iw = new IndexWriter(dir, iwc)) {
      int blockSize = (int) Math.min(totalBytes, (Integer.MAX_VALUE / (dim * Float.BYTES)) * (dim * Float.BYTES));
      float[] vector = new float[dim];
      try (FileChannel in = FileChannel.open(docsPath)) {
        int i = 0;
        while (i < numDocs) {
          FloatBuffer vectors = in.map(FileChannel.MapMode.READ_ONLY, offset, blockSize)
            .order(ByteOrder.LITTLE_ENDIAN)
            .asFloatBuffer();
          offset += blockSize;
          for (; vectors.hasRemaining() && i < numDocs ; i++) {
            vectors.get(vector);
            Document doc = new Document();
            //System.out.println("vector=" + vector[0] + "," + vector[1] + "...");
            doc.add(new VectorField(KNN_FIELD, vector, VectorValues.ScoreFunction.DOT_PRODUCT));
            doc.add(new StoredField(ID_FIELD, i));
            iw.addDocument(doc);
          }
        }
        System.out.println("Done indexing " + numDocs + " documents; now flush");
      }
      long elapsed = System.nanoTime() - start;
      System.out.println("Indexed " + numDocs + " documents in " + elapsed / 1_000_000_000 + "s");
    }
  }

  private static void usage() {
    String error = "Usage: TestKnnGraph -generate|-search|-stats|-check {datafile} [-fanout N]";
    System.err.println(error);
    System.exit(1);
  }

}
