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

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

import java.io.Closeable;
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
import java.util.Set;
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
import org.apache.lucene.index.RandomAccessVectorValues;
import org.apache.lucene.index.RandomAccessVectorValuesProducer;
import org.apache.lucene.index.VectorValues;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IntroSorter;
import org.apache.lucene.util.PrintStreamInfoStream;
import org.apache.lucene.util.SuppressForbidden;

/**
 * For testing indexing and search performance of a knn-graph
 *
 * <p>java -cp .../lib/*.jar org.apache.lucene.util.hnsw.KnnGraphTester -ndoc 1000000 -search
 * .../vectors.bin
 */
public class KnnGraphTester {

  private static final String KNN_FIELD = "knn";
  private static final String ID_FIELD = "id";
  private static final VectorValues.SearchStrategy SEARCH_STRATEGY =
      VectorValues.SearchStrategy.DOT_PRODUCT_HNSW;

  private int numDocs;
  private int dim;
  private int topK;
  private int warmCount;
  private int numIters;
  private int fanout;
  private Path indexPath;
  private boolean quiet;
  private boolean reindex;
  private int reindexTimeMsec;

  @SuppressForbidden(reason = "uses Random()")
  private KnnGraphTester() {
    // set defaults
    numDocs = 1000;
    numIters = 1000;
    dim = 256;
    topK = 100;
    warmCount = 1000;
    fanout = topK;
  }

  public static void main(String... args) throws Exception {
    new KnnGraphTester().run(args);
  }

  private void run(String... args) throws Exception {
    String operation = null;
    Path docVectorsPath = null, queryPath = null, outputPath = null;
    for (int iarg = 0; iarg < args.length; iarg++) {
      String arg = args[iarg];
      switch (arg) {
        case "-search":
        case "-check":
        case "-stats":
        case "-dump":
          if (operation != null) {
            throw new IllegalArgumentException(
                "Specify only one operation, not both " + arg + " and " + operation);
          }
          operation = arg;
          if (operation.equals("-search")) {
            if (iarg == args.length - 1) {
              throw new IllegalArgumentException(
                  "Operation " + arg + " requires a following pathname");
            }
            queryPath = Paths.get(args[++iarg]);
          }
          break;
        case "-fanout":
          if (iarg == args.length - 1) {
            throw new IllegalArgumentException("-fanout requires a following number");
          }
          fanout = Integer.parseInt(args[++iarg]);
          break;
        case "-beamWidthIndex":
          if (iarg == args.length - 1) {
            throw new IllegalArgumentException("-beamWidthIndex requires a following number");
          }
          HnswGraphBuilder.DEFAULT_BEAM_WIDTH = Integer.parseInt(args[++iarg]);
          break;
        case "-maxConn":
          if (iarg == args.length - 1) {
            throw new IllegalArgumentException("-maxConn requires a following number");
          }
          HnswGraphBuilder.DEFAULT_MAX_CONN = Integer.parseInt(args[++iarg]);
          break;
        case "-dim":
          if (iarg == args.length - 1) {
            throw new IllegalArgumentException("-dim requires a following number");
          }
          dim = Integer.parseInt(args[++iarg]);
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
        case "-topK":
          if (iarg == args.length - 1) {
            throw new IllegalArgumentException("-topK requires a following number");
          }
          topK = Integer.parseInt(args[++iarg]);
          break;
        case "-out":
          outputPath = Paths.get(args[++iarg]);
          break;
        case "-warm":
          warmCount = Integer.parseInt(args[++iarg]);
          break;
        case "-docs":
          docVectorsPath = Paths.get(args[++iarg]);
          break;
        case "-forceMerge":
          operation = "-forceMerge";
          break;
        case "-quiet":
          quiet = true;
          break;
        default:
          throw new IllegalArgumentException("unknown argument " + arg);
          // usage();
      }
    }
    if (operation == null && reindex == false) {
      usage();
    }
    indexPath = Paths.get(formatIndexPath(docVectorsPath));
    if (reindex) {
      if (docVectorsPath == null) {
        throw new IllegalArgumentException("-docs argument is required when indexing");
      }
      reindexTimeMsec = createIndex(docVectorsPath, indexPath);
    }
    if (operation != null) {
      switch (operation) {
        case "-search":
          if (docVectorsPath == null) {
            throw new IllegalArgumentException("missing -docs arg");
          }
          if (outputPath != null) {
            testSearch(indexPath, queryPath, outputPath, null);
          } else {
            testSearch(indexPath, queryPath, null, getNN(docVectorsPath, queryPath));
          }
          break;
        case "-forceMerge":
          forceMerge();
          break;
        case "-dump":
          dumpGraph(docVectorsPath);
          break;
        case "-stats":
          printFanoutHist(indexPath);
          break;
      }
    }
  }

  private String formatIndexPath(Path docsPath) {
    return docsPath.getFileName()
        + "-"
        + HnswGraphBuilder.DEFAULT_MAX_CONN
        + "-"
        + HnswGraphBuilder.DEFAULT_BEAM_WIDTH
        + ".index";
  }

  @SuppressForbidden(reason = "Prints stuff")
  private void printFanoutHist(Path indexPath) throws IOException {
    try (Directory dir = FSDirectory.open(indexPath);
        DirectoryReader reader = DirectoryReader.open(dir)) {
      // int[] globalHist = new int[reader.maxDoc()];
      for (LeafReaderContext context : reader.leaves()) {
        LeafReader leafReader = context.reader();
        KnnGraphValues knnValues =
            ((Lucene90VectorReader) ((CodecReader) leafReader).getVectorReader())
                .getGraphValues(KNN_FIELD);
        System.out.printf("Leaf %d has %d documents\n", context.ord, leafReader.maxDoc());
        printGraphFanout(knnValues, leafReader.maxDoc());
      }
    }
  }

  private void dumpGraph(Path docsPath) throws IOException {
    try (BinaryFileVectors vectors = new BinaryFileVectors(docsPath)) {
      RandomAccessVectorValues values = vectors.randomAccess();
      HnswGraphBuilder builder =
          new HnswGraphBuilder(
              vectors, HnswGraphBuilder.DEFAULT_MAX_CONN, HnswGraphBuilder.DEFAULT_BEAM_WIDTH, 0);
      // start at node 1
      for (int i = 1; i < numDocs; i++) {
        builder.addGraphNode(values.vectorValue(i));
        System.out.println("\nITERATION " + i);
        dumpGraph(builder.hnsw);
      }
    }
  }

  private void dumpGraph(HnswGraph hnsw) {
    for (int i = 0; i < hnsw.size(); i++) {
      NeighborArray neighbors = hnsw.getNeighbors(i);
      System.out.printf(Locale.ROOT, "%5d", i);
      NeighborArray sorted = new NeighborArray(neighbors.size());
      for (int j = 0; j < neighbors.size(); j++) {
        int node = neighbors.node[j];
        float score = neighbors.score[j];
        sorted.add(node, score);
      }
      new NeighborArraySorter(sorted).sort(0, sorted.size());
      for (int j = 0; j < sorted.size(); j++) {
        System.out.printf(Locale.ROOT, " [%d, %.4f]", sorted.node[j], sorted.score[j]);
      }
      System.out.println();
    }
  }

  @SuppressForbidden(reason = "Prints stuff")
  private void forceMerge() throws IOException {
    IndexWriterConfig iwc = new IndexWriterConfig().setOpenMode(IndexWriterConfig.OpenMode.APPEND);
    iwc.setInfoStream(new PrintStreamInfoStream(System.out));
    System.out.println("Force merge index in " + indexPath);
    try (IndexWriter iw = new IndexWriter(FSDirectory.open(indexPath), iwc)) {
      iw.forceMerge(1);
    }
  }

  @SuppressForbidden(reason = "Prints stuff")
  private void printGraphFanout(KnnGraphValues knnValues, int numDocs) throws IOException {
    int min = Integer.MAX_VALUE, max = 0, total = 0;
    int count = 0;
    int[] leafHist = new int[numDocs];
    for (int node = 0; node < numDocs; node++) {
      knnValues.seek(node);
      int n = 0;
      while (knnValues.nextNeighbor() != NO_MORE_DOCS) {
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
    System.out.printf(
        "Graph size=%d, Fanout min=%d, mean=%.2f, max=%d\n",
        count, min, total / (float) count, max);
    printHist(leafHist, max, count, 10);
  }

  @SuppressForbidden(reason = "Prints stuff")
  private void printHist(int[] hist, int max, int count, int nbuckets) {
    System.out.print("%");
    for (int i = 0; i <= nbuckets; i++) {
      System.out.printf("%4d", i * 100 / nbuckets);
    }
    System.out.printf("\n %4d", hist[0]);
    int total = 0, ibucket = 1;
    for (int i = 1; i <= max && ibucket <= nbuckets; i++) {
      total += hist[i];
      while (total >= count * ibucket / nbuckets) {
        System.out.printf("%4d", i);
        ++ibucket;
      }
    }
    System.out.println();
  }

  @SuppressForbidden(reason = "Prints stuff")
  private void testSearch(Path indexPath, Path queryPath, Path outputPath, int[][] nn)
      throws IOException {
    TopDocs[] results = new TopDocs[numIters];
    long elapsed, totalCpuTime, totalVisited = 0;
    try (FileChannel q = FileChannel.open(queryPath)) {
      FloatBuffer targets =
          q.map(FileChannel.MapMode.READ_ONLY, 0, numIters * dim * Float.BYTES)
              .order(ByteOrder.LITTLE_ENDIAN)
              .asFloatBuffer();
      float[] target = new float[dim];
      if (quiet == false) {
        System.out.println("running " + numIters + " targets; topK=" + topK + ", fanout=" + fanout);
      }
      long start;
      ThreadMXBean bean = ManagementFactory.getThreadMXBean();
      long cpuTimeStartNs;
      try (Directory dir = FSDirectory.open(indexPath);
          DirectoryReader reader = DirectoryReader.open(dir)) {
        numDocs = reader.maxDoc();
        for (int i = 0; i < warmCount; i++) {
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
          totalVisited += results[i].totalHits.value;
          for (ScoreDoc doc : results[i].scoreDocs) {
            doc.doc = Integer.parseInt(reader.document(doc.doc).get("id"));
          }
        }
      }
      if (quiet == false) {
        System.out.println(
            "completed "
                + numIters
                + " searches in "
                + elapsed
                + " ms: "
                + ((1000 * numIters) / elapsed)
                + " QPS "
                + "CPU time="
                + totalCpuTime
                + "ms");
      }
    }
    if (outputPath != null) {
      ByteBuffer buf = ByteBuffer.allocate(4);
      IntBuffer ibuf = buf.order(ByteOrder.LITTLE_ENDIAN).asIntBuffer();
      try (OutputStream out = Files.newOutputStream(outputPath)) {
        for (int i = 0; i < numIters; i++) {
          for (ScoreDoc doc : results[i].scoreDocs) {
            ibuf.position(0);
            ibuf.put(doc.doc);
            out.write(buf.array());
          }
        }
      }
    } else {
      if (quiet == false) {
        System.out.println("checking results");
      }
      float recall = checkResults(results, nn);
      totalVisited /= numIters;
      if (quiet) {
        System.out.printf(
            Locale.ROOT,
            "%5.3f\t%5.2f\t%d\t%d\t%d\t%d\t%d\t%d\n",
            recall,
            totalCpuTime / (float) numIters,
            numDocs,
            fanout,
            HnswGraphBuilder.DEFAULT_MAX_CONN,
            HnswGraphBuilder.DEFAULT_BEAM_WIDTH,
            totalVisited,
            reindexTimeMsec);
      }
    }
  }

  private static TopDocs doKnnSearch(
      IndexReader reader, String field, float[] vector, int k, int fanout) throws IOException {
    TopDocs[] results = new TopDocs[reader.leaves().size()];
    for (LeafReaderContext ctx : reader.leaves()) {
      results[ctx.ord] = ctx.reader().getVectorValues(field).search(vector, k, fanout);
      int docBase = ctx.docBase;
      for (ScoreDoc scoreDoc : results[ctx.ord].scoreDocs) {
        scoreDoc.doc += docBase;
      }
    }
    return TopDocs.merge(k, results);
  }

  private float checkResults(TopDocs[] results, int[][] nn) {
    int totalMatches = 0;
    int totalResults = 0;
    for (int i = 0; i < results.length; i++) {
      int n = results[i].scoreDocs.length;
      totalResults += n;
      // System.out.println(Arrays.toString(nn[i]));
      // System.out.println(Arrays.toString(results[i].scoreDocs));
      totalMatches += compareNN(nn[i], results[i]);
    }
    if (quiet == false) {
      System.out.println("total matches = " + totalMatches + " out of " + totalResults);
      System.out.printf(
          Locale.ROOT, "Average overlap = %.2f%%\n", ((100.0 * totalMatches) / totalResults));
    }
    return totalMatches / (float) totalResults;
  }

  private int compareNN(int[] expected, TopDocs results) {
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

  private int[][] getNN(Path docPath, Path queryPath) throws IOException {
    // look in working directory for cached nn file
    String nnFileName = "nn-" + numDocs + "-" + numIters + "-" + topK + "-" + dim + ".bin";
    Path nnPath = Paths.get(nnFileName);
    if (Files.exists(nnPath)) {
      return readNN(nnPath);
    } else {
      int[][] nn = computeNN(docPath, queryPath);
      writeNN(nn, nnPath);
      return nn;
    }
  }

  private int[][] readNN(Path nnPath) throws IOException {
    int[][] result = new int[numIters][];
    try (FileChannel in = FileChannel.open(nnPath)) {
      IntBuffer intBuffer =
          in.map(FileChannel.MapMode.READ_ONLY, 0, numIters * topK * Integer.BYTES)
              .order(ByteOrder.LITTLE_ENDIAN)
              .asIntBuffer();
      for (int i = 0; i < numIters; i++) {
        result[i] = new int[topK];
        intBuffer.get(result[i]);
      }
    }
    return result;
  }

  private void writeNN(int[][] nn, Path nnPath) throws IOException {
    if (quiet == false) {
      System.out.println("writing true nearest neighbors to " + nnPath);
    }
    ByteBuffer tmp =
        ByteBuffer.allocate(nn[0].length * Integer.BYTES).order(ByteOrder.LITTLE_ENDIAN);
    try (OutputStream out = Files.newOutputStream(nnPath)) {
      for (int i = 0; i < numIters; i++) {
        tmp.asIntBuffer().put(nn[i]);
        out.write(tmp.array());
      }
    }
  }

  private int[][] computeNN(Path docPath, Path queryPath) throws IOException {
    int[][] result = new int[numIters][];
    if (quiet == false) {
      System.out.println("computing true nearest neighbors of " + numIters + " target vectors");
    }
    try (FileChannel in = FileChannel.open(docPath);
        FileChannel qIn = FileChannel.open(queryPath)) {
      FloatBuffer queries =
          qIn.map(FileChannel.MapMode.READ_ONLY, 0, numIters * dim * Float.BYTES)
              .order(ByteOrder.LITTLE_ENDIAN)
              .asFloatBuffer();
      float[] vector = new float[dim];
      float[] query = new float[dim];
      for (int i = 0; i < numIters; i++) {
        queries.get(query);
        long totalBytes = (long) numDocs * dim * Float.BYTES;
        int
            blockSize =
                (int)
                    Math.min(
                        totalBytes,
                        (Integer.MAX_VALUE / (dim * Float.BYTES)) * (dim * Float.BYTES)),
            offset = 0;
        int j = 0;
        // System.out.println("totalBytes=" + totalBytes);
        while (j < numDocs) {
          FloatBuffer vectors =
              in.map(FileChannel.MapMode.READ_ONLY, offset, blockSize)
                  .order(ByteOrder.LITTLE_ENDIAN)
                  .asFloatBuffer();
          offset += blockSize;
          NeighborQueue queue = new NeighborQueue(topK, SEARCH_STRATEGY.reversed);
          for (; j < numDocs && vectors.hasRemaining(); j++) {
            vectors.get(vector);
            float d = SEARCH_STRATEGY.compare(query, vector);
            queue.insertWithOverflow(j, d);
          }
          result[i] = new int[topK];
          for (int k = topK - 1; k >= 0; k--) {
            result[i][k] = queue.topNode();
            queue.pop();
            // System.out.print(" " + n);
          }
          if (quiet == false && (i + 1) % 10 == 0) {
            System.out.print(" " + (i + 1));
            System.out.flush();
          }
        }
      }
    }
    return result;
  }

  private int createIndex(Path docsPath, Path indexPath) throws IOException {
    IndexWriterConfig iwc = new IndexWriterConfig().setOpenMode(IndexWriterConfig.OpenMode.CREATE);
    // iwc.setMergePolicy(NoMergePolicy.INSTANCE);
    iwc.setRAMBufferSizeMB(1994d);
    if (quiet == false) {
      iwc.setInfoStream(new PrintStreamInfoStream(System.out));
      System.out.println("creating index in " + indexPath);
    }
    long start = System.nanoTime();
    long totalBytes = (long) numDocs * dim * Float.BYTES, offset = 0;
    try (FSDirectory dir = FSDirectory.open(indexPath);
        IndexWriter iw = new IndexWriter(dir, iwc)) {
      int blockSize =
          (int)
              Math.min(totalBytes, (Integer.MAX_VALUE / (dim * Float.BYTES)) * (dim * Float.BYTES));
      float[] vector = new float[dim];
      try (FileChannel in = FileChannel.open(docsPath)) {
        int i = 0;
        while (i < numDocs) {
          FloatBuffer vectors =
              in.map(FileChannel.MapMode.READ_ONLY, offset, blockSize)
                  .order(ByteOrder.LITTLE_ENDIAN)
                  .asFloatBuffer();
          offset += blockSize;
          for (; vectors.hasRemaining() && i < numDocs; i++) {
            vectors.get(vector);
            Document doc = new Document();
            // System.out.println("vector=" + vector[0] + "," + vector[1] + "...");
            doc.add(
                new VectorField(KNN_FIELD, vector, VectorValues.SearchStrategy.DOT_PRODUCT_HNSW));
            doc.add(new StoredField(ID_FIELD, i));
            iw.addDocument(doc);
          }
        }
        if (quiet == false) {
          System.out.println("Done indexing " + numDocs + " documents; now flush");
        }
      }
    }
    long elapsed = System.nanoTime() - start;
    if (quiet == false) {
      System.out.println("Indexed " + numDocs + " documents in " + elapsed / 1_000_000_000 + "s");
    }
    return (int) (elapsed / 1_000_000);
  }

  private static void usage() {
    String error =
        "Usage: TestKnnGraph [-reindex] [-search {queryfile}|-stats|-check] [-docs {datafile}] [-niter N] [-fanout N] [-maxConn N] [-beamWidth N]";
    System.err.println(error);
    System.exit(1);
  }

  class BinaryFileVectors implements RandomAccessVectorValuesProducer, Closeable {

    private final int size;
    private final FileChannel in;
    private final FloatBuffer mmap;

    BinaryFileVectors(Path filePath) throws IOException {
      in = FileChannel.open(filePath);
      long totalBytes = (long) numDocs * dim * Float.BYTES;
      if (totalBytes > Integer.MAX_VALUE) {
        throw new IllegalArgumentException("input over 2GB not supported");
      }
      int vectorByteSize = dim * Float.BYTES;
      size = (int) (totalBytes / vectorByteSize);
      mmap =
          in.map(FileChannel.MapMode.READ_ONLY, 0, totalBytes)
              .order(ByteOrder.LITTLE_ENDIAN)
              .asFloatBuffer();
    }

    @Override
    public void close() throws IOException {
      in.close();
    }

    @Override
    public RandomAccessVectorValues randomAccess() {
      return new Values();
    }

    class Values implements RandomAccessVectorValues {

      float[] vector = new float[dim];
      FloatBuffer source = mmap.slice();

      @Override
      public int size() {
        return size;
      }

      @Override
      public int dimension() {
        return dim;
      }

      @Override
      public VectorValues.SearchStrategy searchStrategy() {
        return SEARCH_STRATEGY;
      }

      @Override
      public float[] vectorValue(int targetOrd) {
        int pos = targetOrd * dim;
        source.position(pos);
        source.get(vector);
        return vector;
      }

      @Override
      public BytesRef binaryValue(int targetOrd) {
        throw new UnsupportedOperationException();
      }
    }
  }

  static class NeighborArraySorter extends IntroSorter {
    private final int[] node;
    private final float[] score;

    NeighborArraySorter(NeighborArray neighbors) {
      node = neighbors.node;
      score = neighbors.score;
    }

    int pivot;

    @Override
    protected void swap(int i, int j) {
      int tmpNode = node[i];
      float tmpScore = score[i];
      node[i] = node[j];
      score[i] = score[j];
      node[j] = tmpNode;
      score[j] = tmpScore;
    }

    @Override
    protected void setPivot(int i) {
      pivot = i;
    }

    @Override
    protected int comparePivot(int j) {
      return Float.compare(score[pivot], score[j]);
    }
  }
}
