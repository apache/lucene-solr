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

import java.io.IOException;
import java.io.BufferedOutputStream;
import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.VectorField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.KnnGraphValues;
import org.apache.lucene.index.VectorValues;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.KnnGraphQuery;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.PriorityQueue;

/** For testing indexing and search performance of a knn-graph using random vectors */
public class KnnGraphTester {

  private final static String KNN_FIELD = "knn";
  private final static String ID_FIELD = "id";

  private Random random;
  private int numDocs;
  private int dim;
  private int topK;
  private int numProbe;
  private float[] vectors;
  private int[] nabors;

  public KnnGraphTester() {
    // set defaults
    numDocs = 10_000;
    dim = 256;
    topK = 10;
    numProbe = 500;
    random = new Random();
  }

  public static void main(String... args) throws Exception {
    if (args.length != 2) {
      usage();
    }
    switch (args[0]) {
      case "-generate":
        new KnnGraphTester().create(args[1]);
        break;
      case "-search":
        new KnnGraphTester().search(args[1]);
        break;
      case "-stats":
        new KnnGraphTester().stats(args[1]);
        break;
      default:
        usage();
    }
  }

  private void search(String dataFile) throws IOException {
    readDataFile(dataFile);
    Path indexPath = Paths.get("knn_test_index");
    createIndex(indexPath);
    // topK = 25;
    testSearch(indexPath, 1000);
    //GraphSearch.VERBOSE = true;
    //testSearch(indexPath, 1);
  }

  private void stats(String dataFile) throws IOException {
    readDataFile(dataFile);
    Path indexPath = Paths.get("knn_test_index");
    createIndex(indexPath);
    printFanoutHist(indexPath);
  }

  private void printFanoutHist(Path indexPath) throws IOException {
    try (Directory dir = FSDirectory.open(indexPath);
         DirectoryReader reader = DirectoryReader.open(dir)) {
      // int[] globalHist = new int[reader.maxDoc()];
      for (LeafReaderContext context : reader.leaves()) {
        LeafReader leafReader = context.reader();
        KnnGraphValues knnValues = leafReader.getKnnGraphValues(KNN_FIELD);
        System.out.printf("Leaf %d has %d documents and %d graph layers\n",
                          context.ord, leafReader.maxDoc(), knnValues.getTopLevel());
        for (int i = 0; i < knnValues.getTopLevel(); i++) {
          printLayerFanout(knnValues, i, leafReader.maxDoc());
          // reset for next layer:
          knnValues = leafReader.getKnnGraphValues(KNN_FIELD);
        }
      }
      /*
      System.out.println("Whole index fanout");
      printHist(globalHist, maxFanout);
      */
    }
  }

  private void printLayerFanout(KnnGraphValues knnValues, int layer, int numDocs) throws IOException {
    int min = Integer.MAX_VALUE, max = 0, total = 0;
    int count = 0;
    int[] leafHist = new int[numDocs];
    while(knnValues.nextDoc() != KnnGraphValues.NO_MORE_DOCS) {
      // ok to call when the doc is not in a layer? we should get zero?
      if (knnValues.getMaxLevel() < layer) {
        //++leafHist[0];
        continue;
      }
      int n = knnValues.getFriends(layer).length;
      ++leafHist[n];
      max = Math.max(max, n);
      min = Math.min(min, n);
      if (n > 0) {
        ++count;
        total += n;
      }
    }
    System.out.printf("Layer %d size=%d, Fanout min=%d, mean=%.2f, max=%d\n", layer, count, min, total / (float) count, max);
    printHist(leafHist, max, count, 10);
  }

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

  private void testSearch(Path indexPath, int numIters) throws IOException {
    float[][] targets = new float[numIters][];
    TopDocs[] results = new TopDocs[numIters];
    for (int i = 0; i < numIters; i++) {
      targets[i] = new float[dim];
      randomVector(targets[i]);
    }
    System.out.println("running " + numIters + " targets; topK=" + topK + ", numProbe=" + numProbe);
    long start = System.nanoTime();
    try (Directory dir = FSDirectory.open(indexPath);
         DirectoryReader reader = DirectoryReader.open(dir)) {
      IndexSearcher searcher = new IndexSearcher(reader);
      int result = 0;
      for (int i = 0; i < numIters; i++) {
        KnnGraphQuery query = new KnnGraphQuery(KNN_FIELD, targets[i], numProbe);
        results[i] = searcher.search(query, topK);
        for (ScoreDoc scoreDoc : results[i].scoreDocs) {
          int id = searcher.doc(scoreDoc.doc).getFields().get(0).numericValue().intValue();
          scoreDoc.doc = id;
        }
      }
    }
    long elapsed = (System.nanoTime() - start) / 1_000_000; // ns -> ms
    System.out.println("completed " + numIters + " searches in " + elapsed + " ms: " + (1000 * numIters / elapsed) + " QPS");
    System.out.println("checking results");
    checkResults(targets, results);
  }

  private void checkResults(float[][] targets, TopDocs[] results) {
    int[] expected = new int[topK];
    int totalMatches = 0;
    for (int i = 0; i < results.length; i++) {
      if (results[i].scoreDocs.length != topK) {
        System.err.println("search " + i + " got " + results[i].scoreDocs.length + " results, expecting " + topK);
      }
      getActualNN(targets[i], 0, expected, 0);
      int matched = compareNN(expected, results[i]);
      totalMatches += matched;
    }
    System.out.println("total matches = " + totalMatches);
    System.out.println("Average overlap = " + (100.0 * totalMatches / (results.length * topK)) + "%");
  }

  int compareNN(int[] expected, TopDocs results) {
    int matched = 0;
    int i = 0;
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
    for (int doc : expected) {
      expectedSet.add(doc);
    }
    for (ScoreDoc scoreDoc : results.scoreDocs) {
      if (expectedSet.contains(scoreDoc.doc)) {
        ++matched;
      }
    }
    return matched;
  }

  /** Find the closest this.topK vectors in this.vectors to a target vector by exhaustive
   * comparison to all vectors.
   * @param targetArray an array containing the target vector
   * @param targetOffset offset of the target vector in the target parameter
   * @param nn an array in which to write the resulting nearest neighbor vector indexes
   * @param nnOfset the offset in the nearest neighbor array at which to start writing
   */
  void getActualNN(float[] targetArray, int targetOffset, int[] nn, int nnOffset) {
      final ScoreDocQueue queue = new ScoreDocQueue(topK);
      assert queue.size() == topK : " queue.size()=" + queue.size();
      int vectorOffset = 0;
      ScoreDoc bottom = queue.top();
      for (int j = 0; j < numDocs; j++) {
        if (targetArray == vectors && targetOffset == vectorOffset) {
          continue;
        }
        float d = distance(targetArray, targetOffset, vectorOffset, bottom.score);
        if (d < bottom.score) {
          bottom.doc = j;
          bottom.score = d;
          bottom = queue.updateTop();
          bottom = queue.top();
        }
        vectorOffset += dim;
      }
      assert queue.size() == topK;
      nnOffset += topK;
      for (int k = 1; k <= topK; k++) {
        ScoreDoc scoreDoc = queue.pop();
        //System.out.println("" + scoreDoc.doc + ":" + scoreDoc.score);
        nn[nnOffset - k] = scoreDoc.doc;
      }
  }

  private void randomVector(float[] vector) {
    for(int i =0; i < vector.length; i++) {
      vector[i] = random.nextFloat();
    }
  }

  private void createIndex(Path indexPath) throws IOException {
    IndexWriterConfig iwc = new IndexWriterConfig()
      .setOpenMode(IndexWriterConfig.OpenMode.CREATE);
    System.out.println("creating index in " + indexPath);
    long start = System.nanoTime();
    try (FSDirectory dir = FSDirectory.open(indexPath);
         IndexWriter iw = new IndexWriter(dir, iwc)) {
      for (int i = 0; i < numDocs; i++) {
        float[] vector = new float[dim];
        System.arraycopy(vectors, i * dim, vector, 0, dim);
        Document doc = new Document();
        doc.add(new VectorField(KNN_FIELD, vector, VectorValues.DistanceFunction.EUCLIDEAN));
        doc.add(new StoredField(ID_FIELD, i));
        iw.addDocument(doc);
      }
    }
    long elapsed = System.nanoTime() - start;
    System.out.println("Indexed " + numDocs + " documents in " + elapsed / 1_000_000 + "ms");
  }

  private void create(String dataFile) throws IOException {
    generateRandomVectors(dim * numDocs);
    System.out.println("Generated " + numDocs + " random vectors");
    computeNearest();
    writeDataFile(dataFile);
  }

  private void readDataFile(String dataFile) throws IOException {
    try (InputStream in = Files.newInputStream(Paths.get(dataFile));
         BufferedInputStream bin = new BufferedInputStream(in);
         DataInputStream din = new DataInputStream(bin)) {
      numDocs = din.readInt();
      dim = din.readInt();
      topK = din.readInt();
      vectors = new float[numDocs * dim];
      for (int i = 0; i < vectors.length; i++) {
        vectors[i] = din.readFloat();
      }
      nabors = new int[numDocs * topK];
      for (int i = 0; i < nabors.length; i++) {
        nabors[i] = din.readInt();
      }
    }
  }

  private void writeDataFile(String dataFile) throws IOException {
    try (OutputStream out = Files.newOutputStream(Paths.get(dataFile));
         BufferedOutputStream bout = new BufferedOutputStream(out);
         DataOutputStream dout = new DataOutputStream(bout)) {
      dout.writeInt(numDocs);
      dout.writeInt(dim);
      dout.writeInt(topK);
      for (int i = 0; i < vectors.length; i++) {
        dout.writeFloat(vectors[i]);
      }
      for (int i = 0; i < nabors.length; i++) {
        dout.writeInt(nabors[i]);
      }
    }
  }

  private void generateRandomVectors(int size) {
    System.out.println("Allocating " + size * 4 / 1024 / 1024 + "MB");
    vectors = new float[size];
    randomVector(vectors);
  }

  private void computeNearest() {
    nabors = new int[topK * numDocs];
    System.out.println("finding nearest...");
    for (int i = 0; i < numDocs; i++) {
      if (i % 1000 == 1) {
        System.out.println("  " + (i - 1));
      }
      getActualNN(vectors, i * dim, nabors, i * topK);
    }
  }

  private float distance(float[] target, int targetOffset, int vectorOffset, float scoreToBeat) {
    float total = 0;
    for (int i = 0; i < dim; i++) {
      float d = target[targetOffset++] - vectors[vectorOffset++];
      total += d * d;
      if (total > scoreToBeat) {
        // return early since every dimension of the score is positive; it can only increase
        return Float.MAX_VALUE;
      }
    }
    return total;
  }

  private static void usage() {
    String error = "Usage: TestKnnGraph -generate|-search {datafile}";
    System.err.println(error);
    System.exit(1);
  }

  private static class ScoreDocQueue extends PriorityQueue<ScoreDoc> {
    ScoreDocQueue(int size) {
      super(size, () -> new ScoreDoc(-1, Float.MAX_VALUE));
    }

    @Override
    protected boolean lessThan(ScoreDoc a, ScoreDoc b) {
      if (a.score > b.score) {
        return true;
      } else if (a.score < b.score) {
        return false;
      } else {
        return a.doc > b.doc;
      }
    }
  }

}
