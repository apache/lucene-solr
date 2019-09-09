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

package org.apache.lucene.search

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
import org.apache.lucene.document.KnnGraphField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.PriorityQueue;

/** Tests indexing of a knn-graph by KnnGraphWriter */
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
    
  KnnGraphTester() {
    // set defaults
    numDocs = 10_000;
    dim = 256;
    topK = 10;
    numProbe = 20;
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
      int maxFanout = 0;
      int[] globalHist = new int[reader.maxDoc()];
      for (LeafReaderContext context : reader.leaves()) {
        LeafReader leafReader = context.reader();
        SortedNumericDocValues nbr = DocValues.getSortedNumeric(leafReader, KNN_FIELD + "$nbr");
        int leafMaxFanout = 0;
        int[] leafHist = new int[leafReader.maxDoc()];
        while(nbr.nextDoc() != SortedNumericDocValues.NO_MORE_DOCS) {
          int n = nbr.docValueCount();
          ++leafHist[n];
          leafMaxFanout = Math.max(leafMaxFanout, n);
          ++globalHist[n];
          maxFanout = Math.max(maxFanout, n);
        }
        System.out.printf("Segment %d fanout\n", context.ord);
        printHist(leafHist, leafMaxFanout);
      }
      System.out.println("Whole index fanout");
      printHist(globalHist, maxFanout);
    }
  }

  private void printHist(int[] hist, int max) {
    System.out.printf("max fanout=%d, count[max]=%d\n", max, hist[max]);
    int i = 0;
    while (i <= max) {
      int ii = i;
      for (int j=0; j < 25 && ii <= max; ii++, j++) {
        System.out.printf("%4d", ii);
      }
      System.out.println("");
      for (int j=0; j < 25 && i <= max; i++, j++) {
        System.out.printf("%4d", hist[i]);
      }
      System.out.println("");
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
        results[i] = GraphSearch.search(searcher, KNN_FIELD, topK, numProbe, targets[i]);
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

  void getActualNN(float[] target, int targetOffset, int[] nn, int nnOffset) {
      final ScoreDocQueue queue = new ScoreDocQueue(topK);
      assert queue.size() == topK : " queue.size()=" + queue.size();
      int j = 0;
      int vectorOffset = 0;
      ScoreDoc bottom = queue.top();
      while (j < numDocs) {
        if (target == vectors && targetOffset == vectorOffset) {
          continue;
        }
        float d = distance(target, targetOffset, vectorOffset, bottom.score);
        if (d < bottom.score) {
          bottom.doc = j;
          bottom.score = d;
          bottom = queue.updateTop();
          bottom = queue.top();
        }
        vectorOffset += dim;
        ++j;
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
        doc.add(new KnnGraphField(KNN_FIELD, vector));
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
      if (i % 100 == 1) {
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
