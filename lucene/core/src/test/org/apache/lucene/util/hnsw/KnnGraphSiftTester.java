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

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Random;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.VectorField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SerialMergeScheduler;
import org.apache.lucene.index.VectorValues;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.KnnGraphQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.similarities.AssertingSimilarity;
import org.apache.lucene.search.similarities.RandomSimilarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

public class KnnGraphSiftTester {
  private static final String HNSW_INDEX_DIR = "/tmp/hnsw";

  private static final String KNN_VECTOR_FIELD = "vector";

  private static final Random random = new Random();

  /**
   * @param args the first arg is the file path of test data set，the second is the query file path and the third is the groundtruth file path.
   */
  public static void main(String[] args) {
    if (args.length < 3) {
      PrintHelp();
    }

    try {
      final List<float[]> siftDataset = SiftDataReader.fvecReadAll(args[0]);
      assert !siftDataset.isEmpty();

      final List<float[]> queryDataset = SiftDataReader.fvecReadAll(args[1]);
      assert !queryDataset.isEmpty();

      final List<int[]> groundTruthVects = SiftDataReader.ivecReadAll(args[2], queryDataset.size());
      assert !groundTruthVects.isEmpty();

      evaluateRecallRatio(siftDataset, queryDataset, groundTruthVects);

    } catch (IOException e) {
      e.printStackTrace();
      System.exit(1);
    }
  }

  private static void evaluateRecallRatio(final List<float[]> siftDataset, final List<float[]> queryDataset,
                                          final List<int[]> groundTruthVects) {
    runCase(siftDataset.size(), siftDataset.get(0).length, HNSW_INDEX_DIR,
        siftDataset, queryDataset, groundTruthVects);
  }

  public static boolean runCase(int numDoc, int dimension, String indexDir, List<float[]> randomVectors,
                                final List<float[]> queryDataset,
                                final List<int[]> groundTruthVects) {
    safeDelete(indexDir);

    try (Directory dir = FSDirectory.open(Paths.get(indexDir)); IndexWriter iw = new IndexWriter(
        dir, new IndexWriterConfig(new StandardAnalyzer()).setSimilarity(new AssertingSimilarity(new RandomSimilarity(new Random())))
        .setMaxBufferedDocs(1500000).setRAMBufferSizeMB(4096).setMergeScheduler(new SerialMergeScheduler()).setUseCompoundFile(false)
        .setReaderPooling(false).setCodec(Codec.forName("Lucene90")))) {
      long totalIndexTime = 0, commitTime = 0, forceMergeTime = 0;
      long addStartTime = System.currentTimeMillis();
      for (int i = 0; i < numDoc; ++i) {
        KnnTestHelper.add(iw, i, randomVectors.get(i));
      }

      long addEndTime = System.currentTimeMillis();
      iw.commit();
      long commitEndTime = System.currentTimeMillis();

      iw.forceMerge(1);
      long forceEndTime = System.currentTimeMillis();

      System.out.println("[***HNSW***] [ADD] cost " + (addEndTime - addStartTime) + " msec, [COMMIT] cost "
          + (commitEndTime - addEndTime) + " msec, [ForceMerge1] cost " + (forceEndTime - commitEndTime) + " msec, total cost "
          + (forceEndTime - commitEndTime) + " msec");

      assertResult(numDoc, dimension, dir, queryDataset, groundTruthVects,
          randomVectors, groundTruthVects.get(0).length);
    } catch (IOException e) {
      e.printStackTrace();
      return false;
    }

    safeDelete(indexDir);

    return true;
  }

  public static void assertResult(int numDoc, int dimension, Directory dir, final List<float[]> queryDataset,
                                  final List<int[]> groundTruthVects, final List<float[]> totalVecs,
                                  int topK) throws IOException {
    long totalCostTime = 0;
    int totalRecallCnt = 0;
    QueryResult result;
    int querySize = queryDataset.size();
    try (IndexReader reader = DirectoryReader.open(dir)) {
      for (int i = 0; i < querySize; ++i) {
        result = KnnTestHelper.assertRecall(reader, topK, queryDataset.get(i),
            groundTruthVects.get(i), totalVecs);
        totalCostTime += result.costTime;
        totalRecallCnt += result.recallCnt;
      }
    }

    System.out.println("[***HNSW***] Total number of docs -> " + numDoc +
        ", dimension -> " + dimension + ", number of recall experiments -> " + queryDataset.size() +
        ", exact recall times -> " + totalRecallCnt + ", total search time -> " +
        totalCostTime + "msec, avg search time -> " + 1.0F * totalCostTime / queryDataset.size() +
        "msec, recall percent -> " + 100.0F * totalRecallCnt / (queryDataset.size() * topK) + "%");
  }

  private static void safeDelete(final String indexDir) {
    try {
      Files.walk(Paths.get(indexDir)).sorted(Comparator.reverseOrder())
          .map(Path::toFile).forEach(File::deleteOnExit);
    } catch (NoSuchFileException ignored) {
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private static void PrintHelp() {
    /// sift data path should be indicated
    System.err.println("Usage: KnnIvfAndGraphPerformTester ${baseVecPath} ${queryVecPath} ${groundTruthPath}");
    System.exit(1);
  }

  private static final class SiftDataReader {
    private SiftDataReader() {
    }

    public static List<float[]> fvecReadAll(final String fileName) throws IOException {
      final List<float[]> vectors = new ArrayList<>();
      try (FileInputStream stream = new FileInputStream(fileName);
           InputStream fileStream = new DataInputStream(stream)) {
        byte[] allBytes = fileStream.readAllBytes();
        ByteBuffer byteBuffer = ByteBuffer.wrap(allBytes);
        while (byteBuffer.hasRemaining()) {
          int vecDims = byteBuffer.order(ByteOrder.LITTLE_ENDIAN).getInt();
          assert vecDims > 0;

          float[] vec = new float[vecDims];
          for (int i = 0; i < vecDims; ++i) {
            vec[i] = byteBuffer.order(ByteOrder.LITTLE_ENDIAN).getFloat();
          }
          vectors.add(vec);
        }
      }

      return vectors;
    }

    public static List<int[]> ivecReadAll(final String fileName, int expectSize) throws IOException {
      final List<int[]> vectors = new ArrayList<>(expectSize);
      try (FileInputStream stream = new FileInputStream(fileName);
           InputStream fileStream = new DataInputStream(stream)) {
        byte[] allBytes = fileStream.readAllBytes();
        ByteBuffer byteBuffer = ByteBuffer.wrap(allBytes);
        while (byteBuffer.hasRemaining()) {
          int vecDims = byteBuffer.order(ByteOrder.LITTLE_ENDIAN).getInt();
          assert vecDims > 0;

          int[] vec = new int[vecDims];
          for (int i = 0; i < vecDims; ++i) {
            vec[i] = byteBuffer.order(ByteOrder.LITTLE_ENDIAN).getInt();
          }
          vectors.add(vec);
        }
      }

      return vectors;
    }
  }

  private static final class KnnTestHelper {
    private KnnTestHelper() {
    }

    public static void add(IndexWriter iw, int id, float[] vector) throws IOException {
      Document doc = new Document();
      if (vector != null) {
        doc.add(new VectorField(KNN_VECTOR_FIELD, vector, VectorValues.DistanceFunction.EUCLIDEAN));
      }
      doc.add(new StringField("id", Integer.toString(id), Field.Store.YES));
      iw.addDocument(doc);
    }

    public static float[][] randomVectors(int numDocs, int numDims) {
      float[][] vectors = new float[numDocs][];
      for (int i = 0; i < numDocs; ++i) {
        vectors[i] = randomVector(numDims);
      }

      return vectors;
    }

    private static float[] randomVector(int numDims) {
      float[] vector = new float[numDims];
      for (int i = 0; i < numDims; i++) {
        vector[i] = random.nextFloat();
      }

      return vector;
    }

    public static QueryResult assertRecall(IndexReader reader, int topK, float[] value,
                                           int[] truth, final List<float[]> totalVecs) throws IOException {
      IndexSearcher searcher = new IndexSearcher(reader, null);
      Query query = new KnnGraphQuery(KNN_VECTOR_FIELD, value, topK);

      long startTime = System.currentTimeMillis();
      TopDocs result = searcher.search(query, topK);
      long costTime = System.currentTimeMillis() - startTime;

      int totalRecallCnt = 0, exactRecallCnt = 0;
      final List<float[]> recallVecs = new ArrayList<>(result.scoreDocs.length);
      for (LeafReaderContext ctx : reader.leaves()) {
        VectorValues vector = ctx.reader().getVectorValues(KNN_VECTOR_FIELD);
        for (ScoreDoc doc : result.scoreDocs) {
          if (vector.seek(doc.doc - ctx.docBase)) {
            ++totalRecallCnt;
            recallVecs.add(vector.vectorValue().clone());
          }
        }
      }
      assert topK == totalRecallCnt;

      boolean[] matched = new boolean[truth.length];
      Arrays.fill(matched, false);

      for (float[] vec : recallVecs) {
        for (int i = 0; i < truth.length; ++i) {
          if (matched[i]) {
            continue;
          }
          if (Arrays.equals(vec, totalVecs.get(truth[i]))) {
            ++exactRecallCnt;
            matched[i] = true;

            break;
          }
        }
      }

      return new QueryResult(exactRecallCnt, costTime);
    }
  }

  public static final class QueryResult {
    public int recallCnt;
    public long costTime;

    public QueryResult(int recall, long costTime) {
      this.recallCnt = recall;
      this.costTime = costTime;
    }
  }
}
