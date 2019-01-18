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

package org.apache.lucene.classification;


import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Stream;

import com.carrotsearch.randomizedtesting.annotations.TimeoutSuite;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.classification.utils.ConfusionMatrixGenerator;
import org.apache.lucene.classification.utils.DatasetSplitter;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.similarities.AfterEffectB;
import org.apache.lucene.search.similarities.AxiomaticF1EXP;
import org.apache.lucene.search.similarities.AxiomaticF1LOG;
import org.apache.lucene.search.similarities.BasicModelG;
import org.apache.lucene.search.similarities.ClassicSimilarity;
import org.apache.lucene.search.similarities.DFRSimilarity;
import org.apache.lucene.search.similarities.DistributionLL;
import org.apache.lucene.search.similarities.DistributionSPL;
import org.apache.lucene.search.similarities.IBSimilarity;
import org.apache.lucene.search.similarities.LMDirichletSimilarity;
import org.apache.lucene.search.similarities.LMJelinekMercerSimilarity;
import org.apache.lucene.search.similarities.LambdaDF;
import org.apache.lucene.search.similarities.LambdaTTF;
import org.apache.lucene.search.similarities.Normalization;
import org.apache.lucene.search.similarities.NormalizationH1;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TimeUnits;
import org.junit.Test;

@LuceneTestCase.SuppressSysoutChecks(bugUrl = "none")
@TimeoutSuite(millis = 365 * 24 * TimeUnits.HOUR) // hopefully ~1 year is long enough ;)
@LuceneTestCase.Monster("takes a lot!")
public final class Test20NewsgroupsClassification extends LuceneTestCase {

  private static final String PATH_TO_20N = "/path/to/20n/";
  private static final String INDEX = PATH_TO_20N + "index";

  private static final String CATEGORY_FIELD = "category";
  private static final String BODY_FIELD = "body";
  private static final String SUBJECT_FIELD = "subject";

  private static boolean index = true;
  private static boolean split = true;

  @Test
  public void test20Newsgroups() throws Exception {

    String indexProperty = System.getProperty("index");
    if (indexProperty != null) {
      try {
        index = Boolean.valueOf(indexProperty);
      } catch (Exception e) {
        // ignore
      }
    }

    String splitProperty = System.getProperty("split");
    if (splitProperty != null) {
      try {
        split = Boolean.valueOf(splitProperty);
      } catch (Exception e) {
        // ignore
      }
    }

    Path mainIndexPath = Paths.get(INDEX + "/original");
    Directory directory = FSDirectory.open(mainIndexPath);
    Path trainPath = Paths.get(INDEX + "/train");
    Path testPath = Paths.get(INDEX + "/test");
    Path cvPath = Paths.get(INDEX + "/cv");
    FSDirectory cv = null;
    FSDirectory test = null;
    FSDirectory train = null;
    IndexReader testReader = null;
    if (split) {
      cv = FSDirectory.open(cvPath);
      test = FSDirectory.open(testPath);
      train = FSDirectory.open(trainPath);
    }

    if (index) {
      delete(mainIndexPath);
      if (split) {
        delete(trainPath, testPath, cvPath);
      }
    }

    IndexReader reader = null;
    List<Classifier<BytesRef>> classifiers = new LinkedList<>();
    try {
      Analyzer analyzer = new StandardAnalyzer();
      if (index) {

        System.out.format("Indexing 20 Newsgroups...%n");

        long startIndex = System.currentTimeMillis();
        IndexWriter indexWriter = new IndexWriter(directory, new IndexWriterConfig(analyzer));

        int docsIndexed = buildIndex(new File(PATH_TO_20N), indexWriter);

        long endIndex = System.currentTimeMillis();
        System.out.format("Indexed %d pages in %ds %n", docsIndexed, (endIndex - startIndex) / 1000);

        indexWriter.close();

      }

      if (split && !index) {
        reader = DirectoryReader.open(train);
      } else {
        reader = DirectoryReader.open(directory);
      }

      if (index && split) {
        // split the index
        System.out.format("Splitting the index...%n");

        long startSplit = System.currentTimeMillis();
        DatasetSplitter datasetSplitter = new DatasetSplitter(0.2, 0);
        datasetSplitter.split(reader, train, test, cv, analyzer, false, CATEGORY_FIELD, BODY_FIELD, SUBJECT_FIELD, CATEGORY_FIELD);
        reader.close();
        reader = DirectoryReader.open(train); // using the train index from now on
        long endSplit = System.currentTimeMillis();
        System.out.format("Splitting done in %ds %n", (endSplit - startSplit) / 1000);
      }

      final long startTime = System.currentTimeMillis();


      classifiers.add(new KNearestNeighborClassifier(reader, new ClassicSimilarity(), analyzer, null, 1, 0, 0, CATEGORY_FIELD, BODY_FIELD));
      classifiers.add(new KNearestNeighborClassifier(reader, null, analyzer, null, 1, 0, 0, CATEGORY_FIELD, BODY_FIELD));
      classifiers.add(new KNearestNeighborClassifier(reader, new ClassicSimilarity(), analyzer, null, 3, 0, 0, CATEGORY_FIELD, BODY_FIELD));
      classifiers.add(new KNearestNeighborClassifier(reader, new AxiomaticF1EXP(), analyzer, null, 3, 0, 0, CATEGORY_FIELD, BODY_FIELD));
      classifiers.add(new KNearestNeighborClassifier(reader, new AxiomaticF1LOG(), analyzer, null, 3, 0, 0, CATEGORY_FIELD, BODY_FIELD));
      classifiers.add(new KNearestNeighborClassifier(reader, new LMDirichletSimilarity(), analyzer, null, 3, 1, 1, CATEGORY_FIELD, BODY_FIELD));
      classifiers.add(new KNearestNeighborClassifier(reader, new LMJelinekMercerSimilarity(0.3f), analyzer, null, 3, 1, 1, CATEGORY_FIELD, BODY_FIELD));
      classifiers.add(new KNearestNeighborClassifier(reader, null, analyzer, null, 3, 1, 1, CATEGORY_FIELD, BODY_FIELD));
      classifiers.add(new KNearestNeighborClassifier(reader, new DFRSimilarity(new BasicModelG(), new AfterEffectB(), new NormalizationH1()), analyzer, null, 3, 1, 1, CATEGORY_FIELD, BODY_FIELD));
      classifiers.add(new KNearestNeighborClassifier(reader, new IBSimilarity(new DistributionSPL(), new LambdaDF(), new Normalization.NoNormalization()), analyzer, null, 3, 1, 1, CATEGORY_FIELD, BODY_FIELD));
      classifiers.add(new KNearestNeighborClassifier(reader, new IBSimilarity(new DistributionLL(), new LambdaTTF(), new NormalizationH1()), analyzer, null, 3, 1, 1, CATEGORY_FIELD, BODY_FIELD));
      classifiers.add(new KNearestFuzzyClassifier(reader, new LMJelinekMercerSimilarity(0.3f), analyzer, null, 1, CATEGORY_FIELD, BODY_FIELD));
      classifiers.add(new KNearestFuzzyClassifier(reader, new IBSimilarity(new DistributionLL(), new LambdaTTF(), new NormalizationH1()), analyzer, null, 1, CATEGORY_FIELD, BODY_FIELD));
      classifiers.add(new KNearestFuzzyClassifier(reader, new ClassicSimilarity(), analyzer, null, 1, CATEGORY_FIELD, BODY_FIELD));
      classifiers.add(new KNearestFuzzyClassifier(reader, new ClassicSimilarity(), analyzer, null, 3, CATEGORY_FIELD, BODY_FIELD));
      classifiers.add(new KNearestFuzzyClassifier(reader, null, analyzer, null, 1, CATEGORY_FIELD, BODY_FIELD));
      classifiers.add(new KNearestFuzzyClassifier(reader, null, analyzer, null, 3, CATEGORY_FIELD, BODY_FIELD));
      classifiers.add(new KNearestFuzzyClassifier(reader, new AxiomaticF1EXP(), analyzer, null, 3, CATEGORY_FIELD, BODY_FIELD));
      classifiers.add(new KNearestFuzzyClassifier(reader, new AxiomaticF1LOG(), analyzer, null, 3, CATEGORY_FIELD, BODY_FIELD));
      classifiers.add(new BM25NBClassifier(reader, analyzer, null, CATEGORY_FIELD, BODY_FIELD));
      classifiers.add(new CachingNaiveBayesClassifier(reader, analyzer, null, CATEGORY_FIELD, BODY_FIELD));
      classifiers.add(new SimpleNaiveBayesClassifier(reader, analyzer, null, CATEGORY_FIELD, BODY_FIELD));

      int maxdoc;

      if (split) {
        testReader = DirectoryReader.open(test);
        maxdoc = testReader.maxDoc();
      } else {
        maxdoc = reader.maxDoc();
      }

      System.out.format("Starting evaluation on %d docs...%n", maxdoc);

      ExecutorService service = Executors.newCachedThreadPool();
      List<Future<String>> futures = new LinkedList<>();
      for (Classifier<BytesRef> classifier : classifiers) {
        testClassifier(reader, startTime, testReader, service, futures, classifier);
      }
      for (Future<String> f : futures) {
        System.out.println(f.get());
      }

      Thread.sleep(10000);
      service.shutdown();

    } finally {
      if (reader != null) {
        reader.close();
      }
      directory.close();
      if (test != null) {
        test.close();
      }
      if (train != null) {
        train.close();
      }
      if (cv != null) {
        cv.close();
      }
      if (testReader != null) {
        testReader.close();
      }

      for (Classifier c : classifiers) {
        if (c instanceof Closeable) {
          ((Closeable) c).close();
        }
      }
    }
  }

  private void testClassifier(final IndexReader ar, long startTime, IndexReader testReader, ExecutorService service, List<Future<String>> futures, Classifier<BytesRef> classifier) {
    futures.add(service.submit(() -> {
      ConfusionMatrixGenerator.ConfusionMatrix confusionMatrix;
      if (split) {
        confusionMatrix = ConfusionMatrixGenerator.getConfusionMatrix(testReader, classifier, CATEGORY_FIELD, BODY_FIELD, 60000 * 30);
      } else {
        confusionMatrix = ConfusionMatrixGenerator.getConfusionMatrix(ar, classifier, CATEGORY_FIELD, BODY_FIELD, 60000 * 30);
      }

      final long endTime = System.currentTimeMillis();
      final int elapse = (int) (endTime - startTime) / 1000;

      return " * " + classifier + " \n    * accuracy = " + confusionMatrix.getAccuracy() +
          "\n    * precision = " + confusionMatrix.getPrecision() +
          "\n    * recall = " + confusionMatrix.getRecall() +
          "\n    * f1-measure = " + confusionMatrix.getF1Measure() +
          "\n    * avgClassificationTime = " + confusionMatrix.getAvgClassificationTime() +
          "\n    * time = " + elapse + " (sec)\n ";
    }));
  }

  private void delete(Path... paths) throws IOException {
    for (Path path : paths) {
      if (Files.isDirectory(path)) {
        Stream<Path> pathStream = Files.list(path);
        Iterator<Path> iterator = pathStream.iterator();
        while (iterator.hasNext()) {
          Files.delete(iterator.next());
        }
      }
    }

  }


  int buildIndex(File indexDir, IndexWriter indexWriter)
      throws IOException {
    File[] groupsDir = indexDir.listFiles();
    int i = 0;
    if (groupsDir != null) {
      for (File group : groupsDir) {
        String groupName = group.getName();
        File[] posts = group.listFiles();
        if (posts != null) {
          for (File postFile : posts) {
            String number = postFile.getName();
            NewsPost post = parse(postFile, groupName, number);
            if (post != null) {
              Document d = new Document();
              d.add(new StringField(CATEGORY_FIELD,
                  post.getGroup(), Field.Store.YES));
              d.add(new SortedDocValuesField(CATEGORY_FIELD,
                  new BytesRef(post.getGroup())));
              d.add(new TextField(SUBJECT_FIELD,
                  post.getSubject(), Field.Store.YES));
              d.add(new TextField(BODY_FIELD,
                  post.getBody(), Field.Store.YES));
              indexWriter.addDocument(d);
              i++;
            }
          }
        }
      }
    }
    indexWriter.commit();
    return i;
  }

  private NewsPost parse(File postFile, String groupName, String number) throws IOException {
    StringBuilder body = new StringBuilder();
    String subject = "";
    boolean inBody = false;
    Path path = postFile.toPath();
    try {
      if (Files.isReadable(path)) {
        for (String line : Files.readAllLines(path)) {
          if (line.startsWith("Subject:")) {
            subject = line.substring(8);
          } else {
            if (inBody) {
              if (body.length() > 0) {
                body.append("\n");
              }
              body.append(line);
            } else if (line.isEmpty() || line.trim().length() == 0) {
              inBody = true;
            }
          }
        }
      }
      return new NewsPost(body.toString(), subject, groupName, number);
    } catch (Throwable e) {
      return null;
    }
  }

  private class NewsPost {
    private final String body;
    private final String subject;
    private final String group;
    private final String number;

    private NewsPost(String body, String subject, String group, String number) {
      this.body = body;
      this.subject = subject;
      this.group = group;
      this.number = number;
    }

    public String getBody() {
      return body;
    }

    public String getSubject() {
      return subject;
    }

    public String getGroup() {
      return group;
    }

    public String getNumber() {
      return number;
    }
  }
}