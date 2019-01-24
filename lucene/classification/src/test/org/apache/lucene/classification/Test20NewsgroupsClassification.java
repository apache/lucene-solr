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
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

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
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.NamedThreadFactory;
import org.apache.lucene.util.TestUtil;
import org.apache.lucene.util.TimeUnits;
import org.junit.Test;

@LuceneTestCase.SuppressSysoutChecks(bugUrl = "none")
@TimeoutSuite(millis = 365 * 24 * TimeUnits.HOUR) // hopefully ~1 year is long enough ;)
@LuceneTestCase.Monster("takes a lot!")
public final class Test20NewsgroupsClassification extends LuceneTestCase {

  private static final String CATEGORY_FIELD = "category";
  private static final String BODY_FIELD = "body";
  private static final String SUBJECT_FIELD = "subject";
  private static final String INDEX_DIR = "/path/to/lucene-solr/lucene/classification/20n";

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

    Directory directory = newDirectory();
    Directory cv = null;
    Directory test = null;
    Directory train = null;
    IndexReader testReader = null;
    if (split) {
      cv = newDirectory();
      test = newDirectory();
      train = newDirectory();
    }

    IndexReader reader = null;
    List<Classifier<BytesRef>> classifiers = new LinkedList<>();
    try {
      Analyzer analyzer = new StandardAnalyzer();
      if (index) {

        System.out.println("Indexing 20 Newsgroups...");

        long startIndex = System.currentTimeMillis();
        IndexWriter indexWriter = new IndexWriter(directory, new IndexWriterConfig(analyzer));

        Path indexDir = Paths.get(INDEX_DIR);
        int docsIndexed = buildIndex(indexDir, indexWriter);

        long endIndex = System.currentTimeMillis();
        System.out.println("Indexed " + docsIndexed + " docs in " + (endIndex - startIndex) / 1000 + "s");

        indexWriter.close();

      }

      if (split && !index) {
        reader = DirectoryReader.open(train);
      } else {
        reader = DirectoryReader.open(directory);
      }

      if (index && split) {
        System.out.println("Splitting the index...");

        long startSplit = System.currentTimeMillis();
        DatasetSplitter datasetSplitter = new DatasetSplitter(0.2, 0);
        datasetSplitter.split(reader, train, test, cv, analyzer, false, CATEGORY_FIELD, BODY_FIELD, SUBJECT_FIELD, CATEGORY_FIELD);
        reader.close();
        reader = DirectoryReader.open(train); // using the train index from now on
        long endSplit = System.currentTimeMillis();
        System.out.println("Splitting done in " + (endSplit - startSplit) / 1000 + "s");
      }

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

      System.out.println("Starting evaluation on " + maxdoc + " docs...");

      ExecutorService service = new ThreadPoolExecutor(1, TestUtil.nextInt(random(), 2, 6), Long.MAX_VALUE, TimeUnit.MILLISECONDS,
          new LinkedBlockingQueue<>(),
          new NamedThreadFactory(getClass().getName()));
      List<Future<String>> futures = new LinkedList<>();
      for (Classifier<BytesRef> classifier : classifiers) {
        testClassifier(reader, testReader, service, futures, classifier);
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
      if (testReader != null) {
        testReader.close();
      }
      if (test != null) {
        test.close();
      }
      if (train != null) {
        train.close();
      }
      if (cv != null) {
        cv.close();
      }

      for (Classifier<BytesRef> c : classifiers) {
        if (c instanceof Closeable) {
          ((Closeable) c).close();
        }
      }
    }
  }

  private void testClassifier(final IndexReader ar, IndexReader testReader, ExecutorService service, List<Future<String>> futures, Classifier<BytesRef> classifier) {
    futures.add(service.submit(() -> {
      final long startTime = System.currentTimeMillis();
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

  private int buildIndex(Path indexDir, IndexWriter indexWriter)
      throws IOException {
    int i = 0;
    try (DirectoryStream<Path> groupsStream = Files.newDirectoryStream(indexDir)) {
      for (Path groupsDir : groupsStream) {
        if (!Files.isHidden(groupsDir)) {
          try (DirectoryStream<Path> stream = Files.newDirectoryStream(groupsDir)) {
            for (Path p : stream) {
              if (!Files.isHidden(p)) {
                NewsPost post = parse(p, p.getParent().getFileName().toString(), p.getFileName().toString());
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
      }
    }
    indexWriter.commit();
    return i;
  }

  private NewsPost parse(Path path, String groupName, String number) {
    StringBuilder body = new StringBuilder();
    String subject = "";
    boolean inBody = false;
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