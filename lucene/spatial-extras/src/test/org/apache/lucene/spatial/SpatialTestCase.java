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
package org.apache.lucene.spatial;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.LuceneTestCase.SuppressSysoutChecks;
import org.locationtech.spatial4j.context.SpatialContext;
import org.locationtech.spatial4j.distance.DistanceUtils;
import org.locationtech.spatial4j.shape.Point;
import org.locationtech.spatial4j.shape.Rectangle;

import static com.carrotsearch.randomizedtesting.RandomizedTest.randomDouble;
import static com.carrotsearch.randomizedtesting.RandomizedTest.randomGaussian;
import static com.carrotsearch.randomizedtesting.RandomizedTest.randomInt;
import static com.carrotsearch.randomizedtesting.RandomizedTest.randomIntBetween;

/** A base test class for spatial lucene. It's mostly Lucene generic. */
@SuppressSysoutChecks(bugUrl = "These tests use JUL extensively.")
public abstract class SpatialTestCase extends LuceneTestCase {

  protected Logger log = Logger.getLogger(getClass().getName());

  private DirectoryReader indexReader;
  protected RandomIndexWriter indexWriter;
  private Directory directory;
  private Analyzer analyzer;
  protected IndexSearcher indexSearcher;

  protected SpatialContext ctx;//subclass must initialize

  @Override
  public void setUp() throws Exception {
    super.setUp();
    directory = newDirectory();
    analyzer = new MockAnalyzer(random());
    indexWriter = new RandomIndexWriter(random(), directory, LuceneTestCase.newIndexWriterConfig(random(), analyzer));
    indexReader = indexWriter.getReader();
    indexSearcher = newSearcher(indexReader);
  }

  @Override
  public void tearDown() throws Exception {
    IOUtils.close(indexWriter, indexReader, analyzer, directory);
    super.tearDown();
  }

  // ================================================= Helper Methods ================================================

  protected void addDocument(Document doc) throws IOException {
    indexWriter.addDocument(doc);
  }

  protected void addDocumentsAndCommit(List<Document> documents) throws IOException {
    for (Document document : documents) {
      indexWriter.addDocument(document);
    }
    commit();
  }

  protected void deleteAll() throws IOException {
    indexWriter.deleteAll();
  }

  protected void commit() throws IOException {
    indexWriter.commit();
    DirectoryReader newReader = DirectoryReader.openIfChanged(indexReader);
    if (newReader != null) {
      IOUtils.close(indexReader);
      indexReader = newReader;
    }
    indexSearcher = newSearcher(indexReader);
  }

  protected void verifyDocumentsIndexed(int numDocs) {
    assertEquals(numDocs, indexReader.numDocs());
  }

  protected SearchResults executeQuery(Query query, int numDocs) {
    try {
      TopDocs topDocs = indexSearcher.search(query, numDocs);

      List<SearchResult> results = new ArrayList<>();
      for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
        results.add(new SearchResult(scoreDoc.score, indexSearcher.doc(scoreDoc.doc)));
      }
      return new SearchResults(topDocs.totalHits.value, results);
    } catch (IOException ioe) {
      throw new RuntimeException("IOException thrown while executing query", ioe);
    }
  }

  @SuppressWarnings("deprecation")
  protected Point randomPoint() {
    final Rectangle WB = ctx.getWorldBounds();
    return ctx.makePoint(
        randomIntBetween((int) WB.getMinX(), (int) WB.getMaxX()),
        randomIntBetween((int) WB.getMinY(), (int) WB.getMaxY()));
  }

  protected Rectangle randomRectangle() {
    return randomRectangle(ctx.getWorldBounds());
  }

  @SuppressWarnings("deprecation")
  protected Rectangle randomRectangle(Rectangle bounds) {
    double[] xNewStartAndWidth = randomSubRange(bounds.getMinX(), bounds.getWidth());
    double xMin = xNewStartAndWidth[0];
    double xMax = xMin + xNewStartAndWidth[1];
    if (bounds.getCrossesDateLine()) {
      xMin = DistanceUtils.normLonDEG(xMin);
      xMax = DistanceUtils.normLonDEG(xMax);
    }

    double[] yNewStartAndHeight = randomSubRange(bounds.getMinY(), bounds.getHeight());
    double yMin = yNewStartAndHeight[0];
    double yMax = yMin + yNewStartAndHeight[1];

    return ctx.makeRectangle(xMin, xMax, yMin, yMax);
  }

  /** Returns new minStart and new length that is inside the range specified by the arguments. */
  @SuppressWarnings("deprecation")
  protected double[] randomSubRange(double boundStart, double boundLen) {
    if (boundLen >= 3 && usually()) { // typical
      // prefer integers for ease of debugability ... and prefer 1/16th of bound
      int intBoundStart = (int) Math.ceil(boundStart);
      int intBoundEnd = (int) (boundStart + boundLen);
      int intBoundLen = intBoundEnd - intBoundStart;
      int newLen = (int) randomGaussianMeanMax(intBoundLen / 16.0, intBoundLen);
      int newStart = intBoundStart + randomInt(intBoundLen - newLen);
      return new double[]{newStart, newLen};
    } else { // (no int rounding)
      double newLen = randomGaussianMeanMax(boundLen / 16, boundLen);
      double newStart = boundStart + (boundLen - newLen == 0 ? 0 : (randomDouble() % (boundLen - newLen)));
      return new double[]{newStart, newLen};
    }
  }

  private double randomGaussianMinMeanMax(double min, double mean, double max) {
    assert mean > min;
    return randomGaussianMeanMax(mean - min, max - min) + min;
  }

  /**
   * Within one standard deviation (68% of the time) the result is "close" to
   * mean. By "close": when greater than mean, it's the lesser of 2*mean or half
   * way to max, when lesser than mean, it's the greater of max-2*mean or half
   * way to 0. The other 32% of the time it's in the rest of the range, touching
   * either 0 or max but never exceeding.
   */
  private double randomGaussianMeanMax(double mean, double max) {
    // DWS: I verified the results empirically
    assert mean <= max && mean >= 0;
    double g = randomGaussian();
    double mean2 = mean;
    double flip = 1;
    if (g < 0) {
      mean2 = max - mean;
      flip = -1;
      g *= -1;
    }
    // pivot is the distance from mean2 towards max where the boundary of
    // 1 standard deviation alters the calculation
    double pivotMax = max - mean2;
    double pivot = Math.min(mean2, pivotMax / 2);//from 0 to max-mean2
    assert pivot >= 0 && pivotMax >= pivot && g >= 0;
    double pivotResult;
    if (g <= 1)
      pivotResult = pivot * g;
    else
      pivotResult = Math.min(pivotMax, (g - 1) * (pivotMax - pivot) + pivot);

    double result = mean + flip * pivotResult;
    return (result < 0 || result > max) ? mean : result; // due this due to computational numerical precision
  }

  // ================================================= Inner Classes =================================================

  protected static class SearchResults {

    public long numFound;
    public List<SearchResult> results;

    public SearchResults(long numFound, List<SearchResult> results) {
      this.numFound = numFound;
      this.results = results;
    }

    public StringBuilder toDebugString() {
      StringBuilder str = new StringBuilder();
      str.append("found: ").append(numFound).append('[');
      for(SearchResult r : results) {
        String id = r.getId();
        str.append(id).append(", ");
      }
      str.append(']');
      return str;
    }

    @Override
    public String toString() {
      return "[found:"+numFound+" "+results+"]";
    }
  }

  protected static class SearchResult {

    public float score;
    public Document document;

    public SearchResult(float score, Document document) {
      this.score = score;
      this.document = document;
    }

    public String getId() {
      return document.get("id");
    }

    @Override
    public String toString() {
      return "["+score+"="+document+"]";
    }
  }
}
