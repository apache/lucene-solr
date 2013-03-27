package org.apache.lucene.spatial;

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

import com.spatial4j.core.context.SpatialContext;
import com.spatial4j.core.shape.Point;
import com.spatial4j.core.shape.Rectangle;
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
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.carrotsearch.randomizedtesting.RandomizedTest.randomGaussian;
import static com.carrotsearch.randomizedtesting.RandomizedTest.randomIntBetween;

/** A base test class for spatial lucene. It's mostly Lucene generic. */
public abstract class SpatialTestCase extends LuceneTestCase {

  private DirectoryReader indexReader;
  protected RandomIndexWriter indexWriter;
  private Directory directory;
  protected IndexSearcher indexSearcher;

  protected SpatialContext ctx;//subclass must initialize

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();

    directory = newDirectory();
    indexWriter = new RandomIndexWriter(random(),directory);
  }

  @Override
  @After
  public void tearDown() throws Exception {
    IOUtils.close(indexWriter,indexReader,directory);
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
    IOUtils.close(indexReader);
    indexReader = indexWriter.getReader();
    indexSearcher = newSearcher(indexReader);
  }

  protected void verifyDocumentsIndexed(int numDocs) {
    assertEquals(numDocs, indexReader.numDocs());
  }

  protected SearchResults executeQuery(Query query, int numDocs) {
    try {
      TopDocs topDocs = indexSearcher.search(query, numDocs);

      List<SearchResult> results = new ArrayList<SearchResult>();
      for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
        results.add(new SearchResult(scoreDoc.score, indexSearcher.doc(scoreDoc.doc)));
      }
      return new SearchResults(topDocs.totalHits, results);
    } catch (IOException ioe) {
      throw new RuntimeException("IOException thrown while executing query", ioe);
    }
  }

  protected Point randomPoint() {
    final Rectangle WB = ctx.getWorldBounds();
    return ctx.makePoint(
        randomIntBetween((int) WB.getMinX(), (int) WB.getMaxX()),
        randomIntBetween((int) WB.getMinY(), (int) WB.getMaxY()));
  }

  protected Rectangle randomRectangle() {
    final Rectangle WB = ctx.getWorldBounds();
    int rW = (int) randomGaussianMeanMax(10, WB.getWidth());
    double xMin = randomIntBetween((int) WB.getMinX(), (int) WB.getMaxX() - rW);
    double xMax = xMin + rW;

    int yH = (int) randomGaussianMeanMax(Math.min(rW, WB.getHeight()), WB.getHeight());
    double yMin = randomIntBetween((int) WB.getMinY(), (int) WB.getMaxY() - yH);
    double yMax = yMin + yH;

    return ctx.makeRectangle(xMin, xMax, yMin, yMax);
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

    return mean + flip * pivotResult;
  }

  // ================================================= Inner Classes =================================================

  protected static class SearchResults {

    public int numFound;
    public List<SearchResult> results;

    public SearchResults(int numFound, List<SearchResult> results) {
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

