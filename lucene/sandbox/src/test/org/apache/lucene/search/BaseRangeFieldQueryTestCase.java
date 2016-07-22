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
package org.apache.lucene.search;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.MultiDocValues;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SerialMergeScheduler;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LuceneTestCase;

/**
 * Abstract class to do basic tests for a RangeField query.
 */
public abstract class BaseRangeFieldQueryTestCase extends LuceneTestCase {
  protected abstract Field newRangeField(double[] min, double[] max);

  protected abstract Query newIntersectsQuery(double[] min, double[] max);

  protected abstract Query newContainsQuery(double[] min, double[] max);

  protected abstract Query newWithinQuery(double[] min, double[] max);

  protected int dimension() {
    return random().nextInt(4) + 1;
  }

  public void testRandomTiny() throws Exception {
    // Make sure single-leaf-node case is OK:
    doTestRandom(10, false);
  }

  public void testRandomMedium() throws Exception {
    doTestRandom(10000, false);
  }

  @Nightly
  public void testRandomBig() throws Exception {
    doTestRandom(200000, false);
  }

  public void testMultiValued() throws Exception {
    doTestRandom(10000, true);
  }

  private void doTestRandom(int count, boolean multiValued) throws Exception {
    int numDocs = atLeast(count);
    int dimensions = dimension();

    if (VERBOSE) {
      System.out.println("TEST: numDocs=" + numDocs);
    }

    Box[][] boxes = new Box[numDocs][];

    boolean haveRealDoc = true;

    nextdoc: for (int id=0; id<numDocs; ++id) {
      int x = random().nextInt(20);
      if (boxes[id] == null) {
        boxes[id] = new Box[] {nextBox(dimensions)};
      }
      if (x == 17) {
        // dome docs don't have a box:
        boxes[id][0].min[0] = Double.NaN;
        if (VERBOSE) {
          System.out.println("  id=" + id + " is missing");
        }
        continue;
      }

      if (multiValued == true && random().nextBoolean()) {
        // randomly add multi valued documents (up to 2 fields)
        int n = random().nextInt(2) + 1;
        boxes[id] = new Box[n];
        for (int i=0; i<n; ++i) {
          boxes[id][i] = nextBox(dimensions);
        }
      }

      if (id > 0 && x < 9 && haveRealDoc) {
        int oldID;
        int i=0;
        // don't step on missing boxes:
        while (true) {
          oldID = random().nextInt(id);
          if (Double.isNaN(boxes[oldID][0].min[0]) == false) {
            break;
          } else if (++i > id) {
            continue nextdoc;
          }
        }

        if (x == dimensions*2) {
          // Fully identical box (use first box in case current is multivalued but old is not)
          for (int d=0; d<dimensions; ++d) {
            boxes[id][0].min[d] = boxes[oldID][0].min[d];
            boxes[id][0].max[d] = boxes[oldID][0].max[d];
          }
          if (VERBOSE) {
            System.out.println("  id=" + id + " box=" + boxes[id] + " (same box as doc=" + oldID + ")");
          }
        } else {
          for (int m = 0, even = dimensions % 2; m < dimensions * 2; ++m) {
            if (x == m) {
              int d = (int)Math.floor(m/2);
              // current could be multivalue but old may not be, so use first box
              if (even == 0) {
                boxes[id][0].setVal(d, boxes[oldID][0].min[d]);
                if (VERBOSE) {
                  System.out.println("  id=" + id + " box=" + boxes[id] + " (same min[" + d + "] as doc=" + oldID + ")");
                }
              } else {
                boxes[id][0].setVal(d, boxes[oldID][0].max[d]);
                if (VERBOSE) {
                  System.out.println("  id=" + id + " box=" + boxes[id] + " (same max[" + d + "] as doc=" + oldID + ")");
                }
              }
            }
          }
        }
      }
    }
    verify(boxes);
  }

  private void verify(Box[][] boxes) throws Exception {
    IndexWriterConfig iwc = newIndexWriterConfig();
    // Else seeds may not reproduce:
    iwc.setMergeScheduler(new SerialMergeScheduler());
    // Else we can get O(N^2) merging
    int mbd = iwc.getMaxBufferedDocs();
    if (mbd != -1 && mbd < boxes.length/100) {
      iwc.setMaxBufferedDocs(boxes.length/100);
    }
    Directory dir;
    if (boxes.length > 50000) {
      dir = newFSDirectory(createTempDir(getClass().getSimpleName()));
    } else {
      dir = newDirectory();
    }

    Set<Integer> deleted = new HashSet<>();
    IndexWriter w = new IndexWriter(dir, iwc);
    for (int id=0; id < boxes.length; ++id) {
      Document doc = new Document();
      doc.add(newStringField("id", ""+id, Field.Store.NO));
      doc.add(new NumericDocValuesField("id", id));
      if (Double.isNaN(boxes[id][0].min[0]) == false) {
        for (int n=0; n<boxes[id].length; ++n) {
          doc.add(newRangeField(boxes[id][n].min, boxes[id][n].max));
        }
      }
      w.addDocument(doc);
      if (id > 0 && random().nextInt(100) == 1) {
        int idToDelete = random().nextInt(id);
        w.deleteDocuments(new Term("id", ""+idToDelete));
        deleted.add(idToDelete);
        if (VERBOSE) {
          System.out.println("  delete id=" + idToDelete);
        }
      }
    }

    if (random().nextBoolean()) {
      w.forceMerge(1);
    }
    final IndexReader r = DirectoryReader.open(w);
    w.close();
    IndexSearcher s = newSearcher(r);

    int dimensions = boxes[0][0].min.length;
    int iters = atLeast(25);
    NumericDocValues docIDToID = MultiDocValues.getNumericValues(r, "id");
    Bits liveDocs = MultiFields.getLiveDocs(s.getIndexReader());
    int maxDoc = s.getIndexReader().maxDoc();

    for (int iter=0; iter<iters; ++iter) {
      if (VERBOSE) {
        System.out.println("\nTEST: iter=" + iter + " s=" + s);
      }

      // occasionally test open ended bounding boxes
      Box queryBox = nextBox(dimensions);
      int rv = random().nextInt(3);
      Query query;
      Box.QueryType queryType;
      if (rv == 0) {
        queryType = Box.QueryType.INTERSECTS;
        query = newIntersectsQuery(queryBox.min, queryBox.max);
      } else if (rv == 1)  {
        queryType = Box.QueryType.CONTAINS;
        query = newContainsQuery(queryBox.min, queryBox.max);
      } else {
        queryType = Box.QueryType.WITHIN;
        query = newWithinQuery(queryBox.min, queryBox.max);
      }

      if (VERBOSE) {
        System.out.println("  query=" + query);
      }

      final FixedBitSet hits = new FixedBitSet(maxDoc);
      s.search(query, new SimpleCollector() {
        private int docBase;

        @Override
        public void collect(int doc) {
          hits.set(docBase + doc);
        }

        @Override
        protected void doSetNextReader(LeafReaderContext context) throws IOException {
          docBase = context.docBase;
        }

        @Override
        public boolean needsScores() { return false; }
      });

      for (int docID=0; docID<maxDoc; ++docID) {
        int id = (int) docIDToID.get(docID);
        boolean expected;
        if (liveDocs != null && liveDocs.get(docID) == false) {
          // document is deleted
          expected = false;
        } else if (Double.isNaN(boxes[id][0].min[0])) {
          expected = false;
        } else {
          expected = expectedResult(queryBox, boxes[id], queryType);
        }

        if (hits.get(docID) != expected) {
          StringBuilder b = new StringBuilder();
          b.append("FAIL (iter " + iter + "): ");
          if (expected == true) {
            b.append("id=" + id + (boxes[id].length > 1 ? " (MultiValue) " : " ") + "should match but did not\n");
          } else {
            b.append("id=" + id + " should not match but did\n");
          }
          b.append(" queryBox=" + queryBox + "\n");
          b.append(" box" + ((boxes[id].length > 1) ? "es=" : "=" ) + boxes[id][0]);
          for (int n=1; n<boxes[id].length; ++n) {
            b.append(", ");
            b.append(boxes[id][n]);
          }
          b.append("\n queryType=" + queryType + "\n");
          b.append(" deleted?=" + (liveDocs != null && liveDocs.get(docID) == false));
          fail("wrong hit (first of possibly more):\n\n" + b);
        }
      }
    }
    IOUtils.close(r, dir);
  }

  protected boolean expectedResult(Box queryBox, Box[] box, Box.QueryType queryType) {
    for (int i=0; i<box.length; ++i) {
      if (expectedBBoxQueryResult(queryBox, box[i], queryType) == true) {
        return true;
      }
    }
    return false;
  }

  protected boolean expectedBBoxQueryResult(Box queryBox, Box box, Box.QueryType queryType) {
    if (box.equals(queryBox)) {
      return true;
    }
    Box.QueryType relation = box.relate(queryBox);
    if (queryType == Box.QueryType.INTERSECTS) {
      return relation != null;
    }
    return relation == queryType;
  }

  protected double nextDoubleInternal() {
    if (rarely()) {
      return random().nextBoolean() ? Double.POSITIVE_INFINITY : Double.NEGATIVE_INFINITY;
    }
    double max = 100 / 2;
    return (max + max) * random().nextDouble() - max;
  }

  protected Box nextBox(int dimensions) {
    double[] min = new double[dimensions];
    double[] max = new double[dimensions];

    for (int d=0; d<dimensions; ++d) {
      min[d] = nextDoubleInternal();
      max[d] = nextDoubleInternal();
    }

    return new Box(min, max);
  }

  protected static class Box {
    double[] min;
    double[] max;

    enum QueryType { INTERSECTS, WITHIN, CONTAINS }

    Box(double[] min, double[] max) {
      assert min != null && max != null && min.length > 0 && max.length > 0
          : "test box: min/max cannot be null or empty";
      assert min.length == max.length : "test box: min/max length do not agree";
      this.min = new double[min.length];
      this.max = new double[max.length];
      for (int d=0; d<min.length; ++d) {
        this.min[d] = Math.min(min[d], max[d]);
        this.max[d] = Math.max(min[d], max[d]);
      }
    }

    protected void setVal(int dimension, double val) {
      if (val <= min[dimension]) {
        min[dimension] = val;
      } else {
        max[dimension] = val;
      }
    }

    @Override
    public boolean equals(Object o) {
      return o != null
          && getClass() == o.getClass()
          && equalTo(getClass().cast(o));
    }

    private boolean equalTo(Box o) {
      return Arrays.equals(min, o.min)
          && Arrays.equals(max, o.max);
    }

    @Override
    public int hashCode() {
      int result = Arrays.hashCode(min);
      result = 31 * result + Arrays.hashCode(max);
      return result;
    }

    QueryType relate(Box other) {
      // check disjoint
      for (int d=0; d<this.min.length; ++d) {
        if (this.min[d] > other.max[d] || this.max[d] < other.min[d]) {
          // disjoint:
          return null;
        }
      }

      // check within
      boolean within = true;
      for (int d=0; d<this.min.length; ++d) {
        if ((this.min[d] >= other.min[d] && this.max[d] <= other.max[d]) == false) {
          // not within:
          within = false;
          break;
        }
      }
      if (within == true) {
        return QueryType.WITHIN;
      }

      // check contains
      boolean contains = true;
      for (int d=0; d<this.min.length; ++d) {
        if ((this.min[d] <= other.min[d] && this.max[d] >= other.max[d]) == false) {
          // not contains:
          contains = false;
          break;
        }
      }
      if (contains == true) {
        return QueryType.CONTAINS;
      }
      return QueryType.INTERSECTS;
    }

    @Override
    public String toString() {
      StringBuilder b = new StringBuilder();
      b.append("Box(");
      b.append(min[0]);
      b.append(" TO ");
      b.append(max[0]);
      for (int d=1; d<min.length; ++d) {
        b.append(", ");
        b.append(min[d]);
        b.append(" TO ");
        b.append(max[d]);
      }
      b.append(")");

      return b.toString();
    }
  }
}
