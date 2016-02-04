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
package org.apache.lucene.uninverting;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.apache.lucene.uninverting.UninvertingReader.Type;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;

/** random sorting tests with uninversion */
public class TestFieldCacheSortRandom extends LuceneTestCase {

  public void testRandomStringSort() throws Exception {
    testRandomStringSort(SortField.Type.STRING);
  }

  public void testRandomStringValSort() throws Exception {
    testRandomStringSort(SortField.Type.STRING_VAL);
  }

  private void testRandomStringSort(SortField.Type type) throws Exception {
    Random random = new Random(random().nextLong());

    final int NUM_DOCS = atLeast(100);
    final Directory dir = newDirectory();
    final RandomIndexWriter writer = new RandomIndexWriter(random, dir);
    final boolean allowDups = random.nextBoolean();
    final Set<String> seen = new HashSet<>();
    final int maxLength = TestUtil.nextInt(random, 5, 100);
    if (VERBOSE) {
      System.out.println("TEST: NUM_DOCS=" + NUM_DOCS + " maxLength=" + maxLength + " allowDups=" + allowDups);
    }

    int numDocs = 0;
    final List<BytesRef> docValues = new ArrayList<>();
    // TODO: deletions
    while (numDocs < NUM_DOCS) {
      final Document doc = new Document();

      // 10% of the time, the document is missing the value:
      final BytesRef br;
      if (random().nextInt(10) != 7) {
        final String s;
        if (random.nextBoolean()) {
          s = TestUtil.randomSimpleString(random, maxLength);
        } else {
          s = TestUtil.randomUnicodeString(random, maxLength);
        }

        if (!allowDups) {
          if (seen.contains(s)) {
            continue;
          }
          seen.add(s);
        }

        if (VERBOSE) {
          System.out.println("  " + numDocs + ": s=" + s);
        }

        doc.add(new StringField("stringdv", s, Field.Store.NO));
        docValues.add(new BytesRef(s));

      } else {
        br = null;
        if (VERBOSE) {
          System.out.println("  " + numDocs + ": <missing>");
        }
        docValues.add(null);
      }

      doc.add(new IntField("id", numDocs, Field.Store.YES));
      writer.addDocument(doc);
      numDocs++;

      if (random.nextInt(40) == 17) {
        // force flush
        writer.getReader().close();
      }
    }

    Map<String,UninvertingReader.Type> mapping = new HashMap<>();
    mapping.put("stringdv", Type.SORTED);
    mapping.put("id", Type.INTEGER);
    final IndexReader r = UninvertingReader.wrap(writer.getReader(), mapping);
    writer.close();
    if (VERBOSE) {
      System.out.println("  reader=" + r);
    }
    
    final IndexSearcher s = newSearcher(r, false);
    final int ITERS = atLeast(100);
    for(int iter=0;iter<ITERS;iter++) {
      final boolean reverse = random.nextBoolean();

      final TopFieldDocs hits;
      final SortField sf;
      final boolean sortMissingLast;
      final boolean missingIsNull;
      sf = new SortField("stringdv", type, reverse);
      sortMissingLast = random().nextBoolean();
      missingIsNull = true;

      if (sortMissingLast) {
        sf.setMissingValue(SortField.STRING_LAST);
      }
      
      final Sort sort;
      if (random.nextBoolean()) {
        sort = new Sort(sf);
      } else {
        sort = new Sort(sf, SortField.FIELD_DOC);
      }
      final int hitCount = TestUtil.nextInt(random, 1, r.maxDoc() + 20);
      final RandomQuery f = new RandomQuery(random.nextLong(), random.nextFloat(), docValues);
      int queryType = random.nextInt(2);
      if (queryType == 0) {
        hits = s.search(new ConstantScoreQuery(f),
                        hitCount, sort, random.nextBoolean(), random.nextBoolean());
      } else {
        hits = s.search(f, hitCount, sort, random.nextBoolean(), random.nextBoolean());
      }

      if (VERBOSE) {
        System.out.println("\nTEST: iter=" + iter + " " + hits.totalHits + " hits; topN=" + hitCount + "; reverse=" + reverse + "; sortMissingLast=" + sortMissingLast + " sort=" + sort);
      }

      // Compute expected results:
      Collections.sort(f.matchValues, new Comparator<BytesRef>() {
          @Override
          public int compare(BytesRef a, BytesRef b) {
            if (a == null) {
              if (b == null) {
                return 0;
              }
              if (sortMissingLast) {
                return 1;
              } else {
                return -1;
              }
            } else if (b == null) {
              if (sortMissingLast) {
                return -1;
              } else {
                return 1;
              }
            } else {
              return a.compareTo(b);
            }
          }
        });

      if (reverse) {
        Collections.reverse(f.matchValues);
      }
      final List<BytesRef> expected = f.matchValues;
      if (VERBOSE) {
        System.out.println("  expected:");
        for(int idx=0;idx<expected.size();idx++) {
          BytesRef br = expected.get(idx);
          if (br == null && missingIsNull == false) {
            br = new BytesRef();
          }
          System.out.println("    " + idx + ": " + (br == null ? "<missing>" : br.utf8ToString()));
          if (idx == hitCount-1) {
            break;
          }
        }
      }
      
      if (VERBOSE) {
        System.out.println("  actual:");
        for(int hitIDX=0;hitIDX<hits.scoreDocs.length;hitIDX++) {
          final FieldDoc fd = (FieldDoc) hits.scoreDocs[hitIDX];
          BytesRef br = (BytesRef) fd.fields[0];

          System.out.println("    " + hitIDX + ": " + (br == null ? "<missing>" : br.utf8ToString()) + " id=" + s.doc(fd.doc).get("id"));
        }
      }
      for(int hitIDX=0;hitIDX<hits.scoreDocs.length;hitIDX++) {
        final FieldDoc fd = (FieldDoc) hits.scoreDocs[hitIDX];
        BytesRef br = expected.get(hitIDX);
        if (br == null && missingIsNull == false) {
          br = new BytesRef();
        }

        // Normally, the old codecs (that don't support
        // docsWithField via doc values) will always return
        // an empty BytesRef for the missing case; however,
        // if all docs in a given segment were missing, in
        // that case it will return null!  So we must map
        // null here, too:
        BytesRef br2 = (BytesRef) fd.fields[0];
        if (br2 == null && missingIsNull == false) {
          br2 = new BytesRef();
        }
        
        assertEquals(br, br2);
      }
    }

    r.close();
    dir.close();
  }
  
  private static class RandomQuery extends Query {
    private final long seed;
    private float density;
    private final List<BytesRef> docValues;
    public final List<BytesRef> matchValues = Collections.synchronizedList(new ArrayList<BytesRef>());

    // density should be 0.0 ... 1.0
    public RandomQuery(long seed, float density, List<BytesRef> docValues) {
      this.seed = seed;
      this.density = density;
      this.docValues = docValues;
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, boolean needsScores) throws IOException {
      return new ConstantScoreWeight(this) {
        @Override
        public Scorer scorer(LeafReaderContext context) throws IOException {
          Random random = new Random(seed ^ context.docBase);
          final int maxDoc = context.reader().maxDoc();
          final NumericDocValues idSource = DocValues.getNumeric(context.reader(), "id");
          assertNotNull(idSource);
          final FixedBitSet bits = new FixedBitSet(maxDoc);
          for(int docID=0;docID<maxDoc;docID++) {
            if (random.nextFloat() <= density) {
              bits.set(docID);
              //System.out.println("  acc id=" + idSource.getInt(docID) + " docID=" + docID);
              matchValues.add(docValues.get((int) idSource.get(docID)));
            }
          }

          return new ConstantScoreScorer(this, score(), new BitSetIterator(bits, bits.approximateCardinality()));
        }
      };
    }

    @Override
    public String toString(String field) {
      return "RandomFilter(density=" + density + ")";
    }

    @Override
    public boolean equals(Object obj) {
      if (super.equals(obj) == false) {
        return false;
      }
      RandomQuery other = (RandomQuery) obj;
      return seed == other.seed && docValues == other.docValues;
    }

    @Override
    public int hashCode() {
      int h = Objects.hash(seed, density);
      h = 31 * h + System.identityHashCode(docValues);
      h = 31 * h + super.hashCode();
      return h;
    }
  }
}
