package org.apache.lucene.search;

/**
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

import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.store.MockRAMDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.util._TestUtil;
import org.apache.lucene.util.FieldCacheSanityChecker;
import org.apache.lucene.util.FieldCacheSanityChecker.Insanity;
import org.apache.lucene.util.FieldCacheSanityChecker.InsanityType;

import java.util.Random;
import java.util.Arrays;

public class TestStressSort extends LuceneTestCase {

  private final static int NUM_DOCS = 5000;
  // NOTE: put seed in here to make failures
  // deterministic, but do not commit with a seed (to
  // better test):
  private Random r;
  private Directory dir, dir2, dir3;
  private IndexSearcher searcherMultiSegment;
  private IndexSearcher searcherFewSegment;
  private IndexSearcher searcherSingleSegment;

  private static final boolean VERBOSE = false;

  // min..max
  private int nextInt(int min, int max) {
    return min + r.nextInt(max-min+1);
  }

  // 0..(lim-1)
  private int nextInt(int lim) {
    return r.nextInt(lim);
  }

  final char[] buffer = new char[20];
  private String randomString(int size) {
    assert size < 20;
    for(int i=0;i<size;i++) {
      buffer[i] = (char) nextInt(48, 122);
    }
    return new String(buffer, 0, size);
  }

  private void create() throws Throwable {

    // NOTE: put seed in here to make failures
    // deterministic, but do not commit with a seed (to
    // better test):
    dir = new MockRAMDirectory();
    IndexWriter writer = new IndexWriter(dir, new StandardAnalyzer(), IndexWriter.MaxFieldLength.LIMITED);
    writer.setMaxBufferedDocs(17);

    final Document doc = new Document();
    final Document doc2 = new Document();

    final Field id = new Field("id", "", Field.Store.YES, Field.Index.NO);
    doc.add(id);
    doc2.add(id);

    final Field contents = new Field("contents", "", Field.Store.NO, Field.Index.ANALYZED);
    doc.add(contents);
    doc2.add(contents);

    final Field byteField = new Field("byte", "", Field.Store.NO, Field.Index.NOT_ANALYZED);
    doc.add(byteField);
    doc2.add(byteField);

    final Field shortField = new Field("short", "", Field.Store.NO, Field.Index.NOT_ANALYZED);
    doc.add(shortField);
    doc2.add(shortField);

    final Field intField = new Field("int", "", Field.Store.NO, Field.Index.NOT_ANALYZED);
    doc.add(intField);
    doc2.add(intField);

    final Field longField = new Field("long", "", Field.Store.NO, Field.Index.NOT_ANALYZED);
    doc.add(longField);
    doc2.add(longField);

    final Field floatField = new Field("float", "", Field.Store.NO, Field.Index.NOT_ANALYZED);
    doc.add(floatField);
    doc2.add(floatField);

    final Field doubleField = new Field("double", "", Field.Store.NO, Field.Index.NOT_ANALYZED);
    doc.add(doubleField);
    doc2.add(doubleField);

    // we use two diff string fields so our FieldCache usage
    // is less suspicious to cache inspection
    final Field stringField = new Field("string", "", Field.Store.NO, Field.Index.NOT_ANALYZED);
    doc.add(stringField);
    final Field stringFieldIdx = new Field("stringIdx", "", Field.Store.NO, Field.Index.NOT_ANALYZED);
    doc.add(stringFieldIdx);
    // doc2 doesn't have stringField or stringFieldIdx, so we get nulls

    for(int i=0;i<NUM_DOCS;i++) {
      id.setValue(""+i);
      if (i % 1000 == 0) {
        contents.setValue("a b c z");
      } else if (i % 100 == 0) {
        contents.setValue("a b c y");
      } else if (i % 10 == 0) {
        contents.setValue("a b c x");
      } else {
        contents.setValue("a b c");
      }
      byteField.setValue(""+nextInt(Byte.MIN_VALUE, Byte.MAX_VALUE));
      if (nextInt(10) == 3) {
        shortField.setValue(""+Short.MIN_VALUE);
      } else if (nextInt(10) == 7) {
        shortField.setValue(""+Short.MAX_VALUE);
      } else {
        shortField.setValue(""+nextInt(Short.MIN_VALUE, Short.MAX_VALUE));
      }

      if (nextInt(10) == 3) {
        intField.setValue(""+Integer.MIN_VALUE);
      } else if (nextInt(10) == 7) {
        intField.setValue(""+Integer.MAX_VALUE);
      } else {
        intField.setValue(""+r.nextInt());
      }

      if (nextInt(10) == 3) {
        longField.setValue(""+Long.MIN_VALUE);
      } else if (nextInt(10) == 7) {
        longField.setValue(""+Long.MAX_VALUE);
      } else {
        longField.setValue(""+r.nextLong());
      }
      floatField.setValue(""+r.nextFloat());
      doubleField.setValue(""+r.nextDouble());
      if (i % 197 == 0) {
        writer.addDocument(doc2);
      } else {
        String r = randomString(nextInt(20));
        stringField.setValue(r);
        stringFieldIdx.setValue(r);
        writer.addDocument(doc);
      }
    }
    writer.close();
    searcherMultiSegment = new IndexSearcher(dir);
    searcherMultiSegment.setDefaultFieldSortScoring(true, true);

    dir2 = new MockRAMDirectory(dir);
    writer = new IndexWriter(dir2, new StandardAnalyzer(), IndexWriter.MaxFieldLength.LIMITED);
    writer.optimize();
    writer.close();
    searcherSingleSegment = new IndexSearcher(dir2);
    searcherSingleSegment.setDefaultFieldSortScoring(true, true);
    dir3 = new MockRAMDirectory(dir);
    writer = new IndexWriter(dir3, new StandardAnalyzer(), IndexWriter.MaxFieldLength.LIMITED);
    writer.optimize(3);
    writer.close();
    searcherFewSegment = new IndexSearcher(dir3);
    searcherFewSegment.setDefaultFieldSortScoring(true, true);
  }

  private void close() throws Throwable {
    searcherMultiSegment.close();
    searcherFewSegment.close();
    searcherSingleSegment.close();
    dir.close();
    dir2.close();
  }

  public void testSort() throws Throwable {
    r = newRandom();

    // reverse & not
    // all types
    // restrictive & non restrictive searches (on contents)

    create();

    Sort[] sorts = new Sort[50];
    int sortCount = 0;

    for(int r=0;r<2;r++) {
      Sort sort;
      boolean reverse = 1 == r;

      sorts[sortCount++] = sort = new Sort();
      sort.setSort(new SortField[] {new SortField("byte", SortField.BYTE, reverse)});
      
      sorts[sortCount++] = sort = new Sort();
      sort.setSort(new SortField[] {new SortField("short", SortField.SHORT, reverse)});

      sorts[sortCount++] = sort = new Sort();
      sort.setSort(new SortField[] {new SortField("int", SortField.INT, reverse)});

      sorts[sortCount++] = sort = new Sort();
      sort.setSort(new SortField[] {new SortField("long", SortField.LONG, reverse)});

      sorts[sortCount++] = sort = new Sort();
      sort.setSort(new SortField[] {new SortField("float", SortField.FLOAT, reverse)});

      sorts[sortCount++] = sort = new Sort();
      sort.setSort(new SortField[] {new SortField("double", SortField.DOUBLE, reverse)});

      sorts[sortCount++] = sort = new Sort();
      sort.setSort(new SortField[] {new SortField("string", SortField.STRING_VAL, reverse)});

      sorts[sortCount++] = sort = new Sort();
      sort.setSort(new SortField[] {new SortField("stringIdx", SortField.STRING, reverse)});

      //sorts[sortCount++] = sort = new Sort();
      //sort.setSort(new SortField[] {new SortField("string", SortField.STRING_ORD, reverse)});

      //sorts[sortCount++] = sort = new Sort();
      //sort.setSort(new SortField[] {new SortField("string", SortField.STRING_ORD_VAL, reverse)});

      //sorts[sortCount++] = sort = new Sort();
      //sort.setSort(new SortField[] {new SortField("string", SortField.STRING_ORD_VAL_DEM, reverse)});

      //sorts[sortCount++] = sort = new Sort();
      //sort.setSort(new SortField[] {new SortField("string", SortField.STRING_ORD_VAL_DEM2, reverse)});

      sorts[sortCount++] = sort = new Sort();
      sort.setSort(new SortField[] {new SortField(null, SortField.SCORE, reverse)});

      sorts[sortCount++] = sort = new Sort();
      sort.setSort(new SortField[] {new SortField(null, SortField.DOC, reverse)});
    }

    Query[] queries = new Query[4];
    queries[0] = new MatchAllDocsQuery();
    queries[1] = new TermQuery(new Term("contents", "x"));  // matches every 10th doc
    queries[2] = new TermQuery(new Term("contents", "y"));  // matches every 100th doc
    queries[3] = new TermQuery(new Term("contents", "z"));  // matches every 1000th doc

    for(int sx=0;sx<3;sx++) {
      final IndexSearcher searcher;
      if (sx == 0) {
        searcher = searcherSingleSegment;
      } else if (sx == 1) {
        searcher = searcherFewSegment;
      } else {
        searcher = searcherMultiSegment;
      }

      for(int qx=0;qx<queries.length;qx++) {
        final Query query = queries[qx];

        for(int q=0;q<3;q++) {

          final int queueSize;
          if (q == 0) {
            queueSize = 10;
          } else if (q == 1) {
            queueSize = 100;
          } else {
            queueSize = 1000;
          }
        
          for(int s=0;s<sortCount;s++) {
            Sort sort1 = sorts[s];

            for(int s2=-1;s2<sortCount;s2++) {
              Sort sort;
              if (s2 == -1) {
                // Single field sort
                sort = sort1;
              } else {
                sort = new Sort(new SortField[] {sort1.getSort()[0], sorts[s2].getSort()[0]});
              }

              // Old
              Sort oldSort = getOldSort(sort);

              if (VERBOSE) {
                System.out.println("query=" + query);
                if (sx == 0) {
                  System.out.println("  single-segment index");
                } else if (sx == 1) {
                  System.out.println("  few-segment index");
                } else {
                  System.out.println("  many-segment index");
                }
                System.out.println("  numHit=" + queueSize);
                System.out.println("  old=" + oldSort);
                System.out.println("  new=" + sort);
              }

              TopDocs newHits = searcher.search(query, null, queueSize, sort);
              TopDocs oldHits = searcher.search(query, null, queueSize, oldSort);

              compare(oldHits, newHits);
            }
          }
        }
      }
    }

    // we explicitly test the old sort method and
    // compare with the new, so we expect to see SUBREADER
    // sanity checks fail.
    Insanity[] insanity = FieldCacheSanityChecker.checkSanity
      (FieldCache.DEFAULT);
    try {
      int ignored = 0;
      for (int i = 0; i < insanity.length; i++) {
        if (insanity[i].getType() == InsanityType.SUBREADER) {
          insanity[i] = new Insanity(InsanityType.EXPECTED,
                                     insanity[i].getMsg(), 
                                     insanity[i].getCacheEntries());
          ignored++;
        }
      }
      assertEquals("Not all insane field cache usage was expected",
                   ignored, insanity.length);

      insanity = null;
    } finally {
      // report this in the event of any exception/failure
      // if no failure, then insanity will be null
      if (null != insanity) {
        dumpArray(getTestLabel() + ": Insane FieldCache usage(s)", insanity, System.err);
      }
    }
    // we've already checked FieldCache, purge so tearDown doesn't complain
    purgeFieldCache(FieldCache.DEFAULT); // so

    close();
  }

  private Sort getOldSort(Sort sort) {
    SortField[] fields = sort.getSort();
    SortField[] oldFields = new SortField[fields.length];
    for(int i=0;i<fields.length;i++) {
      int sortType;
      if (fields[i].getField() != null && fields[i].getField().equals("string")) {
        sortType = SortField.STRING;
      } else {
        sortType = fields[i].getType();
      }
      oldFields[i] = new SortField(fields[i].getField(),
                                   sortType,
                                   fields[i].getReverse());
      oldFields[i].setUseLegacySearch(true);
    }
    return new Sort(oldFields);
  }

  private void compare(TopDocs oldHits, TopDocs newHits) {
    assertEquals(oldHits.totalHits, newHits.totalHits);
    assertEquals(oldHits.scoreDocs.length, newHits.scoreDocs.length);
    final ScoreDoc[] oldDocs = oldHits.scoreDocs;
    final ScoreDoc[] newDocs = newHits.scoreDocs;

    for(int i=0;i<oldDocs.length;i++) {
      if (oldDocs[i] instanceof FieldDoc) {
        assert newDocs[i] instanceof FieldDoc;
        FieldDoc oldHit = (FieldDoc) oldDocs[i];
        FieldDoc newHit = (FieldDoc) newDocs[i];
        assertEquals("hit " + i + " of " + oldDocs.length + " differs: oldDoc=" + oldHit.doc + " vs newDoc=" + newHit.doc + " oldFields=" + _TestUtil.arrayToString(oldHit.fields) + " newFields=" + _TestUtil.arrayToString(newHit.fields),
                     oldHit.doc, newHit.doc);

        assertEquals(oldHit.score, newHit.score, 0.00001);
        assertTrue(Arrays.equals(oldHit.fields, newHit.fields));
      } else {
        ScoreDoc oldHit = oldDocs[i];
        ScoreDoc newHit = newDocs[i];
        assertEquals(oldHit.doc, newHit.doc);
        assertEquals(oldHit.score, newHit.score, 0.00001);
      }
    }
    
  }
}
