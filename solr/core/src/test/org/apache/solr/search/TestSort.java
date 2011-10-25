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

package org.apache.solr.search;

import java.io.IOException;
import java.util.*;

import org.apache.lucene.analysis.core.SimpleAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexReader.AtomicReaderContext;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.*;
import org.apache.lucene.search.SortField.Type;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.OpenBitSet;
import org.apache.lucene.util._TestUtil;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.request.SolrQueryRequest;
import org.junit.BeforeClass;

public class TestSort extends SolrTestCaseJ4 {
  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig.xml","schema-minimal.xml");
  }

  final Random r = random;

  int ndocs = 77;
  int iter = 50;
  int qiter = 1000;
  int commitCount = ndocs/5 + 1;
  int maxval = ndocs*2;

  static class MyDoc {
    int doc;
    String val;
    String val2;

    @Override
    public String toString() {
      return "{id=" +doc + " val1="+val + " val2="+val2 + "}";
    }
  }

  public void testRandomFieldNameSorts() throws Exception {
    SolrQueryRequest req = lrf.makeRequest("q", "*:*");

    final int iters = atLeast(5000);
    int numberOfOddities = 0;

    for (int i = 0; i < iters; i++) {
      final StringBuilder input = new StringBuilder();
      final String[] names = new String[_TestUtil.nextInt(r,1,10)];
      final boolean[] reverse = new boolean[names.length];
      for (int j = 0; j < names.length; j++) {
        names[j] = _TestUtil.randomRealisticUnicodeString(r, 1, 20);

        // reduce the likelyhood that the random str is a valid query or func 
        names[j] = names[j].replaceFirst("\\{","\\{\\{");
        names[j] = names[j].replaceFirst("\\(","\\(\\(");
        names[j] = names[j].replaceFirst("(\\\"|\\')","$1$1");
        names[j] = names[j].replaceFirst("(\\d)","$1x");

        // eliminate pesky problem chars
        names[j] = names[j].replaceAll("\\p{Cntrl}|\\p{javaWhitespace}","");

        if (0 == names[j].length()) {
          numberOfOddities++;
          // screw it, i'm taking my toys and going home
          names[j] = "last_ditch_i_give_up";
        }
        reverse[j] = r.nextBoolean();

        input.append(r.nextBoolean() ? " " : "");
        input.append(names[j]);
        input.append(" ");
        input.append(reverse[j] ? "desc," : "asc,");
      }
      input.deleteCharAt(input.length()-1);
      SortField[] sorts = null;
      try {
        sorts = QueryParsing.parseSort(input.toString(), req).getSort();
      } catch (RuntimeException e) {
        throw new RuntimeException("Failed to parse sort: " + input, e);
      }
      assertEquals("parsed sorts had unexpected size", 
                   names.length, sorts.length);
      for (int j = 0; j < names.length; j++) {
        assertEquals("sorts["+j+"] had unexpected reverse: " + input,
                     reverse[j], sorts[j].getReverse());

        final Type type = sorts[j].getType();

        if (Type.SCORE.equals(type)) {
          numberOfOddities++;
          assertEquals("sorts["+j+"] is (unexpectedly) type score : " + input,
                       "score", names[j]);
        } else if (Type.DOC.equals(type)) {
          numberOfOddities++;
          assertEquals("sorts["+j+"] is (unexpectedly) type doc : " + input,
                       "_docid_", names[j]);
        } else if (Type.CUSTOM.equals(type) || Type.REWRITEABLE.equals(type)) {
          numberOfOddities++;

          // our orig string better be parsable as a func/query
          QParser qp = 
            QParser.getParser(names[j], FunctionQParserPlugin.NAME, req);
          try { 
            Query q = qp.getQuery();
            assertNotNull("sorts["+j+"] had type " + type + 
                          " but parsed to null func/query: " + input, q);
          } catch (Exception e) {
            assertNull("sorts["+j+"] had type " + type + 
                       " but errored parsing as func/query: " + input, e);
          }
        } else {
          assertEquals("sorts["+j+"] had unexpected field: " + input,
                       names[j], sorts[j].getField());
        }
      }
    }

    assertTrue("Over 0.2% oddities in test: " +
               numberOfOddities + "/" + iters +
               " have func/query parsing semenatics gotten broader?",
               numberOfOddities < 0.002 * iters);
  }



  public void testSort() throws Exception {
    Directory dir = new RAMDirectory();
    Field f = new Field("f", "0", StringField.TYPE_UNSTORED);
    Field f2 = new Field("f2", "0", StringField.TYPE_UNSTORED);

    for (int iterCnt = 0; iterCnt<iter; iterCnt++) {
      IndexWriter iw = new IndexWriter(
          dir,
          new IndexWriterConfig(TEST_VERSION_CURRENT, new SimpleAnalyzer(TEST_VERSION_CURRENT)).
              setOpenMode(IndexWriterConfig.OpenMode.CREATE)
      );
      final MyDoc[] mydocs = new MyDoc[ndocs];

      int v1EmptyPercent = 50;
      int v2EmptyPercent = 50;

      int commitCountdown = commitCount;
      for (int i=0; i< ndocs; i++) {
        MyDoc mydoc = new MyDoc();
        mydoc.doc = i;
        mydocs[i] = mydoc;

        Document document = new Document();
        if (r.nextInt(100) < v1EmptyPercent) {
          mydoc.val = Integer.toString(r.nextInt(maxval));
          f.setValue(mydoc.val);
          document.add(f);
        }
        if (r.nextInt(100) < v2EmptyPercent) {
          mydoc.val2 = Integer.toString(r.nextInt(maxval));
          f2.setValue(mydoc.val2);
          document.add(f2);
        }


        iw.addDocument(document);
        if (--commitCountdown <= 0) {
          commitCountdown = commitCount;
          iw.commit();
        }
      }
      iw.close();


      IndexSearcher searcher = new IndexSearcher(dir, true);
      // System.out.println("segments="+searcher.getIndexReader().getSequentialSubReaders().length);
      assertTrue(searcher.getIndexReader().getSequentialSubReaders().length > 1);

      for (int i=0; i<qiter; i++) {
        Filter filt = new Filter() {
          @Override
          public DocIdSet getDocIdSet(AtomicReaderContext context, Bits acceptDocs) throws IOException {
            return BitsFilteredDocIdSet.wrap(randSet(context.reader.maxDoc()), acceptDocs);
          }
        };

        int top = r.nextInt((ndocs>>3)+1)+1;
        final boolean luceneSort = r.nextBoolean();
        final boolean sortMissingLast = !luceneSort && r.nextBoolean();
        final boolean sortMissingFirst = !luceneSort && !sortMissingLast;
        final boolean reverse = r.nextBoolean();
        List<SortField> sfields = new ArrayList<SortField>();

        final boolean secondary = r.nextBoolean();
        final boolean luceneSort2 = r.nextBoolean();
        final boolean sortMissingLast2 = !luceneSort2 && r.nextBoolean();
        final boolean sortMissingFirst2 = !luceneSort2 && !sortMissingLast2;
        final boolean reverse2 = r.nextBoolean();

        if (r.nextBoolean()) sfields.add( new SortField(null, SortField.Type.SCORE));
        // hit both use-cases of sort-missing-last
        sfields.add( Sorting.getStringSortField("f", reverse, sortMissingLast, sortMissingFirst) );
        if (secondary) {
          sfields.add( Sorting.getStringSortField("f2", reverse2, sortMissingLast2, sortMissingFirst2) );
        }
        if (r.nextBoolean()) sfields.add( new SortField(null, SortField.Type.SCORE));

        Sort sort = new Sort(sfields.toArray(new SortField[sfields.size()]));

        final String nullRep = luceneSort || sortMissingFirst && !reverse || sortMissingLast && reverse ? "" : "zzz";
        final String nullRep2 = luceneSort2 || sortMissingFirst2 && !reverse2 || sortMissingLast2 && reverse2 ? "" : "zzz";

        boolean trackScores = r.nextBoolean();
        boolean trackMaxScores = r.nextBoolean();
        boolean scoreInOrder = r.nextBoolean();
        final TopFieldCollector topCollector = TopFieldCollector.create(sort, top, true, trackScores, trackMaxScores, scoreInOrder);

        final List<MyDoc> collectedDocs = new ArrayList<MyDoc>();
        // delegate and collect docs ourselves
        Collector myCollector = new Collector() {
          int docBase;

          @Override
          public void setScorer(Scorer scorer) throws IOException {
            topCollector.setScorer(scorer);
          }

          @Override
          public void collect(int doc) throws IOException {
            topCollector.collect(doc);
            collectedDocs.add(mydocs[doc + docBase]);
          }

          @Override
          public void setNextReader(AtomicReaderContext context) throws IOException {
            topCollector.setNextReader(context);
            docBase = context.docBase;
          }

          @Override
          public boolean acceptsDocsOutOfOrder() {
            return topCollector.acceptsDocsOutOfOrder();
          }
        };

        searcher.search(new MatchAllDocsQuery(), filt, myCollector);

        Collections.sort(collectedDocs, new Comparator<MyDoc>() {
          public int compare(MyDoc o1, MyDoc o2) {
            String v1 = o1.val==null ? nullRep : o1.val;
            String v2 = o2.val==null ? nullRep : o2.val;
            int cmp = v1.compareTo(v2);
            if (reverse) cmp = -cmp;
            if (cmp != 0) return cmp;

            if (secondary) {
               v1 = o1.val2==null ? nullRep2 : o1.val2;
               v2 = o2.val2==null ? nullRep2 : o2.val2;
               cmp = v1.compareTo(v2);
               if (reverse2) cmp = -cmp;
            }

            cmp = cmp==0 ? o1.doc-o2.doc : cmp;
            return cmp;
          }
        });


        TopDocs topDocs = topCollector.topDocs();
        ScoreDoc[] sdocs = topDocs.scoreDocs;
        for (int j=0; j<sdocs.length; j++) {
          int id = sdocs[j].doc;
          if (id != collectedDocs.get(j).doc) {
            log.error("Error at pos " + j
            + "\n\tsortMissingFirst=" + sortMissingFirst + " sortMissingLast=" + sortMissingLast + " reverse=" + reverse
            + "\n\tEXPECTED=" + collectedDocs 
            );
          }
          assertEquals(id, collectedDocs.get(j).doc);
        }
      }
      searcher.close();
    }
    dir.close();

  }

  public DocIdSet randSet(int sz) {
    OpenBitSet obs = new OpenBitSet(sz);
    int n = r.nextInt(sz);
    for (int i=0; i<n; i++) {
      obs.fastSet(r.nextInt(sz));
    }
    return obs;
  }  
  

}
