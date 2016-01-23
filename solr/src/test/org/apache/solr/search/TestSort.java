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

import org.apache.lucene.analysis.SimpleAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.*;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.OpenBitSet;
import org.apache.solr.util.AbstractSolrTestCase;

import java.io.IOException;
import java.util.*;

public class TestSort extends AbstractSolrTestCase {
  public String getSchemaFile() { return null; }
  public String getSolrConfigFile() { return null; }

  Random r = new Random();

  int ndocs = 77;
  int iter = 100;  
  int qiter = 1000;
  int commitCount = ndocs/5 + 1;
  int maxval = ndocs*2;

  static class MyDoc {
    int doc;
    String val;
  }

  public void testSort() throws Exception {
    RAMDirectory dir = new RAMDirectory();
    Document smallDoc = new Document();
    // Field id = new Field("id","0", Field.Store.NO, Field.Index.NOT_ANALYZED_NO_NORMS);
    Field f = new Field("f","0", Field.Store.NO, Field.Index.NOT_ANALYZED_NO_NORMS);
    smallDoc.add(f);

    Document emptyDoc = new Document();

    for (int iterCnt = 0; iterCnt<iter; iterCnt++) {
      IndexWriter iw = new IndexWriter(dir, new SimpleAnalyzer(), true, IndexWriter.MaxFieldLength.UNLIMITED);
      final MyDoc[] mydocs = new MyDoc[ndocs];

      int commitCountdown = commitCount;
      for (int i=0; i< ndocs; i++) {
        Document doc;
        MyDoc mydoc = new MyDoc();
        mydoc.doc = i;
        mydocs[i] = mydoc;

        if (r.nextInt(3)==0) {
          doc = emptyDoc;
          mydoc.val = null;
        } else {
          mydoc.val = Integer.toString(r.nextInt(maxval));
          f.setValue(mydoc.val);
          doc = smallDoc;
        }
        iw.addDocument(doc);
        if (--commitCountdown <= 0) {
          commitCountdown = commitCount;
          iw.commit();
        }
      }
      iw.close();

      /***
      Arrays.sort(mydocs, new Comparator<MyDoc>() {
        public int compare(MyDoc o1, MyDoc o2) {
          String v1 = o1.val==null ? "zzz" : o1.val;
          String v2 = o2.val==null ? "zzz" : o2.val;
          int cmp = v1.compareTo(v2);
          cmp = cmp==0 ? o1.doc-o2.doc : cmp;
          return cmp;
        }
      });
      ***/

      IndexSearcher searcher = new IndexSearcher(dir, true);
      // System.out.println("segments="+searcher.getIndexReader().getSequentialSubReaders().length);
      assertTrue(searcher.getIndexReader().getSequentialSubReaders().length > 1);

      for (int i=0; i<qiter; i++) {
        Filter filt = new Filter() {
          @Override
          public DocIdSet getDocIdSet(IndexReader reader) throws IOException {
            return randSet(reader.maxDoc());
          }
        };

        int top = r.nextInt((ndocs>>3)+1)+1;
        final boolean sortMissingLast = r.nextBoolean();
        final boolean reverse = !sortMissingLast;
        List<SortField> sfields = new ArrayList<SortField>();

        if (r.nextBoolean()) sfields.add( new SortField(null, SortField.SCORE));
        // hit both use-cases of sort-missing-last
        sfields.add( Sorting.getStringSortField("f", reverse, sortMissingLast, !sortMissingLast) );
        int sortIdx = sfields.size() - 1;
        if (r.nextBoolean()) sfields.add( new SortField(null, SortField.SCORE));

        Sort sort = new Sort(sfields.toArray(new SortField[sfields.size()]));

        // final String nullRep = sortMissingLast ? "zzz" : "";
        final String nullRep = "zzz";

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
          public void setNextReader(IndexReader reader, int docBase) throws IOException {
            topCollector.setNextReader(reader,docBase);
            this.docBase = docBase;
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
            cmp = cmp==0 ? o1.doc-o2.doc : cmp;
            return cmp;
          }
        });


        TopDocs topDocs = topCollector.topDocs();
        ScoreDoc[] sdocs = topDocs.scoreDocs;
        for (int j=0; j<sdocs.length; j++) {
          int id = sdocs[j].doc;
          String s = (String)((FieldDoc)sdocs[j]).fields[sortIdx];
          if (id != collectedDocs.get(j).doc) {
            System.out.println("Error at pos " + j);
          }
          assertEquals(id, collectedDocs.get(j).doc);
        }
      }
    }

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
