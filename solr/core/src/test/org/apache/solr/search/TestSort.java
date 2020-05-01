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
package org.apache.solr.search;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.lucene.analysis.core.SimpleAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.FilterCollector;
import org.apache.lucene.search.FilterLeafCollector;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField.Type;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopFieldCollector;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.BitDocIdSet;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.TestUtil;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.uninverting.UninvertingReader;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestSort extends SolrTestCaseJ4 {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig.xml","schema-minimal.xml");
  }

  Random r;

  int ndocs = 77;
  int iter = 50;
  int qiter = 1000;
  int commitCount = ndocs/5 + 1;
  int maxval = ndocs*2;

  @Override
  public void setUp() throws Exception {
    super.setUp();
    r = random();
  }

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

    // infinite loop abort when trying to generate a non-blank sort "name"
    final int nonBlankAttempts = 37;

    for (int i = 0; i < iters; i++) {
      final StringBuilder input = new StringBuilder();
      final String[] names = new String[TestUtil.nextInt(r, 1, 10)];
      final boolean[] reverse = new boolean[names.length];
      for (int j = 0; j < names.length; j++) {
        names[j] = null;
        for (int k = 0; k < nonBlankAttempts && null == names[j]; k++) {
          names[j] = TestUtil.randomRealisticUnicodeString(r, 1, 100);

          // munge anything that might make this a function
          names[j] = names[j].replaceFirst("\\{","\\}\\{");
          names[j] = names[j].replaceFirst("\\(","\\)\\(");
          names[j] = names[j].replaceFirst("(\\\"|\\')","$1$1z");
          names[j] = names[j].replaceFirst("(\\d)","$1x");

          // eliminate pesky problem chars
          names[j] = names[j].replaceAll("\\p{Cntrl}|\\p{javaWhitespace}","");
          
          if (0 == names[j].length()) {
            names[j] = null;
          }
        }
        // with luck this bad, never go to vegas
        // alternatively: if (null == names[j]) names[j] = "never_go_to_vegas";
        assertNotNull("Unable to generate a (non-blank) names["+j+"] after "
                      + nonBlankAttempts + " attempts", names[j]);

        reverse[j] = r.nextBoolean();

        input.append(r.nextBoolean() ? " " : "");
        input.append(names[j]);
        input.append(" ");
        input.append(reverse[j] ? "desc," : "asc,");
      }
      input.deleteCharAt(input.length()-1);
      SortField[] sorts = null;
      List<SchemaField> fields = null;
      try {
        SortSpec spec = SortSpecParsing.parseSortSpec(input.toString(), req);
        sorts = spec.getSort().getSort();
        fields = spec.getSchemaFields();
      } catch (RuntimeException e) {
        throw new RuntimeException("Failed to parse sort: " + input, e);
      }
      assertEquals("parsed sorts had unexpected size", 
                   names.length, sorts.length);
      assertEquals("parsed sort schema fields had unexpected size", 
                   names.length, fields.size());
      for (int j = 0; j < names.length; j++) {
        assertEquals("sorts["+j+"] had unexpected reverse: " + input,
                     reverse[j], sorts[j].getReverse());

        final Type type = sorts[j].getType();

        if (Type.SCORE.equals(type)) {
          assertEquals("sorts["+j+"] is (unexpectedly) type score : " + input,
                       "score", names[j]);
        } else if (Type.DOC.equals(type)) {
          assertEquals("sorts["+j+"] is (unexpectedly) type doc : " + input,
                       "_docid_", names[j]);
        } else if (Type.CUSTOM.equals(type) || Type.REWRITEABLE.equals(type)) {
          log.error("names[{}] : {}", j, names[j]);
          log.error("sorts[{}] : {}", j, sorts[j]);
          fail("sorts["+j+"] resulted in a '" + type.toString()
               + "', either sort parsing code is broken, or func/query " 
               + "semantics have gotten broader and munging in this test "
               + "needs improved: " + input);

        } else {
          assertEquals("sorts["+j+"] ("+type.toString()+
                       ") had unexpected field in: " + input,
                       names[j], sorts[j].getField());
          assertEquals("fields["+j+"] ("+type.toString()+
                       ") had unexpected name in: " + input,
                       names[j], fields.get(j).getName());
        }
      }
    }
  }



  public void testSort() throws Exception {
    Directory dir = new RAMDirectory();
    Field f = new StringField("f", "0", Field.Store.NO);
    Field f2 = new StringField("f2", "0", Field.Store.NO);

    for (int iterCnt = 0; iterCnt<iter; iterCnt++) {
      IndexWriter iw = new IndexWriter(
          dir,
          new IndexWriterConfig(new SimpleAnalyzer()).
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
          f.setStringValue(mydoc.val);
          document.add(f);
        }
        if (r.nextInt(100) < v2EmptyPercent) {
          mydoc.val2 = Integer.toString(r.nextInt(maxval));
          f2.setStringValue(mydoc.val2);
          document.add(f2);
        }


        iw.addDocument(document);
        if (--commitCountdown <= 0) {
          commitCountdown = commitCount;
          iw.commit();
        }
      }
      iw.close();

      Map<String,UninvertingReader.Type> mapping = new HashMap<>();
      mapping.put("f", UninvertingReader.Type.SORTED);
      mapping.put("f2", UninvertingReader.Type.SORTED);

      DirectoryReader reader = UninvertingReader.wrap(DirectoryReader.open(dir), mapping);
      IndexSearcher searcher = new IndexSearcher(reader);
      // System.out.println("segments="+searcher.getIndexReader().getSequentialSubReaders().length);
      assertTrue(reader.leaves().size() > 1);

      for (int i=0; i<qiter; i++) {
        Filter filt = new Filter() {
          @Override
          public DocIdSet getDocIdSet(LeafReaderContext context, Bits acceptDocs) {
            return BitsFilteredDocIdSet.wrap(randSet(context.reader().maxDoc()), acceptDocs);
          }
          @Override
          public String toString(String field) {
            return "TestSortFilter";
          }

          @Override
          public boolean equals(Object other) {
            return other == this;
          }
          
          @Override
          public int hashCode() {
            return System.identityHashCode(this);
          }          
        };

        int top = r.nextInt((ndocs>>3)+1)+1;
        final boolean luceneSort = r.nextBoolean();
        final boolean sortMissingLast = !luceneSort && r.nextBoolean();
        final boolean sortMissingFirst = !luceneSort && !sortMissingLast;
        final boolean reverse = r.nextBoolean();
        List<SortField> sfields = new ArrayList<>();

        final boolean secondary = r.nextBoolean();
        final boolean luceneSort2 = r.nextBoolean();
        final boolean sortMissingLast2 = !luceneSort2 && r.nextBoolean();
        final boolean sortMissingFirst2 = !luceneSort2 && !sortMissingLast2;
        final boolean reverse2 = r.nextBoolean();

        if (r.nextBoolean()) sfields.add( new SortField(null, SortField.Type.SCORE));
        // hit both use-cases of sort-missing-last
        sfields.add( getStringSortField("f", reverse, sortMissingLast, sortMissingFirst) );
        if (secondary) {
          sfields.add( getStringSortField("f2", reverse2, sortMissingLast2, sortMissingFirst2) );
        }
        if (r.nextBoolean()) sfields.add( new SortField(null, SortField.Type.SCORE));

        Sort sort = new Sort(sfields.toArray(new SortField[sfields.size()]));

        final String nullRep = luceneSort || sortMissingFirst && !reverse || sortMissingLast && reverse ? "" : "zzz";
        final String nullRep2 = luceneSort2 || sortMissingFirst2 && !reverse2 || sortMissingLast2 && reverse2 ? "" : "zzz";

        boolean scoreInOrder = r.nextBoolean();
        final TopFieldCollector topCollector = TopFieldCollector.create(sort, top, Integer.MAX_VALUE);

        final List<MyDoc> collectedDocs = new ArrayList<>();
        // delegate and collect docs ourselves
        Collector myCollector = new FilterCollector(topCollector) {

          @Override
          public LeafCollector getLeafCollector(LeafReaderContext context)
              throws IOException {
            final int docBase = context.docBase;
            return new FilterLeafCollector(super.getLeafCollector(context)) {
              @Override
              public void collect(int doc) throws IOException {
                super.collect(doc);
                collectedDocs.add(mydocs[docBase + doc]);
              }
            };
          }

        };

        searcher.search(filt, myCollector);

        Collections.sort(collectedDocs, (o1, o2) -> {
          String v1 = o1.val == null ? nullRep : o1.val;
          String v2 = o2.val == null ? nullRep : o2.val;
          int cmp = v1.compareTo(v2);
          if (reverse) cmp = -cmp;
          if (cmp != 0) return cmp;

          if (secondary) {
            v1 = o1.val2 == null ? nullRep2 : o1.val2;
            v2 = o2.val2 == null ? nullRep2 : o2.val2;
            cmp = v1.compareTo(v2);
            if (reverse2) cmp = -cmp;
          }

          cmp = cmp == 0 ? o1.doc - o2.doc : cmp;
          return cmp;
        });


        TopDocs topDocs = topCollector.topDocs();
        ScoreDoc[] sdocs = topDocs.scoreDocs;
        for (int j=0; j<sdocs.length; j++) {
          int id = sdocs[j].doc;
          if (id != collectedDocs.get(j).doc) {
            log.error("Error at pos {}\n\tsortMissingFirst={} sortMissingLast={} reverse={}\n\tEXPECTED={}"
                , j, sortMissingFirst, sortMissingLast, reverse, collectedDocs
            );
          }
          assertEquals(id, collectedDocs.get(j).doc);
        }
      }
      reader.close();
    }
    dir.close();

  }

  public DocIdSet randSet(int sz) {
    FixedBitSet obs = new FixedBitSet(sz);
    int n = r.nextInt(sz);
    for (int i=0; i<n; i++) {
      obs.set(r.nextInt(sz));
    }
    return new BitDocIdSet(obs);
  }  
  
  private static SortField getStringSortField(String fieldName, boolean reverse, boolean nullLast, boolean nullFirst) {
    SortField sortField = new SortField(fieldName, SortField.Type.STRING, reverse);
    
    // 4 cases:
    // missingFirst / forward: default lucene behavior
    // missingFirst / reverse: set sortMissingLast
    // missingLast  / forward: set sortMissingLast
    // missingLast  / reverse: default lucene behavior
    
    if (nullFirst && reverse) {
      sortField.setMissingValue(SortField.STRING_LAST);
    } else if (nullLast && !reverse) {
      sortField.setMissingValue(SortField.STRING_LAST);
    }
    return sortField;
  }
}
