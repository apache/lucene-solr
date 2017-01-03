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

import java.util.Arrays;
import java.util.Collections;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoubleDocValuesField;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FloatDocValuesField;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.English;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;

public class TestDoubleValuesSource extends LuceneTestCase {

  private Directory dir;
  private IndexReader reader;
  private IndexSearcher searcher;

  @Override
  public void setUp() throws Exception {
    super.setUp();
    dir = newDirectory();
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir);
    int numDocs = TestUtil.nextInt(random(), 2049, 4000);
    for (int i = 0; i < numDocs; i++) {
      Document document = new Document();
      document.add(newTextField("english", English.intToEnglish(i), Field.Store.NO));
      document.add(newTextField("oddeven", (i % 2 == 0) ? "even" : "odd", Field.Store.NO));
      document.add(new NumericDocValuesField("int", random().nextInt()));
      document.add(new NumericDocValuesField("long", random().nextLong()));
      document.add(new FloatDocValuesField("float", random().nextFloat()));
      document.add(new DoubleDocValuesField("double", random().nextDouble()));
      if (i == 545)
        document.add(new DoubleDocValuesField("onefield", 45.72));
      iw.addDocument(document);
    }
    reader = iw.getReader();
    iw.close();
    searcher = newSearcher(reader);
  }

  @Override
  public void tearDown() throws Exception {
    reader.close();
    dir.close();
    super.tearDown();
  }

  public void testSortMissing() throws Exception {
    DoubleValuesSource onefield = DoubleValuesSource.fromDoubleField("onefield");
    TopDocs results = searcher.search(new MatchAllDocsQuery(), 1, new Sort(onefield.getSortField(true)));
    FieldDoc first = (FieldDoc) results.scoreDocs[0];
    assertEquals(45.72, first.fields[0]);
  }

  public void testSimpleFieldEquivalences() throws Exception {
    checkSorts(new MatchAllDocsQuery(), new Sort(new SortField("int", SortField.Type.INT, random().nextBoolean())));
    checkSorts(new MatchAllDocsQuery(), new Sort(new SortField("long", SortField.Type.LONG, random().nextBoolean())));
    checkSorts(new MatchAllDocsQuery(), new Sort(new SortField("float", SortField.Type.FLOAT, random().nextBoolean())));
    checkSorts(new MatchAllDocsQuery(), new Sort(new SortField("double", SortField.Type.DOUBLE, random().nextBoolean())));
  }

  public void testHashCodeAndEquals() {
    DoubleValuesSource vs1 = DoubleValuesSource.fromDoubleField("double");
    DoubleValuesSource vs2 = DoubleValuesSource.fromDoubleField("double");
    assertEquals(vs1, vs2);
    assertEquals(vs1.hashCode(), vs2.hashCode());
    DoubleValuesSource v3 = DoubleValuesSource.fromLongField("long");
    assertFalse(vs1.equals(v3));
  }

  public void testSimpleFieldSortables() throws Exception {
    int n = atLeast(4);
    for (int i = 0; i < n; i++) {
      Sort sort = randomSort();
      checkSorts(new MatchAllDocsQuery(), sort);
      checkSorts(new TermQuery(new Term("english", "one")), sort);
    }
  }

  Sort randomSort() throws Exception {
    boolean reversed = random().nextBoolean();
    SortField fields[] = new SortField[] {
        new SortField("int", SortField.Type.INT, reversed),
        new SortField("long", SortField.Type.LONG, reversed),
        new SortField("float", SortField.Type.FLOAT, reversed),
        new SortField("double", SortField.Type.DOUBLE, reversed),
        new SortField("score", SortField.Type.SCORE)
    };
    Collections.shuffle(Arrays.asList(fields), random());
    int numSorts = TestUtil.nextInt(random(), 1, fields.length);
    return new Sort(Arrays.copyOfRange(fields, 0, numSorts));
  }

  // Take a Sort, and replace any field sorts with Sortables
  Sort convertSortToSortable(Sort sort) {
    SortField original[] = sort.getSort();
    SortField mutated[] = new SortField[original.length];
    for (int i = 0; i < mutated.length; i++) {
      if (random().nextInt(3) > 0) {
        SortField s = original[i];
        boolean reverse = s.getType() == SortField.Type.SCORE || s.getReverse();
        switch (s.getType()) {
          case INT:
            mutated[i] = DoubleValuesSource.fromIntField(s.getField()).getSortField(reverse);
            break;
          case LONG:
            mutated[i] = DoubleValuesSource.fromLongField(s.getField()).getSortField(reverse);
            break;
          case FLOAT:
            mutated[i] = DoubleValuesSource.fromFloatField(s.getField()).getSortField(reverse);
            break;
          case DOUBLE:
            mutated[i] = DoubleValuesSource.fromDoubleField(s.getField()).getSortField(reverse);
            break;
          case SCORE:
            mutated[i] = DoubleValuesSource.SCORES.getSortField(reverse);
            break;
          default:
            mutated[i] = original[i];
        }
      } else {
        mutated[i] = original[i];
      }
    }

    return new Sort(mutated);
  }

  void checkSorts(Query query, Sort sort) throws Exception {
    int size = TestUtil.nextInt(random(), 1, searcher.getIndexReader().maxDoc() / 5);
    TopDocs expected = searcher.search(query, size, sort, random().nextBoolean(), random().nextBoolean());
    Sort mutatedSort = convertSortToSortable(sort);
    TopDocs actual = searcher.search(query, size, mutatedSort, random().nextBoolean(), random().nextBoolean());

    CheckHits.checkEqual(query, expected.scoreDocs, actual.scoreDocs);

    if (size < actual.totalHits) {
      expected = searcher.searchAfter(expected.scoreDocs[size-1], query, size, sort);
      actual = searcher.searchAfter(actual.scoreDocs[size-1], query, size, mutatedSort);
      CheckHits.checkEqual(query, expected.scoreDocs, actual.scoreDocs);
    }
  }
}
