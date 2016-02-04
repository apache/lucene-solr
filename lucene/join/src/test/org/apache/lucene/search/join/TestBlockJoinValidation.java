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
package org.apache.lucene.search.join;

import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;

public class TestBlockJoinValidation extends LuceneTestCase {

  public static final int AMOUNT_OF_SEGMENTS = 5;
  public static final int AMOUNT_OF_PARENT_DOCS = 10;
  public static final int AMOUNT_OF_CHILD_DOCS = 5;
  public static final int AMOUNT_OF_DOCS_IN_SEGMENT = AMOUNT_OF_PARENT_DOCS + AMOUNT_OF_PARENT_DOCS * AMOUNT_OF_CHILD_DOCS;

  private Directory directory;
  private IndexReader indexReader;
  private IndexSearcher indexSearcher;
  private BitSetProducer parentsFilter;

  @Override
  public void setUp() throws Exception {
    super.setUp();
    directory = newDirectory();
    final IndexWriterConfig config = new IndexWriterConfig(new MockAnalyzer(random()));
    final IndexWriter indexWriter = new IndexWriter(directory, config);
    for (int i = 0; i < AMOUNT_OF_SEGMENTS; i++) {
      List<Document> segmentDocs = createDocsForSegment(i);
      indexWriter.addDocuments(segmentDocs);
      indexWriter.commit();
    }
    indexReader = DirectoryReader.open(indexWriter);
    indexWriter.close();
    indexSearcher = new IndexSearcher(indexReader);
    parentsFilter = new QueryBitSetProducer(new WildcardQuery(new Term("parent", "*")));
  }

  @Override
  public void tearDown() throws Exception {
    indexReader.close();
    directory.close();
    super.tearDown();
  }

  public void testNextDocValidationForToParentBjq() throws Exception {
    Query parentQueryWithRandomChild = createChildrenQueryWithOneParent(getRandomChildNumber(0));
    ToParentBlockJoinQuery blockJoinQuery = new ToParentBlockJoinQuery(parentQueryWithRandomChild, parentsFilter, ScoreMode.None);
    try {
      indexSearcher.search(blockJoinQuery, 1);
      fail("didn't get expected exception");
    } catch (IllegalStateException expected) {
      assertTrue(expected.getMessage() != null && expected.getMessage().contains("child query must only match non-parent docs"));
    }
  }

  public void testAdvanceValidationForToParentBjq() throws Exception {
    int randomChildNumber = getRandomChildNumber(0);
    // we need to make advance method meet wrong document, so random child number
    // in BJQ must be greater than child number in Boolean clause
    int nextRandomChildNumber = getRandomChildNumber(randomChildNumber);
    Query parentQueryWithRandomChild = createChildrenQueryWithOneParent(nextRandomChildNumber);
    ToParentBlockJoinQuery blockJoinQuery = new ToParentBlockJoinQuery(parentQueryWithRandomChild, parentsFilter, ScoreMode.None);
    // advance() method is used by ConjunctionScorer, so we need to create Boolean conjunction query
    BooleanQuery.Builder conjunctionQuery = new BooleanQuery.Builder();
    WildcardQuery childQuery = new WildcardQuery(new Term("child", createFieldValue(randomChildNumber)));
    conjunctionQuery.add(new BooleanClause(childQuery, BooleanClause.Occur.MUST));
    conjunctionQuery.add(new BooleanClause(blockJoinQuery, BooleanClause.Occur.MUST));
    
    try {
      indexSearcher.search(conjunctionQuery.build(), 1);
      fail("didn't get expected exception");
    } catch (IllegalStateException expected) {
      assertTrue(expected.getMessage() != null && expected.getMessage().contains("child query must only match non-parent docs"));
    }
  }

  public void testNextDocValidationForToChildBjq() throws Exception {
    Query parentQueryWithRandomChild = createParentsQueryWithOneChild(getRandomChildNumber(0));

    ToChildBlockJoinQuery blockJoinQuery = new ToChildBlockJoinQuery(parentQueryWithRandomChild, parentsFilter);
    
    try {
      indexSearcher.search(blockJoinQuery, 1);
      fail("didn't get expected exception");
    } catch (IllegalStateException expected) {
      assertTrue(expected.getMessage() != null && expected.getMessage().contains(ToChildBlockJoinQuery.INVALID_QUERY_MESSAGE));
    }
  }

  public void testAdvanceValidationForToChildBjq() throws Exception {
    Query parentQuery = new MatchAllDocsQuery();
    ToChildBlockJoinQuery blockJoinQuery = new ToChildBlockJoinQuery(parentQuery, parentsFilter);

    final LeafReaderContext context = indexSearcher.getIndexReader().leaves().get(0);
    Weight weight = indexSearcher.createNormalizedWeight(blockJoinQuery, true);
    Scorer scorer = weight.scorer(context);
    final Bits parentDocs = parentsFilter.getBitSet(context);

    int target;
    do {
      // make the parent scorer advance to a doc ID which is not a parent
      target = TestUtil.nextInt(random(), 0, context.reader().maxDoc() - 2);
    } while (parentDocs.get(target + 1));

    try {
      scorer.iterator().advance(target);
      fail();
    } catch (IllegalStateException expected) {
      assertTrue(expected.getMessage() != null && expected.getMessage().contains(ToChildBlockJoinQuery.INVALID_QUERY_MESSAGE));
    }
  }

  private static List<Document> createDocsForSegment(int segmentNumber) {
    List<List<Document>> blocks = new ArrayList<>(AMOUNT_OF_PARENT_DOCS);
    for (int i = 0; i < AMOUNT_OF_PARENT_DOCS; i++) {
      blocks.add(createParentDocWithChildren(segmentNumber, i));
    }
    List<Document> result = new ArrayList<>(AMOUNT_OF_DOCS_IN_SEGMENT);
    for (List<Document> block : blocks) {
      result.addAll(block);
    }
    return result;
  }

  private static List<Document> createParentDocWithChildren(int segmentNumber, int parentNumber) {
    List<Document> result = new ArrayList<>(AMOUNT_OF_CHILD_DOCS + 1);
    for (int i = 0; i < AMOUNT_OF_CHILD_DOCS; i++) {
      result.add(createChildDoc(segmentNumber, parentNumber, i));
    }
    result.add(createParentDoc(segmentNumber, parentNumber));
    return result;
  }

  private static Document createParentDoc(int segmentNumber, int parentNumber) {
    Document result = new Document();
    result.add(newStringField("id", createFieldValue(segmentNumber * AMOUNT_OF_PARENT_DOCS + parentNumber), Field.Store.YES));
    result.add(newStringField("parent", createFieldValue(parentNumber), Field.Store.NO));
    result.add(newStringField("common_field", "1", Field.Store.NO));
    return result;
  }

  private static Document createChildDoc(int segmentNumber, int parentNumber, int childNumber) {
    Document result = new Document();
    result.add(newStringField("id", createFieldValue(segmentNumber * AMOUNT_OF_PARENT_DOCS + parentNumber, childNumber), Field.Store.YES));
    result.add(newStringField("child", createFieldValue(childNumber), Field.Store.NO));
    result.add(newStringField("common_field", "1", Field.Store.NO));
    return result;
  }

  private static String createFieldValue(int... documentNumbers) {
    StringBuilder stringBuilder = new StringBuilder();
    for (int documentNumber : documentNumbers) {
      if (stringBuilder.length() > 0) {
        stringBuilder.append("_");
      }
      stringBuilder.append(documentNumber);
    }
    return stringBuilder.toString();
  }

  private static Query createChildrenQueryWithOneParent(int childNumber) {
    TermQuery childQuery = new TermQuery(new Term("child", createFieldValue(childNumber)));
    Query randomParentQuery = new TermQuery(new Term("id", createFieldValue(getRandomParentId())));
    BooleanQuery.Builder childrenQueryWithRandomParent = new BooleanQuery.Builder();
    childrenQueryWithRandomParent.add(new BooleanClause(childQuery, BooleanClause.Occur.SHOULD));
    childrenQueryWithRandomParent.add(new BooleanClause(randomParentQuery, BooleanClause.Occur.SHOULD));
    return childrenQueryWithRandomParent.build();
  }

  private static Query createParentsQueryWithOneChild(int randomChildNumber) {
    BooleanQuery.Builder childQueryWithRandomParent = new BooleanQuery.Builder();
    Query parentsQuery = new TermQuery(new Term("parent", createFieldValue(getRandomParentNumber())));
    childQueryWithRandomParent.add(new BooleanClause(parentsQuery, BooleanClause.Occur.SHOULD));
    childQueryWithRandomParent.add(new BooleanClause(randomChildQuery(randomChildNumber), BooleanClause.Occur.SHOULD));
    return childQueryWithRandomParent.build();
  }

  private static int getRandomParentId() {
    return random().nextInt(AMOUNT_OF_PARENT_DOCS * AMOUNT_OF_SEGMENTS);
  }

  private static int getRandomParentNumber() {
    return random().nextInt(AMOUNT_OF_PARENT_DOCS);
  }

  private static Query randomChildQuery(int randomChildNumber) {
    return new TermQuery(new Term("id", createFieldValue(getRandomParentId(), randomChildNumber)));
  }

  private static int getRandomChildNumber(int notLessThan) {
    return notLessThan + random().nextInt(AMOUNT_OF_CHILD_DOCS - notLessThan);
  }

}
