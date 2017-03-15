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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.LogDocMergePolicy;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;

public class TestBlockJoin extends LuceneTestCase {

  // One resume...
  private Document makeResume(String name, String country) {
    Document resume = new Document();
    resume.add(newStringField("docType", "resume", Field.Store.NO));
    resume.add(newStringField("name", name, Field.Store.YES));
    resume.add(newStringField("country", country, Field.Store.NO));
    return resume;
  }

  // ... has multiple jobs
  private Document makeJob(String skill, int year) {
    Document job = new Document();
    job.add(newStringField("skill", skill, Field.Store.YES));
    job.add(new IntPoint("year", year));
    job.add(new StoredField("year", year));
    return job;
  }

  // ... has multiple qualifications
  private Document makeQualification(String qualification, int year) {
    Document job = new Document();
    job.add(newStringField("qualification", qualification, Field.Store.YES));
    job.add(new IntPoint("year", year));
    return job;
  }

  public void testExtractTerms() throws Exception {
    TermQuery termQuery = new TermQuery(new Term("field", "value"));
    QueryBitSetProducer bitSetProducer = new QueryBitSetProducer(new MatchNoDocsQuery());
    ToParentBlockJoinQuery toParentBlockJoinQuery = new ToParentBlockJoinQuery(termQuery, bitSetProducer, ScoreMode.None);
    ToChildBlockJoinQuery toChildBlockJoinQuery = new ToChildBlockJoinQuery(toParentBlockJoinQuery, bitSetProducer);

    Directory directory = newDirectory();
    final IndexWriter w = new IndexWriter(directory, new IndexWriterConfig(new MockAnalyzer(random())));
    w.close();
    IndexReader indexReader = DirectoryReader.open(directory);
    IndexSearcher indexSearcher = new IndexSearcher(indexReader);

    Weight weight = toParentBlockJoinQuery.createWeight(indexSearcher, false);
    Set<Term> terms = new HashSet<>();
    weight.extractTerms(terms);
    Term[] termArr =terms.toArray(new Term[0]);
    assertEquals(1, termArr.length);

    weight = toChildBlockJoinQuery.createWeight(indexSearcher, false);
    terms = new HashSet<>();
    weight.extractTerms(terms);
    termArr =terms.toArray(new Term[0]);
    assertEquals(1, termArr.length);

    indexReader.close();
    directory.close();
  }

  public void testEmptyChildFilter() throws Exception {
    final Directory dir = newDirectory();
    final IndexWriterConfig config = new IndexWriterConfig(new MockAnalyzer(random()));
    config.setMergePolicy(NoMergePolicy.INSTANCE);
    // we don't want to merge - since we rely on certain segment setup
    final IndexWriter w = new IndexWriter(dir, config);

    final List<Document> docs = new ArrayList<>();

    docs.add(makeJob("java", 2007));
    docs.add(makeJob("python", 2010));
    docs.add(makeResume("Lisa", "United Kingdom"));
    w.addDocuments(docs);

    docs.clear();
    docs.add(makeJob("ruby", 2005));
    docs.add(makeJob("java", 2006));
    docs.add(makeResume("Frank", "United States"));
    w.addDocuments(docs);
    w.commit();

    IndexReader r = DirectoryReader.open(w);
    w.close();
    IndexSearcher s = newSearcher(r);
    BitSetProducer parentsFilter = new QueryBitSetProducer(new TermQuery(new Term("docType", "resume")));
    CheckJoinIndex.check(r, parentsFilter);

    BooleanQuery.Builder childQuery = new BooleanQuery.Builder();
    childQuery.add(new BooleanClause(new TermQuery(new Term("skill", "java")), Occur.MUST));
    childQuery.add(new BooleanClause(IntPoint.newRangeQuery("year", 2006, 2011), Occur.MUST));

    ToParentBlockJoinQuery childJoinQuery = new ToParentBlockJoinQuery(childQuery.build(), parentsFilter, ScoreMode.Avg);

    BooleanQuery.Builder fullQuery = new BooleanQuery.Builder();
    fullQuery.add(new BooleanClause(childJoinQuery, Occur.MUST));
    fullQuery.add(new BooleanClause(new MatchAllDocsQuery(), Occur.MUST));
    TopDocs topDocs = s.search(fullQuery.build(), 2);
    assertEquals(2, topDocs.totalHits);
    assertEquals(asSet("Lisa", "Frank"),
        asSet(s.doc(topDocs.scoreDocs[0].doc).get("name"), s.doc(topDocs.scoreDocs[1].doc).get("name")));

    ParentChildrenBlockJoinQuery childrenQuery =
        new ParentChildrenBlockJoinQuery(parentsFilter, childQuery.build(), topDocs.scoreDocs[0].doc);
    TopDocs matchingChildren = s.search(childrenQuery, 1);
    assertEquals(1, matchingChildren.totalHits);
    assertEquals("java", s.doc(matchingChildren.scoreDocs[0].doc).get("skill"));

    childrenQuery = new ParentChildrenBlockJoinQuery(parentsFilter, childQuery.build(), topDocs.scoreDocs[1].doc);
    matchingChildren = s.search(childrenQuery, 1);
    assertEquals(1, matchingChildren.totalHits);
    assertEquals("java", s.doc(matchingChildren.scoreDocs[0].doc).get("skill"));

    r.close();
    dir.close();
  }

  // You must use ToParentBlockJoinSearcher if you want to do BQ SHOULD queries:
  public void testBQShouldJoinedChild() throws Exception {
    final Directory dir = newDirectory();
    final RandomIndexWriter w = new RandomIndexWriter(random(), dir);

    final List<Document> docs = new ArrayList<>();

    docs.add(makeJob("java", 2007));
    docs.add(makeJob("python", 2010));
    docs.add(makeResume("Lisa", "United Kingdom"));
    w.addDocuments(docs);

    docs.clear();
    docs.add(makeJob("ruby", 2005));
    docs.add(makeJob("java", 2006));
    docs.add(makeResume("Frank", "United States"));
    w.addDocuments(docs);

    IndexReader r = w.getReader();
    w.close();
    IndexSearcher s = newSearcher(r, false);
    //IndexSearcher s = new IndexSearcher(r);

    // Create a filter that defines "parent" documents in the index - in this case resumes
    BitSetProducer parentsFilter = new QueryBitSetProducer(new TermQuery(new Term("docType", "resume")));
    CheckJoinIndex.check(r, parentsFilter);

    // Define child document criteria (finds an example of relevant work experience)
    BooleanQuery.Builder childQuery = new BooleanQuery.Builder();
    childQuery.add(new BooleanClause(new TermQuery(new Term("skill", "java")), Occur.MUST));
    childQuery.add(new BooleanClause(IntPoint.newRangeQuery("year", 2006, 2011), Occur.MUST));

    // Define parent document criteria (find a resident in the UK)
    Query parentQuery = new TermQuery(new Term("country", "United Kingdom"));

    // Wrap the child document query to 'join' any matches
    // up to corresponding parent:
    ToParentBlockJoinQuery childJoinQuery = new ToParentBlockJoinQuery(childQuery.build(), parentsFilter, ScoreMode.Avg);

    // Combine the parent and nested child queries into a single query for a candidate
    BooleanQuery.Builder fullQuery = new BooleanQuery.Builder();
    fullQuery.add(new BooleanClause(parentQuery, Occur.SHOULD));
    fullQuery.add(new BooleanClause(childJoinQuery, Occur.SHOULD));

    final TopDocs topDocs = s.search(fullQuery.build(), 2);
    assertEquals(2, topDocs.totalHits);
    assertEquals(asSet("Lisa", "Frank"),
        asSet(s.doc(topDocs.scoreDocs[0].doc).get("name"), s.doc(topDocs.scoreDocs[1].doc).get("name")));

    ParentChildrenBlockJoinQuery childrenQuery =
        new ParentChildrenBlockJoinQuery(parentsFilter, childQuery.build(), topDocs.scoreDocs[0].doc);
    TopDocs matchingChildren = s.search(childrenQuery, 1);
    assertEquals(1, matchingChildren.totalHits);
    assertEquals("java", s.doc(matchingChildren.scoreDocs[0].doc).get("skill"));

    childrenQuery = new ParentChildrenBlockJoinQuery(parentsFilter, childQuery.build(), topDocs.scoreDocs[1].doc);
    matchingChildren = s.search(childrenQuery, 1);
    assertEquals(1, matchingChildren.totalHits);
    assertEquals("java", s.doc(matchingChildren.scoreDocs[0].doc).get("skill"));
    
    r.close();
    dir.close();
  }
  
  public void testSimple() throws Exception {

    final Directory dir = newDirectory();
    final RandomIndexWriter w = new RandomIndexWriter(random(), dir);

    final List<Document> docs = new ArrayList<>();

    docs.add(makeJob("java", 2007));
    docs.add(makeJob("python", 2010));
    docs.add(makeResume("Lisa", "United Kingdom"));
    w.addDocuments(docs);

    docs.clear();
    docs.add(makeJob("ruby", 2005));
    docs.add(makeJob("java", 2006));
    docs.add(makeResume("Frank", "United States"));
    w.addDocuments(docs);

    IndexReader r = w.getReader();
    w.close();
    IndexSearcher s = newSearcher(r, false);

    // Create a filter that defines "parent" documents in the index - in this case resumes
    BitSetProducer parentsFilter = new QueryBitSetProducer(new TermQuery(new Term("docType", "resume")));
    CheckJoinIndex.check(r, parentsFilter);

    // Define child document criteria (finds an example of relevant work experience)
    BooleanQuery.Builder childQuery = new BooleanQuery.Builder();
    childQuery.add(new BooleanClause(new TermQuery(new Term("skill", "java")), Occur.MUST));
    childQuery.add(new BooleanClause(IntPoint.newRangeQuery("year", 2006, 2011), Occur.MUST));

    // Define parent document criteria (find a resident in the UK)
    Query parentQuery = new TermQuery(new Term("country", "United Kingdom"));

    // Wrap the child document query to 'join' any matches
    // up to corresponding parent:
    ToParentBlockJoinQuery childJoinQuery = new ToParentBlockJoinQuery(childQuery.build(), parentsFilter, ScoreMode.Avg);

    // Combine the parent and nested child queries into a single query for a candidate
    BooleanQuery.Builder fullQuery = new BooleanQuery.Builder();
    fullQuery.add(new BooleanClause(parentQuery, Occur.MUST));
    fullQuery.add(new BooleanClause(childJoinQuery, Occur.MUST));

    CheckHits.checkHitCollector(random(), fullQuery.build(), "country", s, new int[] {2});

    TopDocs topDocs = s.search(fullQuery.build(), 1);

    //assertEquals(1, results.totalHitCount);
    assertEquals(1, topDocs.totalHits);
    Document parentDoc = s.doc(topDocs.scoreDocs[0].doc);
    assertEquals("Lisa", parentDoc.get("name"));

    ParentChildrenBlockJoinQuery childrenQuery =
        new ParentChildrenBlockJoinQuery(parentsFilter, childQuery.build(), topDocs.scoreDocs[0].doc);
    TopDocs matchingChildren = s.search(childrenQuery, 1);
    assertEquals(1, matchingChildren.totalHits);
    assertEquals("java", s.doc(matchingChildren.scoreDocs[0].doc).get("skill"));


    //System.out.println("TEST: now test up");

    // Now join "up" (map parent hits to child docs) instead...:
    ToChildBlockJoinQuery parentJoinQuery = new ToChildBlockJoinQuery(parentQuery, parentsFilter);
    BooleanQuery.Builder fullChildQuery = new BooleanQuery.Builder();
    fullChildQuery.add(new BooleanClause(parentJoinQuery, Occur.MUST));
    fullChildQuery.add(new BooleanClause(childQuery.build(), Occur.MUST));

    //System.out.println("FULL: " + fullChildQuery);
    TopDocs hits = s.search(fullChildQuery.build(), 10);
    assertEquals(1, hits.totalHits);
    Document childDoc = s.doc(hits.scoreDocs[0].doc);
    //System.out.println("CHILD = " + childDoc + " docID=" + hits.scoreDocs[0].doc);
    assertEquals("java", childDoc.get("skill"));
    assertEquals(2007, childDoc.getField("year").numericValue());
    assertEquals("Lisa", getParentDoc(r, parentsFilter, hits.scoreDocs[0].doc).get("name"));

    // Test with filter on child docs:
    fullChildQuery.add(new TermQuery(new Term("skill", "foosball")), Occur.FILTER);
    assertEquals(0, s.search(fullChildQuery.build(), 1).totalHits);

    r.close();
    dir.close();
  }

  protected Query skill(String skill) {
    return new TermQuery(new Term("skill", skill));
  }

  public void testSimpleFilter() throws Exception {

    final Directory dir = newDirectory();
    final RandomIndexWriter w = new RandomIndexWriter(random(), dir);

    final List<Document> docs = new ArrayList<>();
    docs.add(makeJob("java", 2007));
    docs.add(makeJob("python", 2010));
    Collections.shuffle(docs, random());
    docs.add(makeResume("Lisa", "United Kingdom"));

    final List<Document> docs2 = new ArrayList<>();
    docs2.add(makeJob("ruby", 2005));
    docs2.add(makeJob("java", 2006));
    Collections.shuffle(docs2, random());
    docs2.add(makeResume("Frank", "United States"));

    addSkillless(w);
    boolean turn = random().nextBoolean();
    w.addDocuments(turn ? docs:docs2);

    addSkillless(w);

    w.addDocuments(!turn ? docs:docs2);

    addSkillless(w);

    IndexReader r = w.getReader();
    w.close();
    IndexSearcher s = newSearcher(r, false);

    // Create a filter that defines "parent" documents in the index - in this case resumes
    BitSetProducer parentsFilter = new QueryBitSetProducer(new TermQuery(new Term("docType", "resume")));
    CheckJoinIndex.check(r, parentsFilter);

    // Define child document criteria (finds an example of relevant work experience)
    BooleanQuery.Builder childQuery = new BooleanQuery.Builder();
    childQuery.add(new BooleanClause(new TermQuery(new Term("skill", "java")), Occur.MUST));
    childQuery.add(new BooleanClause(IntPoint.newRangeQuery("year", 2006, 2011), Occur.MUST));

    // Define parent document criteria (find a resident in the UK)
    Query parentQuery = new TermQuery(new Term("country", "United Kingdom"));

    // Wrap the child document query to 'join' any matches
    // up to corresponding parent:
    ToParentBlockJoinQuery childJoinQuery = new ToParentBlockJoinQuery(childQuery.build(), parentsFilter, ScoreMode.Avg);

    assertEquals("no filter - both passed", 2, s.search(childJoinQuery, 10).totalHits);

    Query query = new BooleanQuery.Builder()
        .add(childJoinQuery, Occur.MUST)
        .add(new TermQuery(new Term("docType", "resume")), Occur.FILTER)
        .build();
    assertEquals("dummy filter passes everyone ", 2, s.search(query, 10).totalHits);
    query = new BooleanQuery.Builder()
        .add(childJoinQuery, Occur.MUST)
        .add(new TermQuery(new Term("docType", "resume")), Occur.FILTER)
        .build();
    assertEquals("dummy filter passes everyone ", 2, s.search(query, 10).totalHits);

    // not found test
    query = new BooleanQuery.Builder()
        .add(childJoinQuery, Occur.MUST)
        .add(new TermQuery(new Term("country", "Oz")), Occur.FILTER)
        .build();
    assertEquals("noone live there", 0, s.search(query, 1).totalHits);

    // apply the UK filter by the searcher
    query = new BooleanQuery.Builder()
        .add(childJoinQuery, Occur.MUST)
        .add(parentQuery, Occur.FILTER)
        .build();
    TopDocs ukOnly = s.search(query, 1);
    assertEquals("has filter - single passed", 1, ukOnly.totalHits);
    assertEquals( "Lisa", r.document(ukOnly.scoreDocs[0].doc).get("name"));

    query = new BooleanQuery.Builder()
        .add(childJoinQuery, Occur.MUST)
        .add(new TermQuery(new Term("country", "United States")), Occur.FILTER)
        .build();
    // looking for US candidates
    TopDocs usThen = s.search(query, 1);
    assertEquals("has filter - single passed", 1, usThen.totalHits);
    assertEquals("Frank", r.document(usThen.scoreDocs[0].doc).get("name"));


    TermQuery us = new TermQuery(new Term("country", "United States"));
    assertEquals("@ US we have java and ruby", 2,
        s.search(new ToChildBlockJoinQuery(us,
                          parentsFilter), 10).totalHits );

    query = new BooleanQuery.Builder()
        .add(new ToChildBlockJoinQuery(us, parentsFilter), Occur.MUST)
        .add(skill("java"), Occur.FILTER)
        .build();
    assertEquals("java skills in US", 1, s.search(query, 10).totalHits );

    BooleanQuery.Builder rubyPython = new BooleanQuery.Builder();
    rubyPython.add(new TermQuery(new Term("skill", "ruby")), Occur.SHOULD);
    rubyPython.add(new TermQuery(new Term("skill", "python")), Occur.SHOULD);
    query = new BooleanQuery.Builder()
        .add(new ToChildBlockJoinQuery(us, parentsFilter), Occur.MUST)
        .add(rubyPython.build(), Occur.FILTER)
        .build();
    assertEquals("ruby skills in US", 1, s.search(query, 10).totalHits );

    r.close();
    dir.close();
  }

  private void addSkillless(final RandomIndexWriter w) throws IOException {
    if (random().nextBoolean()) {
      w.addDocument(makeResume("Skillless", random().nextBoolean() ? "United Kingdom":"United States"));
    }
  }

  private Document getParentDoc(IndexReader reader, BitSetProducer parents, int childDocID) throws IOException {
    final List<LeafReaderContext> leaves = reader.leaves();
    final int subIndex = ReaderUtil.subIndex(childDocID, leaves);
    final LeafReaderContext leaf = leaves.get(subIndex);
    final BitSet bits = parents.getBitSet(leaf);
    return leaf.reader().document(bits.nextSetBit(childDocID - leaf.docBase));
  }

  public void testBoostBug() throws Exception {
    final Directory dir = newDirectory();
    final RandomIndexWriter w = new RandomIndexWriter(random(), dir);
    IndexReader r = w.getReader();
    w.close();
    IndexSearcher s = newSearcher(r);

    ToParentBlockJoinQuery q = new ToParentBlockJoinQuery(new MatchNoDocsQuery(), new QueryBitSetProducer(new MatchAllDocsQuery()), ScoreMode.Avg);
    QueryUtils.check(random(), q, s);
    s.search(q, 10);
    BooleanQuery.Builder bqB = new BooleanQuery.Builder();
    bqB.add(q, BooleanClause.Occur.MUST);
    BooleanQuery bq = bqB.build();
    s.search(new BoostQuery(bq, 2f), 10);
    r.close();
    dir.close();
  }

  private String[][] getRandomFields(int maxUniqueValues) {

    final String[][] fields = new String[TestUtil.nextInt(random(), 2, 4)][];
    for(int fieldID=0;fieldID<fields.length;fieldID++) {
      final int valueCount;
      if (fieldID == 0) {
        valueCount = 2;
      } else {
        valueCount = TestUtil.nextInt(random(), 1, maxUniqueValues);
      }

      final String[] values = fields[fieldID] = new String[valueCount];
      for(int i=0;i<valueCount;i++) {
        values[i] = TestUtil.randomRealisticUnicodeString(random());
        //values[i] = TestUtil.randomSimpleString(random());
      }
    }

    return fields;
  }

  private Term randomParentTerm(String[] values) {
    return new Term("parent0", values[random().nextInt(values.length)]);
  }

  private Term randomChildTerm(String[] values) {
    return new Term("child0", values[random().nextInt(values.length)]);
  }

  private Sort getRandomSort(String prefix, int numFields) {
    final List<SortField> sortFields = new ArrayList<>();
    // TODO: sometimes sort by score; problem is scores are
    // not comparable across the two indices
    // sortFields.add(SortField.FIELD_SCORE);
    if (random().nextBoolean()) {
      sortFields.add(new SortField(prefix + random().nextInt(numFields), SortField.Type.STRING, random().nextBoolean()));
    } else if (random().nextBoolean()) {
      sortFields.add(new SortField(prefix + random().nextInt(numFields), SortField.Type.STRING, random().nextBoolean()));
      sortFields.add(new SortField(prefix + random().nextInt(numFields), SortField.Type.STRING, random().nextBoolean()));
    }
    // Break ties:
    sortFields.add(new SortField(prefix + "ID", SortField.Type.INT));
    return new Sort(sortFields.toArray(new SortField[sortFields.size()]));
  }

  public void testRandom() throws Exception {
    // We build two indices at once: one normalized (which
    // ToParentBlockJoinQuery/Collector,
    // ToChildBlockJoinQuery can query) and the other w/
    // the same docs, just fully denormalized:
    final Directory dir = newDirectory();
    final Directory joinDir = newDirectory();

    final int maxNumChildrenPerParent = 20;
    final int numParentDocs = TestUtil.nextInt(random(), 100 * RANDOM_MULTIPLIER, 300 * RANDOM_MULTIPLIER);
    //final int numParentDocs = 30;

    // Values for parent fields:
    final String[][] parentFields = getRandomFields(numParentDocs/2);
    // Values for child fields:
    final String[][] childFields = getRandomFields(numParentDocs);

    final boolean doDeletes = random().nextBoolean();
    final List<Integer> toDelete = new ArrayList<>();

    // TODO: parallel star join, nested join cases too!
    final RandomIndexWriter w = new RandomIndexWriter(random(), dir);
    final RandomIndexWriter joinW = new RandomIndexWriter(random(), joinDir);
    for(int parentDocID=0;parentDocID<numParentDocs;parentDocID++) {
      Document parentDoc = new Document();
      Document parentJoinDoc = new Document();
      Field id = new StoredField("parentID", parentDocID);
      parentDoc.add(id);
      parentJoinDoc.add(id);
      parentJoinDoc.add(newStringField("isParent", "x", Field.Store.NO));
      id = new NumericDocValuesField("parentID", parentDocID);
      parentDoc.add(id);
      parentJoinDoc.add(id);
      parentJoinDoc.add(newStringField("isParent", "x", Field.Store.NO));
      for(int field=0;field<parentFields.length;field++) {
        if (random().nextDouble() < 0.9) {
          String s = parentFields[field][random().nextInt(parentFields[field].length)];
          Field f = newStringField("parent" + field, s, Field.Store.NO);
          parentDoc.add(f);
          parentJoinDoc.add(f);

          f = new SortedDocValuesField("parent" + field, new BytesRef(s));
          parentDoc.add(f);
          parentJoinDoc.add(f);
        }
      }

      if (doDeletes) {
        parentDoc.add(new IntPoint("blockID", parentDocID));
        parentJoinDoc.add(new IntPoint("blockID", parentDocID));
      }

      final List<Document> joinDocs = new ArrayList<>();

      if (VERBOSE) {
        StringBuilder sb = new StringBuilder();
        sb.append("parentID=").append(parentDoc.get("parentID"));
        for(int fieldID=0;fieldID<parentFields.length;fieldID++) {
          String s = parentDoc.get("parent" + fieldID);
          if (s != null) {
            sb.append(" parent" + fieldID + "=" + s);
          }
        }
        System.out.println("  " + sb.toString());
      }

      final int numChildDocs = TestUtil.nextInt(random(), 1, maxNumChildrenPerParent);
      for(int childDocID=0;childDocID<numChildDocs;childDocID++) {
        // Denormalize: copy all parent fields into child doc:
        Document childDoc = TestUtil.cloneDocument(parentDoc);
        Document joinChildDoc = new Document();
        joinDocs.add(joinChildDoc);

        Field childID = new StoredField("childID", childDocID);
        childDoc.add(childID);
        joinChildDoc.add(childID);
        childID = new NumericDocValuesField("childID", childDocID);
        childDoc.add(childID);
        joinChildDoc.add(childID);

        for(int childFieldID=0;childFieldID<childFields.length;childFieldID++) {
          if (random().nextDouble() < 0.9) {
            String s = childFields[childFieldID][random().nextInt(childFields[childFieldID].length)];
            Field f = newStringField("child" + childFieldID, s, Field.Store.NO);
            childDoc.add(f);
            joinChildDoc.add(f);

            f = new SortedDocValuesField("child" + childFieldID, new BytesRef(s));
            childDoc.add(f);
            joinChildDoc.add(f);
          }
        }

        if (VERBOSE) {
          StringBuilder sb = new StringBuilder();
          sb.append("childID=").append(joinChildDoc.get("childID"));
          for(int fieldID=0;fieldID<childFields.length;fieldID++) {
            String s = joinChildDoc.get("child" + fieldID);
            if (s != null) {
              sb.append(" child" + fieldID + "=" + s);
            }
          }
          System.out.println("    " + sb.toString());
        }

        if (doDeletes) {
          joinChildDoc.add(new IntPoint("blockID", parentDocID));
        }

        w.addDocument(childDoc);
      }

      // Parent last:
      joinDocs.add(parentJoinDoc);
      joinW.addDocuments(joinDocs);

      if (doDeletes && random().nextInt(30) == 7) {
        toDelete.add(parentDocID);
      }
    }

    if (!toDelete.isEmpty()) {
      Query query = IntPoint.newSetQuery("blockID", toDelete);
      w.deleteDocuments(query);
      joinW.deleteDocuments(query);
    }

    final IndexReader r = w.getReader();
    w.close();
    final IndexReader joinR = joinW.getReader();
    joinW.close();

    if (VERBOSE) {
      System.out.println("TEST: reader=" + r);
      System.out.println("TEST: joinReader=" + joinR);

      Bits liveDocs = MultiFields.getLiveDocs(joinR);
      for(int docIDX=0;docIDX<joinR.maxDoc();docIDX++) {
        System.out.println("  docID=" + docIDX + " doc=" + joinR.document(docIDX) + " deleted?=" + (liveDocs != null && liveDocs.get(docIDX) == false));
      }
      PostingsEnum parents = MultiFields.getTermDocsEnum(joinR, "isParent", new BytesRef("x"));
      System.out.println("parent docIDs:");
      while (parents.nextDoc() != PostingsEnum.NO_MORE_DOCS) {
        System.out.println("  " + parents.docID());
      }
    }

    final IndexSearcher s = newSearcher(r, false);

    final IndexSearcher joinS = new IndexSearcher(joinR);

    final BitSetProducer parentsFilter = new QueryBitSetProducer(new TermQuery(new Term("isParent", "x")));
    CheckJoinIndex.check(joinS.getIndexReader(), parentsFilter);

    final int iters = 200*RANDOM_MULTIPLIER;

    for(int iter=0;iter<iters;iter++) {
      if (VERBOSE) {
        System.out.println("TEST: iter=" + (1+iter) + " of " + iters);
      }

      Query childQuery;
      if (random().nextInt(3) == 2) {
        final int childFieldID = random().nextInt(childFields.length);
        childQuery = new TermQuery(new Term("child" + childFieldID,
                                            childFields[childFieldID][random().nextInt(childFields[childFieldID].length)]));
      } else if (random().nextInt(3) == 2) {
        BooleanQuery.Builder bq = new BooleanQuery.Builder();
        final int numClauses = TestUtil.nextInt(random(), 2, 4);
        boolean didMust = false;
        for(int clauseIDX=0;clauseIDX<numClauses;clauseIDX++) {
          Query clause;
          BooleanClause.Occur occur;
          if (!didMust && random().nextBoolean()) {
            occur = random().nextBoolean() ? BooleanClause.Occur.MUST : BooleanClause.Occur.MUST_NOT;
            clause = new TermQuery(randomChildTerm(childFields[0]));
            didMust = true;
          } else {
            occur = BooleanClause.Occur.SHOULD;
            final int childFieldID = TestUtil.nextInt(random(), 1, childFields.length - 1);
            clause = new TermQuery(new Term("child" + childFieldID,
                                            childFields[childFieldID][random().nextInt(childFields[childFieldID].length)]));
          }
          bq.add(clause, occur);
        }
        childQuery = bq.build();
      } else {
        BooleanQuery.Builder bq = new BooleanQuery.Builder();

        bq.add(new TermQuery(randomChildTerm(childFields[0])),
               BooleanClause.Occur.MUST);
        final int childFieldID = TestUtil.nextInt(random(), 1, childFields.length - 1);
        bq.add(new TermQuery(new Term("child" + childFieldID, childFields[childFieldID][random().nextInt(childFields[childFieldID].length)])),
               random().nextBoolean() ? BooleanClause.Occur.MUST : BooleanClause.Occur.MUST_NOT);
        childQuery = bq.build();
      }
      if (random().nextBoolean()) {
        childQuery = new RandomApproximationQuery(childQuery, random());
      }


      final ScoreMode agg = ScoreMode.values()[random().nextInt(ScoreMode.values().length)];
      final ToParentBlockJoinQuery childJoinQuery = new ToParentBlockJoinQuery(childQuery, parentsFilter, agg);

      // To run against the block-join index:
      final Query parentJoinQuery;

      // Same query as parentJoinQuery, but to run against
      // the fully denormalized index (so we can compare
      // results):
      final Query parentQuery;

      if (random().nextBoolean()) {
        parentQuery = childQuery;
        parentJoinQuery = childJoinQuery;
      } else {
        // AND parent field w/ child field
        final BooleanQuery.Builder bq = new BooleanQuery.Builder();
        final Term parentTerm = randomParentTerm(parentFields[0]);
        if (random().nextBoolean()) {
          bq.add(childJoinQuery, BooleanClause.Occur.MUST);
          bq.add(new TermQuery(parentTerm),
                 BooleanClause.Occur.MUST);
        } else {
          bq.add(new TermQuery(parentTerm),
                 BooleanClause.Occur.MUST);
          bq.add(childJoinQuery, BooleanClause.Occur.MUST);
        }

        final BooleanQuery.Builder bq2 = new BooleanQuery.Builder();
        if (random().nextBoolean()) {
          bq2.add(childQuery, BooleanClause.Occur.MUST);
          bq2.add(new TermQuery(parentTerm),
                  BooleanClause.Occur.MUST);
        } else {
          bq2.add(new TermQuery(parentTerm),
                  BooleanClause.Occur.MUST);
          bq2.add(childQuery, BooleanClause.Occur.MUST);
        }
        parentJoinQuery = bq.build();
        parentQuery = bq2.build();
      }

      final Sort parentSort = getRandomSort("parent", parentFields.length);
      final Sort childSort = getRandomSort("child", childFields.length);

      if (VERBOSE) {
        System.out.println("\nTEST: query=" + parentQuery + " joinQuery=" + parentJoinQuery + " parentSort=" + parentSort + " childSort=" + childSort);
      }

      // Merge both sorts:
      final List<SortField> sortFields = new ArrayList<>(Arrays.asList(parentSort.getSort()));
      sortFields.addAll(Arrays.asList(childSort.getSort()));
      final Sort parentAndChildSort = new Sort(sortFields.toArray(new SortField[sortFields.size()]));

      final TopDocs results = s.search(parentQuery, r.numDocs(),
                                       parentAndChildSort);

      if (VERBOSE) {
        System.out.println("\nTEST: normal index gets " + results.totalHits + " hits; sort=" + parentAndChildSort);
        final ScoreDoc[] hits = results.scoreDocs;
        for(int hitIDX=0;hitIDX<hits.length;hitIDX++) {
          final Document doc = s.doc(hits[hitIDX].doc);
          //System.out.println("  score=" + hits[hitIDX].score + " parentID=" + doc.get("parentID") + " childID=" + doc.get("childID") + " (docID=" + hits[hitIDX].doc + ")");
          System.out.println("  parentID=" + doc.get("parentID") + " childID=" + doc.get("childID") + " (docID=" + hits[hitIDX].doc + ")");
          FieldDoc fd = (FieldDoc) hits[hitIDX];
          if (fd.fields != null) {
            System.out.print("    " + fd.fields.length + " sort values: ");
            for(Object o : fd.fields) {
              if (o instanceof BytesRef) {
                System.out.print(((BytesRef) o).utf8ToString() + " ");
              } else {
                System.out.print(o + " ");
              }
            }
            System.out.println();
          }
        }
      }

      TopDocs joinedResults = joinS.search(parentJoinQuery, numParentDocs);
      SortedMap<Integer, TopDocs> joinResults = new TreeMap<>();
      for (ScoreDoc parentHit : joinedResults.scoreDocs) {
        ParentChildrenBlockJoinQuery childrenQuery =
            new ParentChildrenBlockJoinQuery(parentsFilter, childQuery, parentHit.doc);
        TopDocs childTopDocs = joinS.search(childrenQuery, maxNumChildrenPerParent, childSort);
        final Document parentDoc = joinS.doc(parentHit.doc);
        joinResults.put(Integer.valueOf(parentDoc.get("parentID")), childTopDocs);
      }

      final int hitsPerGroup = TestUtil.nextInt(random(), 1, 20);
      //final int hitsPerGroup = 100;

      if (VERBOSE) {
        System.out.println("\nTEST: block join index gets " + (joinResults == null ? 0 : joinResults.size()) + " groups; hitsPerGroup=" + hitsPerGroup);
        if (joinResults != null) {
          for (Map.Entry<Integer, TopDocs> entry : joinResults.entrySet()) {
            System.out.println("  group parentID=" + entry.getKey() + " (docID=" + entry.getKey() + ")");
            for(ScoreDoc childHit : entry.getValue().scoreDocs) {
              final Document doc = joinS.doc(childHit.doc);
//              System.out.println("    score=" + childHit.score + " childID=" + doc.get("childID") + " (docID=" + childHit.doc + ")");
              System.out.println("    childID=" + doc.get("childID") + " child0=" + doc.get("child0") + " (docID=" + childHit.doc + ")");
            }
          }
        }
      }

      if (results.totalHits == 0) {
        assertEquals(0, joinResults.size());
      } else {
        compareHits(r, joinR, results, joinResults);
        TopDocs b = joinS.search(childJoinQuery, 10);
        for (ScoreDoc hit : b.scoreDocs) {
          Explanation explanation = joinS.explain(childJoinQuery, hit.doc);
          Document document = joinS.doc(hit.doc - 1);
          int childId = Integer.parseInt(document.get("childID"));
          //System.out.println("  hit docID=" + hit.doc + " childId=" + childId + " parentId=" + document.get("parentID"));
          assertTrue(explanation.isMatch());
          assertEquals(hit.score, explanation.getValue(), 0.0f);
          Matcher m = Pattern.compile("Score based on ([0-9]+) child docs in range from ([0-9]+) to ([0-9]+), best match:").matcher(explanation.getDescription());
          assertTrue("Block Join description not matches", m.matches());
          assertTrue("Matched children not positive", Integer.parseInt(m.group(1)) > 0);
          assertEquals("Wrong child range start", hit.doc - 1 - childId, Integer.parseInt(m.group(2)));
          assertEquals("Wrong child range end", hit.doc - 1, Integer.parseInt(m.group(3)));
          Explanation childWeightExplanation = explanation.getDetails()[0];
          if ("sum of:".equals(childWeightExplanation.getDescription())) {
            childWeightExplanation = childWeightExplanation.getDetails()[0];
          }
          assertTrue("Wrong child weight description", childWeightExplanation.getDescription().startsWith("weight(child"));
        }
      }

      // Test joining in the opposite direction (parent to
      // child):

      // Get random query against parent documents:
      final Query parentQuery2;
      if (random().nextInt(3) == 2) {
        final int fieldID = random().nextInt(parentFields.length);
        parentQuery2 = new TermQuery(new Term("parent" + fieldID,
                                              parentFields[fieldID][random().nextInt(parentFields[fieldID].length)]));
      } else if (random().nextInt(3) == 2) {
        BooleanQuery.Builder bq = new BooleanQuery.Builder();
        final int numClauses = TestUtil.nextInt(random(), 2, 4);
        boolean didMust = false;
        for(int clauseIDX=0;clauseIDX<numClauses;clauseIDX++) {
          Query clause;
          BooleanClause.Occur occur;
          if (!didMust && random().nextBoolean()) {
            occur = random().nextBoolean() ? BooleanClause.Occur.MUST : BooleanClause.Occur.MUST_NOT;
            clause = new TermQuery(randomParentTerm(parentFields[0]));
            didMust = true;
          } else {
            occur = BooleanClause.Occur.SHOULD;
            final int fieldID = TestUtil.nextInt(random(), 1, parentFields.length - 1);
            clause = new TermQuery(new Term("parent" + fieldID,
                                            parentFields[fieldID][random().nextInt(parentFields[fieldID].length)]));
          }
          bq.add(clause, occur);
        }
        parentQuery2 = bq.build();
      } else {
        BooleanQuery.Builder bq = new BooleanQuery.Builder();

        bq.add(new TermQuery(randomParentTerm(parentFields[0])),
               BooleanClause.Occur.MUST);
        final int fieldID = TestUtil.nextInt(random(), 1, parentFields.length - 1);
        bq.add(new TermQuery(new Term("parent" + fieldID, parentFields[fieldID][random().nextInt(parentFields[fieldID].length)])),
               random().nextBoolean() ? BooleanClause.Occur.MUST : BooleanClause.Occur.MUST_NOT);
        parentQuery2 = bq.build();
      }

      if (VERBOSE) {
        System.out.println("\nTEST: top down: parentQuery2=" + parentQuery2);
      }

      // Maps parent query to child docs:
      final ToChildBlockJoinQuery parentJoinQuery2 = new ToChildBlockJoinQuery(parentQuery2, parentsFilter);

      // To run against the block-join index:
      Query childJoinQuery2;

      // Same query as parentJoinQuery, but to run against
      // the fully denormalized index (so we can compare
      // results):
      Query childQuery2;

      if (random().nextBoolean()) {
        childQuery2 = parentQuery2;
        childJoinQuery2 = parentJoinQuery2;
      } else {
        final Term childTerm = randomChildTerm(childFields[0]);
        if (random().nextBoolean()) { // filtered case
          childJoinQuery2 = parentJoinQuery2;
          childJoinQuery2 = new BooleanQuery.Builder()
              .add(childJoinQuery2, Occur.MUST)
              .add(new TermQuery(childTerm), Occur.FILTER)
              .build();
        } else {
          // AND child field w/ parent query:
          final BooleanQuery.Builder bq = new BooleanQuery.Builder();
          if (random().nextBoolean()) {
            bq.add(parentJoinQuery2, BooleanClause.Occur.MUST);
            bq.add(new TermQuery(childTerm),
                   BooleanClause.Occur.MUST);
          } else {
            bq.add(new TermQuery(childTerm),
                   BooleanClause.Occur.MUST);
            bq.add(parentJoinQuery2, BooleanClause.Occur.MUST);
          }
          childJoinQuery2 = bq.build();
        }

        if (random().nextBoolean()) { // filtered case
          childQuery2 = parentQuery2;
          childQuery2 = new BooleanQuery.Builder()
              .add(childQuery2, Occur.MUST)
              .add(new TermQuery(childTerm), Occur.FILTER)
              .build();
        } else {
          final BooleanQuery.Builder bq2 = new BooleanQuery.Builder();
          if (random().nextBoolean()) {
            bq2.add(parentQuery2, BooleanClause.Occur.MUST);
            bq2.add(new TermQuery(childTerm),
                    BooleanClause.Occur.MUST);
          } else {
            bq2.add(new TermQuery(childTerm),
                    BooleanClause.Occur.MUST);
            bq2.add(parentQuery2, BooleanClause.Occur.MUST);
          }
          childQuery2 = bq2.build();
        }
      }

      final Sort childSort2 = getRandomSort("child", childFields.length);

      // Search denormalized index:
      if (VERBOSE) {
        System.out.println("TEST: run top down query=" + childQuery2 + " sort=" + childSort2);
      }
      final TopDocs results2 = s.search(childQuery2, r.numDocs(),
                                        childSort2);
      if (VERBOSE) {
        System.out.println("  " + results2.totalHits + " totalHits:");
        for(ScoreDoc sd : results2.scoreDocs) {
          final Document doc = s.doc(sd.doc);
          System.out.println("  childID=" + doc.get("childID") + " parentID=" + doc.get("parentID") + " docID=" + sd.doc);
        }
      }

      // Search join index:
      if (VERBOSE) {
        System.out.println("TEST: run top down join query=" + childJoinQuery2 + " sort=" + childSort2);
      }
      TopDocs joinResults2 = joinS.search(childJoinQuery2, joinR.numDocs(), childSort2);
      if (VERBOSE) {
        System.out.println("  " + joinResults2.totalHits + " totalHits:");
        for(ScoreDoc sd : joinResults2.scoreDocs) {
          final Document doc = joinS.doc(sd.doc);
          final Document parentDoc = getParentDoc(joinR, parentsFilter, sd.doc);
          System.out.println("  childID=" + doc.get("childID") + " parentID=" + parentDoc.get("parentID") + " docID=" + sd.doc);
        }
      }

      compareChildHits(r, joinR, results2, joinResults2);
    }

    r.close();
    joinR.close();
    dir.close();
    joinDir.close();
  }

  private void compareChildHits(IndexReader r, IndexReader joinR, TopDocs results, TopDocs joinResults) throws Exception {
    assertEquals(results.totalHits, joinResults.totalHits);
    assertEquals(results.scoreDocs.length, joinResults.scoreDocs.length);
    for(int hitCount=0;hitCount<results.scoreDocs.length;hitCount++) {
      ScoreDoc hit = results.scoreDocs[hitCount];
      ScoreDoc joinHit = joinResults.scoreDocs[hitCount];
      Document doc1 = r.document(hit.doc);
      Document doc2 = joinR.document(joinHit.doc);
      assertEquals("hit " + hitCount + " differs",
                   doc1.get("childID"), doc2.get("childID"));
      // don't compare scores -- they are expected to differ


      assertTrue(hit instanceof FieldDoc);
      assertTrue(joinHit instanceof FieldDoc);

      FieldDoc hit0 = (FieldDoc) hit;
      FieldDoc joinHit0 = (FieldDoc) joinHit;
      assertArrayEquals(hit0.fields, joinHit0.fields);
    }
  }

  private void compareHits(IndexReader r, IndexReader joinR, TopDocs controlHits, Map<Integer, TopDocs> joinResults) throws Exception {
    int currentParentID = -1;
    int childHitSlot = 0;
    TopDocs childHits = new TopDocs(0, new ScoreDoc[0], 0f);
    for (ScoreDoc controlHit : controlHits.scoreDocs) {
      Document controlDoc = r.document(controlHit.doc);
      int parentID = Integer.parseInt(controlDoc.get("parentID"));
      if (parentID != currentParentID) {
        assertEquals(childHitSlot, childHits.scoreDocs.length);
        currentParentID = parentID;
        childHitSlot = 0;
        childHits = joinResults.get(parentID);
      }

      String controlChildID = controlDoc.get("childID");
      Document childDoc = joinR.document(childHits.scoreDocs[childHitSlot++].doc);
      String childID = childDoc.get("childID");
      assertEquals(controlChildID, childID);
    }
  }

  public void testMultiChildTypes() throws Exception {

    final Directory dir = newDirectory();
    final RandomIndexWriter w = new RandomIndexWriter(random(), dir);

    final List<Document> docs = new ArrayList<>();

    docs.add(makeJob("java", 2007));
    docs.add(makeJob("python", 2010));
    docs.add(makeQualification("maths", 1999));
    docs.add(makeResume("Lisa", "United Kingdom"));
    w.addDocuments(docs);

    IndexReader r = w.getReader();
    w.close();
    IndexSearcher s = newSearcher(r, false);

    // Create a filter that defines "parent" documents in the index - in this case resumes
    BitSetProducer parentsFilter = new QueryBitSetProducer(new TermQuery(new Term("docType", "resume")));
    CheckJoinIndex.check(s.getIndexReader(), parentsFilter);

    // Define child document criteria (finds an example of relevant work experience)
    BooleanQuery.Builder childJobQuery = new BooleanQuery.Builder();
    childJobQuery.add(new BooleanClause(new TermQuery(new Term("skill", "java")), Occur.MUST));
    childJobQuery.add(new BooleanClause(IntPoint.newRangeQuery("year", 2006, 2011), Occur.MUST));

    BooleanQuery.Builder childQualificationQuery = new BooleanQuery.Builder();
    childQualificationQuery.add(new BooleanClause(new TermQuery(new Term("qualification", "maths")), Occur.MUST));
    childQualificationQuery.add(new BooleanClause(IntPoint.newRangeQuery("year", 1980, 2000), Occur.MUST));


    // Define parent document criteria (find a resident in the UK)
    Query parentQuery = new TermQuery(new Term("country", "United Kingdom"));

    // Wrap the child document query to 'join' any matches
    // up to corresponding parent:
    ToParentBlockJoinQuery childJobJoinQuery = new ToParentBlockJoinQuery(childJobQuery.build(), parentsFilter, ScoreMode.Avg);
    ToParentBlockJoinQuery childQualificationJoinQuery = new ToParentBlockJoinQuery(childQualificationQuery.build(), parentsFilter, ScoreMode.Avg);

    // Combine the parent and nested child queries into a single query for a candidate
    BooleanQuery.Builder fullQuery = new BooleanQuery.Builder();
    fullQuery.add(new BooleanClause(parentQuery, Occur.MUST));
    fullQuery.add(new BooleanClause(childJobJoinQuery, Occur.MUST));
    fullQuery.add(new BooleanClause(childQualificationJoinQuery, Occur.MUST));

    final TopDocs topDocs = s.search(fullQuery.build(), 10);
    assertEquals(1, topDocs.totalHits);
    Document parentDoc = s.doc(topDocs.scoreDocs[0].doc);
    assertEquals("Lisa", parentDoc.get("name"));

    ParentChildrenBlockJoinQuery childrenQuery =
        new ParentChildrenBlockJoinQuery(parentsFilter, childJobQuery.build(), topDocs.scoreDocs[0].doc);
    TopDocs matchingChildren = s.search(childrenQuery, 1);
    assertEquals(1, matchingChildren.totalHits);
    assertEquals("java", s.doc(matchingChildren.scoreDocs[0].doc).get("skill"));

    childrenQuery = new ParentChildrenBlockJoinQuery(parentsFilter, childQualificationQuery.build(), topDocs.scoreDocs[0].doc);
    matchingChildren = s.search(childrenQuery, 1);
    assertEquals(1, matchingChildren.totalHits);
    assertEquals("maths", s.doc(matchingChildren.scoreDocs[0].doc).get("qualification"));

    r.close();
    dir.close();
  }

  public void testAdvanceSingleParentSingleChild() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir);
    Document childDoc = new Document();
    childDoc.add(newStringField("child", "1", Field.Store.NO));
    Document parentDoc = new Document();
    parentDoc.add(newStringField("parent", "1", Field.Store.NO));
    w.addDocuments(Arrays.asList(childDoc, parentDoc));
    IndexReader r = w.getReader();
    w.close();
    IndexSearcher s = newSearcher(r);
    Query tq = new TermQuery(new Term("child", "1"));
    BitSetProducer parentFilter = new QueryBitSetProducer(
                              new TermQuery(new Term("parent", "1")));
    CheckJoinIndex.check(s.getIndexReader(), parentFilter);

    ToParentBlockJoinQuery q = new ToParentBlockJoinQuery(tq, parentFilter, ScoreMode.Avg);
    Weight weight = s.createNormalizedWeight(q, true);
    Scorer sc = weight.scorer(s.getIndexReader().leaves().get(0));
    assertEquals(1, sc.iterator().advance(1));
    r.close();
    dir.close();
  }

  public void testAdvanceSingleParentNoChild() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir, newIndexWriterConfig(new MockAnalyzer(random())).setMergePolicy(new LogDocMergePolicy()));
    Document parentDoc = new Document();
    parentDoc.add(newStringField("parent", "1", Field.Store.NO));
    parentDoc.add(newStringField("isparent", "yes", Field.Store.NO));
    w.addDocuments(Arrays.asList(parentDoc));

    // Add another doc so scorer is not null
    parentDoc = new Document();
    parentDoc.add(newStringField("parent", "2", Field.Store.NO));
    parentDoc.add(newStringField("isparent", "yes", Field.Store.NO));
    Document childDoc = new Document();
    childDoc.add(newStringField("child", "2", Field.Store.NO));
    w.addDocuments(Arrays.asList(childDoc, parentDoc));

    // Need single seg:
    w.forceMerge(1);
    IndexReader r = w.getReader();
    w.close();
    IndexSearcher s = newSearcher(r);
    Query tq = new TermQuery(new Term("child", "2"));
    BitSetProducer parentFilter = new QueryBitSetProducer(
                              new TermQuery(new Term("isparent", "yes")));
    CheckJoinIndex.check(s.getIndexReader(), parentFilter);

    ToParentBlockJoinQuery q = new ToParentBlockJoinQuery(tq, parentFilter, ScoreMode.Avg);
    Weight weight = s.createNormalizedWeight(q, true);
    Scorer sc = weight.scorer(s.getIndexReader().leaves().get(0));
    assertEquals(2, sc.iterator().advance(0));
    r.close();
    dir.close();
  }

  // LUCENE-4968
  public void testChildQueryNeverMatches() throws Exception {
    Directory d = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), d);
    Document parent = new Document();
    parent.add(new StoredField("parentID", "0"));
    parent.add(new SortedDocValuesField("parentID", new BytesRef("0")));
    parent.add(newTextField("parentText", "text", Field.Store.NO));
    parent.add(newStringField("isParent", "yes", Field.Store.NO));

    List<Document> docs = new ArrayList<>();

    Document child = new Document();
    docs.add(child);
    child.add(new StoredField("childID", "0"));
    child.add(newTextField("childText", "text", Field.Store.NO));

    // parent last:
    docs.add(parent);
    w.addDocuments(docs);

    docs.clear();

    parent = new Document();
    parent.add(newTextField("parentText", "text", Field.Store.NO));
    parent.add(newStringField("isParent", "yes", Field.Store.NO));
    parent.add(new StoredField("parentID", "1"));
    parent.add(new SortedDocValuesField("parentID", new BytesRef("1")));


    // parent last:
    docs.add(parent);
    w.addDocuments(docs);

    IndexReader r = w.getReader();
    w.close();

    IndexSearcher searcher = newSearcher(r);

    // never matches:
    Query childQuery = new TermQuery(new Term("childText", "bogus"));
    BitSetProducer parentsFilter = new QueryBitSetProducer(new TermQuery(new Term("isParent", "yes")));
    CheckJoinIndex.check(r, parentsFilter);
    ToParentBlockJoinQuery childJoinQuery = new ToParentBlockJoinQuery(childQuery, parentsFilter, ScoreMode.Avg);

    Weight weight = searcher.createNormalizedWeight(childJoinQuery, random().nextBoolean());
    Scorer scorer = weight.scorer(searcher.getIndexReader().leaves().get(0));
    assertNull(scorer);

    // never matches and produces a null scorer
    childQuery = new TermQuery(new Term("bogus", "bogus"));
    childJoinQuery = new ToParentBlockJoinQuery(childQuery, parentsFilter, ScoreMode.Avg);

    weight = searcher.createNormalizedWeight(childJoinQuery, random().nextBoolean());
    scorer = weight.scorer(searcher.getIndexReader().leaves().get(0));
    assertNull(scorer);

    r.close();
    d.close();
  }

  public void testAdvanceSingleDeletedParentNoChild() throws Exception {

    final Directory dir = newDirectory();
    final RandomIndexWriter w = new RandomIndexWriter(random(), dir);

    // First doc with 1 children
    Document parentDoc = new Document();
    parentDoc.add(newStringField("parent", "1", Field.Store.NO));
    parentDoc.add(newStringField("isparent", "yes", Field.Store.NO));
    Document childDoc = new Document();
    childDoc.add(newStringField("child", "1", Field.Store.NO));
    w.addDocuments(Arrays.asList(childDoc, parentDoc));

    parentDoc = new Document();
    parentDoc.add(newStringField("parent", "2", Field.Store.NO));
    parentDoc.add(newStringField("isparent", "yes", Field.Store.NO));
    w.addDocuments(Arrays.asList(parentDoc));

    w.deleteDocuments(new Term("parent", "2"));

    parentDoc = new Document();
    parentDoc.add(newStringField("parent", "2", Field.Store.NO));
    parentDoc.add(newStringField("isparent", "yes", Field.Store.NO));
    childDoc = new Document();
    childDoc.add(newStringField("child", "2", Field.Store.NO));
    w.addDocuments(Arrays.asList(childDoc, parentDoc));

    IndexReader r = w.getReader();
    w.close();
    IndexSearcher s = newSearcher(r);

    // Create a filter that defines "parent" documents in the index - in this case resumes
    BitSetProducer parentsFilter = new QueryBitSetProducer(new TermQuery(new Term("isparent", "yes")));
    CheckJoinIndex.check(r, parentsFilter);

    Query parentQuery = new TermQuery(new Term("parent", "2"));

    ToChildBlockJoinQuery parentJoinQuery = new ToChildBlockJoinQuery(parentQuery, parentsFilter);
    TopDocs topdocs = s.search(parentJoinQuery, 3);
    assertEquals(1, topdocs.totalHits);

    r.close();
    dir.close();
  }

  public void testIntersectionWithRandomApproximation() throws IOException {
    final Directory dir = newDirectory();
    final RandomIndexWriter w = new RandomIndexWriter(random(), dir);

    final int numBlocks = atLeast(100);
    for (int i = 0; i < numBlocks; ++i) {
      List<Document> docs = new ArrayList<>();
      final int numChildren = random().nextInt(3);
      for (int j = 0; j < numChildren; ++j) {
        Document child = new Document();
        child.add(new StringField("foo_child", random().nextBoolean() ? "bar" : "baz", Store.NO));
        docs.add(child);
      }
      Document parent = new Document();
      parent.add(new StringField("parent", "true", Store.NO));
      parent.add(new StringField("foo_parent", random().nextBoolean() ? "bar" : "baz", Store.NO));
      docs.add(parent);
      w.addDocuments(docs);
    }
    final IndexReader reader = w.getReader();
    final IndexSearcher searcher = newSearcher(reader);
    searcher.setQueryCache(null); // to have real advance() calls

    final BitSetProducer parentsFilter = new QueryBitSetProducer(new TermQuery(new Term("parent", "true")));
    final Query toChild = new ToChildBlockJoinQuery(new TermQuery(new Term("foo_parent", "bar")), parentsFilter);
    final Query childQuery = new TermQuery(new Term("foo_child", "baz"));

    BooleanQuery bq1 = new BooleanQuery.Builder()
        .add(toChild, Occur.MUST)
        .add(childQuery, Occur.MUST)
        .build();
    BooleanQuery bq2 = new BooleanQuery.Builder()
        .add(toChild, Occur.MUST)
        .add(new RandomApproximationQuery(childQuery, random()), Occur.MUST)
        .build();

    assertEquals(searcher.count(bq1), searcher.count(bq2));

    searcher.getIndexReader().close();
    w.close();
    dir.close();
  }

  //LUCENE-6588
  // delete documents to simulate FilteredQuery applying a filter as acceptDocs
  public void testParentScoringBug() throws Exception {
    final Directory dir = newDirectory();
    final RandomIndexWriter w = new RandomIndexWriter(random(), dir);

    final List<Document> docs = new ArrayList<>();
    docs.add(makeJob("java", 2007));
    docs.add(makeJob("python", 2010));
    docs.add(makeResume("Lisa", "United Kingdom"));
    w.addDocuments(docs);

    docs.clear();
    docs.add(makeJob("java", 2006));
    docs.add(makeJob("ruby", 2005));
    docs.add(makeResume("Frank", "United States"));
    w.addDocuments(docs);
    w.deleteDocuments(new Term("skill", "java")); // delete the first child of every parent

    IndexReader r = w.getReader();
    w.close();
    IndexSearcher s = newSearcher(r, false);

    // Create a filter that defines "parent" documents in the index - in this case resumes
    BitSetProducer parentsFilter = new QueryBitSetProducer(new TermQuery(new Term("docType", "resume")));
    Query parentQuery = new PrefixQuery(new Term("country", "United"));

    ToChildBlockJoinQuery toChildQuery = new ToChildBlockJoinQuery(parentQuery, parentsFilter);

    TopDocs hits = s.search(toChildQuery, 10);
    assertEquals(hits.scoreDocs.length, 2);
    for (int i = 0; i < hits.scoreDocs.length; i++) {
      if (hits.scoreDocs[i].score == 0.0)
        fail("Failed to calculate score for hit #"+i);
    }

    r.close();
    dir.close();
  }

  public void testToChildBlockJoinQueryExplain() throws Exception {
    final Directory dir = newDirectory();
    final RandomIndexWriter w = new RandomIndexWriter(random(), dir);

    final List<Document> docs = new ArrayList<>();
    docs.add(makeJob("java", 2007));
    docs.add(makeJob("python", 2010));
    docs.add(makeResume("Lisa", "United Kingdom"));
    w.addDocuments(docs);

    docs.clear();
    docs.add(makeJob("java", 2006));
    docs.add(makeJob("ruby", 2005));
    docs.add(makeResume("Frank", "United States"));
    w.addDocuments(docs);
    w.deleteDocuments(new Term("skill", "java")); // delete the first child of every parent

    IndexReader r = w.getReader();
    w.close();
    IndexSearcher s = newSearcher(r, false);

    // Create a filter that defines "parent" documents in the index - in this case resumes
    BitSetProducer parentsFilter = new QueryBitSetProducer(new TermQuery(new Term("docType", "resume")));
    Query parentQuery = new PrefixQuery(new Term("country", "United"));

    ToChildBlockJoinQuery toChildQuery = new ToChildBlockJoinQuery(parentQuery, parentsFilter);

    TopDocs hits = s.search(toChildQuery, 10);
    assertEquals(hits.scoreDocs.length, 2);
    for (int i = 0; i < hits.scoreDocs.length; i++) {
      assertEquals(hits.scoreDocs[i].score, s.explain(toChildQuery, hits.scoreDocs[i].doc).getValue(), 0.01);
    }

    r.close();
    dir.close();
  }

  public void testToChildInitialAdvanceParentButNoKids() throws Exception {

    final Directory dir = newDirectory();
    final RandomIndexWriter w = new RandomIndexWriter(random(), dir);

    // degenerate case: first doc has no children
    w.addDocument(makeResume("first", "nokids"));
    w.addDocuments(Arrays.asList(makeJob("job", 42), makeResume("second", "haskid")));

    // single segment
    w.forceMerge(1);

    final IndexReader r = w.getReader();
    final IndexSearcher s = newSearcher(r, false);
    w.close();

    BitSetProducer parentFilter = new QueryBitSetProducer(new TermQuery(new Term("docType", "resume")));
    Query parentQuery = new TermQuery(new Term("docType", "resume"));

    ToChildBlockJoinQuery parentJoinQuery = new ToChildBlockJoinQuery(parentQuery, parentFilter);

    Weight weight = s.createNormalizedWeight(parentJoinQuery, random().nextBoolean());
    Scorer advancingScorer = weight.scorer(s.getIndexReader().leaves().get(0));
    Scorer nextDocScorer = weight.scorer(s.getIndexReader().leaves().get(0));

    final int firstKid = nextDocScorer.iterator().nextDoc();
    assertTrue("firstKid not found", DocIdSetIterator.NO_MORE_DOCS != firstKid);
    assertEquals(firstKid, advancingScorer.iterator().advance(0));

    r.close();
    dir.close();
  }

  public void testMultiChildQueriesOfDiffParentLevels() throws Exception {

    final Directory dir = newDirectory();
    final RandomIndexWriter w = new RandomIndexWriter(random(), dir);

    // randomly generate resume->jobs[]->qualifications[]
    final int numResumes = atLeast(100);
    for (int r = 0; r < numResumes; r++) {
      final List<Document> docs = new ArrayList<>();

      final int rv = TestUtil.nextInt(random(), 1, 10);
      final int numJobs = atLeast(10);
      for (int j = 0; j < numJobs; j++) {
        final int jv = TestUtil.nextInt(random(), -10, -1); // neg so no overlap with q (both used for "year")

        final int numQualifications = atLeast(10);
        for (int q = 0; q < numQualifications; q++) {
          docs.add(makeQualification("q" + q + "_rv" + rv + "_jv" + jv, q));
        }
        docs.add(makeJob("j" + j, jv));
      }
      docs.add(makeResume("r" + r, "rv"+rv));
      w.addDocuments(docs);
    }

    final IndexReader r = w.getReader();
    final IndexSearcher s = newSearcher(r, false);
    w.close();

    BitSetProducer resumeFilter = new QueryBitSetProducer(new TermQuery(new Term("docType", "resume")));
    // anything with a skill is a job
    BitSetProducer jobFilter = new QueryBitSetProducer(new PrefixQuery(new Term("skill", "")));


    final int numQueryIters = atLeast(1);
    for (int i = 0; i < numQueryIters; i++) {
      final int qjv = TestUtil.nextInt(random(), -10, -1);
      final int qrv = TestUtil.nextInt(random(), 1, 10);

      Query resumeQuery = new ToChildBlockJoinQuery(new TermQuery(new Term("country","rv" + qrv)),
                                                    resumeFilter);

      Query jobQuery = new ToChildBlockJoinQuery(IntPoint.newRangeQuery("year", qjv, qjv),
                                                 jobFilter);

      BooleanQuery.Builder fullQuery = new BooleanQuery.Builder();
      fullQuery.add(new BooleanClause(jobQuery, Occur.MUST));
      fullQuery.add(new BooleanClause(resumeQuery, Occur.MUST));

      TopDocs hits = s.search(fullQuery.build(), 100); // NOTE: totally possible that we'll get no matches

      for (ScoreDoc sd : hits.scoreDocs) {
        // since we're looking for children of jobs, all results must be qualifications
        String q = r.document(sd.doc).get("qualification");
        assertNotNull(sd.doc + " has no qualification", q);
        assertTrue(q + " MUST contain jv" + qjv, q.contains("jv"+qjv));
        assertTrue(q + " MUST contain rv" + qrv, q.contains("rv"+qrv));
      }
    }

    r.close();
    dir.close();
  }


}
