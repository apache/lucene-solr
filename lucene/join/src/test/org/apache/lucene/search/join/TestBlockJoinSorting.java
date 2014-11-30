package org.apache.lucene.search.join;

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

import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.FilteredQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.QueryWrapperFilter;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;
import org.junit.Test;

/**
 */
public class TestBlockJoinSorting extends LuceneTestCase {

  @Test
  public void testNestedSorting() throws Exception {
    final Directory dir = newDirectory();
    final RandomIndexWriter w = new RandomIndexWriter(random(), dir, newIndexWriterConfig(new MockAnalyzer(random()))
        .setMergePolicy(NoMergePolicy.INSTANCE));

    List<Document> docs = new ArrayList<>();
    Document document = w.newDocument();
    document.addAtom("field2", "a");
    document.addAtom("filter_1", "T");
    docs.add(document);
    document = w.newDocument();
    document.addAtom("field2", "b");
    document.addAtom("filter_1", "T");
    docs.add(document);
    document = w.newDocument();
    document.addAtom("field2", "c");
    document.addAtom("filter_1", "T");
    docs.add(document);
    document = w.newDocument();
    document.addAtom("__type", "parent");
    document.addAtom("field1", "a");
    docs.add(document);
    w.addDocuments(docs);
    w.commit();

    docs.clear();
    document = w.newDocument();
    document.addAtom("field2", "c");
    document.addAtom("filter_1", "T");
    docs.add(document);
    document = w.newDocument();
    document.addAtom("field2", "d");
    document.addAtom("filter_1", "T");
    docs.add(document);
    document = w.newDocument();
    document.addAtom("field2", "e");
    document.addAtom("filter_1", "T");
    docs.add(document);
    document = w.newDocument();
    document.addAtom("__type", "parent");
    document.addAtom("field1", "b");
    docs.add(document);
    w.addDocuments(docs);

    docs.clear();
    document = w.newDocument();
    document.addAtom("field2", "e");
    document.addAtom("filter_1", "T");
    docs.add(document);
    document = w.newDocument();
    document.addAtom("field2", "f");
    document.addAtom("filter_1", "T");
    docs.add(document);
    document = w.newDocument();
    document.addAtom("field2", "g");
    document.addAtom("filter_1", "T");
    docs.add(document);
    document = w.newDocument();
    document.addAtom("__type", "parent");
    document.addAtom("field1", "c");
    docs.add(document);
    w.addDocuments(docs);

    docs.clear();
    document = w.newDocument();
    document.addAtom("field2", "g");
    document.addAtom("filter_1", "T");
    docs.add(document);
    document = w.newDocument();
    document.addAtom("field2", "h");
    document.addAtom("filter_1", "F");
    docs.add(document);
    document = w.newDocument();
    document.addAtom("field2", "i");
    document.addAtom("filter_1", "F");
    docs.add(document);
    document = w.newDocument();
    document.addAtom("__type", "parent");
    document.addAtom("field1", "d");
    docs.add(document);
    w.addDocuments(docs);
    w.commit();

    docs.clear();
    document = w.newDocument();
    document.addAtom("field2", "i");
    document.addAtom("filter_1", "F");
    docs.add(document);
    document = w.newDocument();
    document.addAtom("field2", "j");
    document.addAtom("filter_1", "F");
    docs.add(document);
    document = w.newDocument();
    document.addAtom("field2", "k");
    document.addAtom("filter_1", "F");
    docs.add(document);
    document = w.newDocument();
    document.addAtom("__type", "parent");
    document.addAtom("field1", "f");
    docs.add(document);
    w.addDocuments(docs);

    docs.clear();
    document = w.newDocument();
    document.addAtom("field2", "k");
    document.addAtom("filter_1", "T");
    docs.add(document);
    document = w.newDocument();
    document.addAtom("field2", "l");
    document.addAtom("filter_1", "T");
    docs.add(document);
    document = w.newDocument();
    document.addAtom("field2", "m");
    document.addAtom("filter_1", "T");
    docs.add(document);
    document = w.newDocument();
    document.addAtom("__type", "parent");
    document.addAtom("field1", "g");
    docs.add(document);
    w.addDocuments(docs);

    // This doc will not be included, because it doesn't have nested docs
    document = w.newDocument();
    document.addAtom("__type", "parent");
    document.addAtom("field1", "h");
    w.addDocument(document);

    docs.clear();
    document = w.newDocument();
    document.addAtom("field2", "m");
    document.addAtom("filter_1", "T");
    docs.add(document);
    document = w.newDocument();
    document.addAtom("field2", "n");
    document.addAtom("filter_1", "F");
    docs.add(document);
    document = w.newDocument();
    document.addAtom("field2", "o");
    document.addAtom("filter_1", "F");
    docs.add(document);
    document = w.newDocument();
    document.addAtom("__type", "parent");
    document.addAtom("field1", "i");
    docs.add(document);
    w.addDocuments(docs);
    w.commit();

    // Some garbage docs, just to check if the NestedFieldComparator can deal with this.
    document = w.newDocument();
    document.addAtom("fieldXXX", "x");
    w.addDocument(document);
    document = w.newDocument();
    document.addAtom("fieldXXX", "x");
    w.addDocument(document);
    document = w.newDocument();
    document.addAtom("fieldXXX", "x");
    w.addDocument(document);

    IndexSearcher searcher = new IndexSearcher(DirectoryReader.open(w.w, false));
    w.close();
    BitDocIdSetFilter parentFilter = new BitDocIdSetCachingWrapperFilter(new QueryWrapperFilter(new TermQuery(new Term("__type", "parent"))));
    BitDocIdSetFilter childFilter = new BitDocIdSetCachingWrapperFilter(new QueryWrapperFilter(new PrefixQuery(new Term("field2"))));
    ToParentBlockJoinQuery query = new ToParentBlockJoinQuery(
        new FilteredQuery(new MatchAllDocsQuery(), childFilter),
        new BitDocIdSetCachingWrapperFilter(parentFilter),
        ScoreMode.None
    );

    // Sort by field ascending, order first
    ToParentBlockJoinSortField sortField = new ToParentBlockJoinSortField(
        "field2", SortField.Type.STRING, false, parentFilter, childFilter
    );
    Sort sort = new Sort(sortField);
    TopFieldDocs topDocs = searcher.search(query, 5, sort);
    assertEquals(7, topDocs.totalHits);
    assertEquals(5, topDocs.scoreDocs.length);
    assertEquals(3, topDocs.scoreDocs[0].doc);
    assertEquals("a", ((BytesRef) ((FieldDoc) topDocs.scoreDocs[0]).fields[0]).utf8ToString());
    assertEquals(7, topDocs.scoreDocs[1].doc);
    assertEquals("c", ((BytesRef) ((FieldDoc) topDocs.scoreDocs[1]).fields[0]).utf8ToString());
    assertEquals(11, topDocs.scoreDocs[2].doc);
    assertEquals("e", ((BytesRef) ((FieldDoc) topDocs.scoreDocs[2]).fields[0]).utf8ToString());
    assertEquals(15, topDocs.scoreDocs[3].doc);
    assertEquals("g", ((BytesRef) ((FieldDoc) topDocs.scoreDocs[3]).fields[0]).utf8ToString());
    assertEquals(19, topDocs.scoreDocs[4].doc);
    assertEquals("i", ((BytesRef) ((FieldDoc) topDocs.scoreDocs[4]).fields[0]).utf8ToString());

    // Sort by field ascending, order last
    sortField = new ToParentBlockJoinSortField(
        "field2", SortField.Type.STRING, false, true, parentFilter, childFilter
    );
    sort = new Sort(sortField);
    topDocs = searcher.search(query, 5, sort);
    assertEquals(7, topDocs.totalHits);
    assertEquals(5, topDocs.scoreDocs.length);
    assertEquals(3, topDocs.scoreDocs[0].doc);
    assertEquals("c", ((BytesRef) ((FieldDoc) topDocs.scoreDocs[0]).fields[0]).utf8ToString());
    assertEquals(7, topDocs.scoreDocs[1].doc);
    assertEquals("e", ((BytesRef) ((FieldDoc) topDocs.scoreDocs[1]).fields[0]).utf8ToString());
    assertEquals(11, topDocs.scoreDocs[2].doc);
    assertEquals("g", ((BytesRef) ((FieldDoc) topDocs.scoreDocs[2]).fields[0]).utf8ToString());
    assertEquals(15, topDocs.scoreDocs[3].doc);
    assertEquals("i", ((BytesRef) ((FieldDoc) topDocs.scoreDocs[3]).fields[0]).utf8ToString());
    assertEquals(19, topDocs.scoreDocs[4].doc);
    assertEquals("k", ((BytesRef) ((FieldDoc) topDocs.scoreDocs[4]).fields[0]).utf8ToString());

    // Sort by field descending, order last
    sortField = new ToParentBlockJoinSortField(
        "field2", SortField.Type.STRING, true, parentFilter, childFilter
    );
    sort = new Sort(sortField);
    topDocs = searcher.search(query, 5, sort);
    assertEquals(topDocs.totalHits, 7);
    assertEquals(5, topDocs.scoreDocs.length);
    assertEquals(28, topDocs.scoreDocs[0].doc);
    assertEquals("o", ((BytesRef) ((FieldDoc) topDocs.scoreDocs[0]).fields[0]).utf8ToString());
    assertEquals(23, topDocs.scoreDocs[1].doc);
    assertEquals("m", ((BytesRef) ((FieldDoc) topDocs.scoreDocs[1]).fields[0]).utf8ToString());
    assertEquals(19, topDocs.scoreDocs[2].doc);
    assertEquals("k", ((BytesRef) ((FieldDoc) topDocs.scoreDocs[2]).fields[0]).utf8ToString());
    assertEquals(15, topDocs.scoreDocs[3].doc);
    assertEquals("i", ((BytesRef) ((FieldDoc) topDocs.scoreDocs[3]).fields[0]).utf8ToString());
    assertEquals(11, topDocs.scoreDocs[4].doc);
    assertEquals("g", ((BytesRef) ((FieldDoc) topDocs.scoreDocs[4]).fields[0]).utf8ToString());

    // Sort by field descending, order last, sort filter (filter_1:T)
    childFilter = new BitDocIdSetCachingWrapperFilter(new QueryWrapperFilter(new TermQuery((new Term("filter_1", "T")))));
    query = new ToParentBlockJoinQuery(
        new FilteredQuery(new MatchAllDocsQuery(), childFilter),
        new BitDocIdSetCachingWrapperFilter(parentFilter),
        ScoreMode.None
    );
    sortField = new ToParentBlockJoinSortField(
        "field2", SortField.Type.STRING, true, parentFilter, childFilter
    );
    sort = new Sort(sortField);
    topDocs = searcher.search(query, 5, sort);
    assertEquals(6, topDocs.totalHits);
    assertEquals(5, topDocs.scoreDocs.length);
    assertEquals(23, topDocs.scoreDocs[0].doc);
    assertEquals("m", ((BytesRef) ((FieldDoc) topDocs.scoreDocs[0]).fields[0]).utf8ToString());
    assertEquals(28, topDocs.scoreDocs[1].doc);
    assertEquals("m", ((BytesRef) ((FieldDoc) topDocs.scoreDocs[1]).fields[0]).utf8ToString());
    assertEquals(11, topDocs.scoreDocs[2].doc);
    assertEquals("g", ((BytesRef) ((FieldDoc) topDocs.scoreDocs[2]).fields[0]).utf8ToString());
    assertEquals(15, topDocs.scoreDocs[3].doc);
    assertEquals("g", ((BytesRef) ((FieldDoc) topDocs.scoreDocs[3]).fields[0]).utf8ToString());
    assertEquals(7, topDocs.scoreDocs[4].doc);
    assertEquals("e", ((BytesRef) ((FieldDoc) topDocs.scoreDocs[4]).fields[0]).utf8ToString());

    searcher.getIndexReader().close();
    dir.close();
  }

}
