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

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.CachingWrapperFilter;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.Filter;
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

import java.util.ArrayList;
import java.util.List;

/**
 */
public class TestBlockJoinSorting extends LuceneTestCase {

  @Test
  public void testNestedSorting() throws Exception {
    final Directory dir = newDirectory();
    final RandomIndexWriter w = new RandomIndexWriter(random(), dir);

    List<Document> docs = new ArrayList<Document>();
    Document document = new Document();
    document.add(new StringField("field2", "a", Field.Store.NO));
    document.add(new StringField("filter_1", "T", Field.Store.NO));
    docs.add(document);
    document = new Document();
    document.add(new StringField("field2", "b", Field.Store.NO));
    document.add(new StringField("filter_1", "T", Field.Store.NO));
    docs.add(document);
    document = new Document();
    document.add(new StringField("field2", "c", Field.Store.NO));
    document.add(new StringField("filter_1", "T", Field.Store.NO));
    docs.add(document);
    document = new Document();
    document.add(new StringField("__type", "parent", Field.Store.NO));
    document.add(new StringField("field1", "a", Field.Store.NO));
    docs.add(document);
    w.addDocuments(docs);
    w.commit();

    docs.clear();
    document = new Document();
    document.add(new StringField("field2", "c", Field.Store.NO));
    document.add(new StringField("filter_1", "T", Field.Store.NO));
    docs.add(document);
    document = new Document();
    document.add(new StringField("field2", "d", Field.Store.NO));
    document.add(new StringField("filter_1", "T", Field.Store.NO));
    docs.add(document);
    document = new Document();
    document.add(new StringField("field2", "e", Field.Store.NO));
    document.add(new StringField("filter_1", "T", Field.Store.NO));
    docs.add(document);
    document = new Document();
    document.add(new StringField("__type", "parent", Field.Store.NO));
    document.add(new StringField("field1", "b", Field.Store.NO));
    docs.add(document);
    w.addDocuments(docs);

    docs.clear();
    document = new Document();
    document.add(new StringField("field2", "e", Field.Store.NO));
    document.add(new StringField("filter_1", "T", Field.Store.NO));
    docs.add(document);
    document = new Document();
    document.add(new StringField("field2", "f", Field.Store.NO));
    document.add(new StringField("filter_1", "T", Field.Store.NO));
    docs.add(document);
    document = new Document();
    document.add(new StringField("field2", "g", Field.Store.NO));
    document.add(new StringField("filter_1", "T", Field.Store.NO));
    docs.add(document);
    document = new Document();
    document.add(new StringField("__type", "parent", Field.Store.NO));
    document.add(new StringField("field1", "c", Field.Store.NO));
    docs.add(document);
    w.addDocuments(docs);

    docs.clear();
    document = new Document();
    document.add(new StringField("field2", "g", Field.Store.NO));
    document.add(new StringField("filter_1", "T", Field.Store.NO));
    docs.add(document);
    document = new Document();
    document.add(new StringField("field2", "h", Field.Store.NO));
    document.add(new StringField("filter_1", "F", Field.Store.NO));
    docs.add(document);
    document = new Document();
    document.add(new StringField("field2", "i", Field.Store.NO));
    document.add(new StringField("filter_1", "F", Field.Store.NO));
    docs.add(document);
    document = new Document();
    document.add(new StringField("__type", "parent", Field.Store.NO));
    document.add(new StringField("field1", "d", Field.Store.NO));
    docs.add(document);
    w.addDocuments(docs);
    w.commit();

    docs.clear();
    document = new Document();
    document.add(new StringField("field2", "i", Field.Store.NO));
    document.add(new StringField("filter_1", "F", Field.Store.NO));
    docs.add(document);
    document = new Document();
    document.add(new StringField("field2", "j", Field.Store.NO));
    document.add(new StringField("filter_1", "F", Field.Store.NO));
    docs.add(document);
    document = new Document();
    document.add(new StringField("field2", "k", Field.Store.NO));
    document.add(new StringField("filter_1", "F", Field.Store.NO));
    docs.add(document);
    document = new Document();
    document.add(new StringField("__type", "parent", Field.Store.NO));
    document.add(new StringField("field1", "f", Field.Store.NO));
    docs.add(document);
    w.addDocuments(docs);

    docs.clear();
    document = new Document();
    document.add(new StringField("field2", "k", Field.Store.NO));
    document.add(new StringField("filter_1", "T", Field.Store.NO));
    docs.add(document);
    document = new Document();
    document.add(new StringField("field2", "l", Field.Store.NO));
    document.add(new StringField("filter_1", "T", Field.Store.NO));
    docs.add(document);
    document = new Document();
    document.add(new StringField("field2", "m", Field.Store.NO));
    document.add(new StringField("filter_1", "T", Field.Store.NO));
    docs.add(document);
    document = new Document();
    document.add(new StringField("__type", "parent", Field.Store.NO));
    document.add(new StringField("field1", "g", Field.Store.NO));
    docs.add(document);
    w.addDocuments(docs);

    // This doc will not be included, because it doesn't have nested docs
    document = new Document();
    document.add(new StringField("__type", "parent", Field.Store.NO));
    document.add(new StringField("field1", "h", Field.Store.NO));
    w.addDocument(document);

    docs.clear();
    document = new Document();
    document.add(new StringField("field2", "m", Field.Store.NO));
    document.add(new StringField("filter_1", "T", Field.Store.NO));
    docs.add(document);
    document = new Document();
    document.add(new StringField("field2", "n", Field.Store.NO));
    document.add(new StringField("filter_1", "F", Field.Store.NO));
    docs.add(document);
    document = new Document();
    document.add(new StringField("field2", "o", Field.Store.NO));
    document.add(new StringField("filter_1", "F", Field.Store.NO));
    docs.add(document);
    document = new Document();
    document.add(new StringField("__type", "parent", Field.Store.NO));
    document.add(new StringField("field1", "i", Field.Store.NO));
    docs.add(document);
    w.addDocuments(docs);
    w.commit();

    // Some garbage docs, just to check if the NestedFieldComparator can deal with this.
    document = new Document();
    document.add(new StringField("fieldXXX", "x", Field.Store.NO));
    w.addDocument(document);
    document = new Document();
    document.add(new StringField("fieldXXX", "x", Field.Store.NO));
    w.addDocument(document);
    document = new Document();
    document.add(new StringField("fieldXXX", "x", Field.Store.NO));
    w.addDocument(document);

    IndexSearcher searcher = new IndexSearcher(DirectoryReader.open(w.w, false));
    w.close();
    Filter parentFilter = new QueryWrapperFilter(new TermQuery(new Term("__type", "parent")));
    Filter childFilter = new QueryWrapperFilter(new PrefixQuery(new Term("field2")));
    ToParentBlockJoinQuery query = new ToParentBlockJoinQuery(
        new FilteredQuery(new MatchAllDocsQuery(), childFilter),
        new CachingWrapperFilter(parentFilter),
        ScoreMode.None
    );

    // Sort by field ascending, order first
    ToParentBlockJoinSortField sortField = new ToParentBlockJoinSortField(
        "field2", SortField.Type.STRING, false, wrap(parentFilter), wrap(childFilter)
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
        "field2", SortField.Type.STRING, false, true, wrap(parentFilter), wrap(childFilter)
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
        "field2", SortField.Type.STRING, true, wrap(parentFilter), wrap(childFilter)
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
    childFilter = new QueryWrapperFilter(new TermQuery((new Term("filter_1", "T"))));
    query = new ToParentBlockJoinQuery(
        new FilteredQuery(new MatchAllDocsQuery(), childFilter),
        new CachingWrapperFilter(parentFilter),
        ScoreMode.None
    );
    sortField = new ToParentBlockJoinSortField(
        "field2", SortField.Type.STRING, true, wrap(parentFilter), wrap(childFilter)
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

  private Filter wrap(Filter filter) {
    return random().nextBoolean() ? new CachingWrapperFilter(filter) : filter;
  }

}
