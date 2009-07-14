package org.apache.lucene.search.similar;

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

import java.io.IOException;
import java.io.StringReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriter.MaxFieldLength;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.LuceneTestCase;

public class TestMoreLikeThis extends LuceneTestCase {
    private RAMDirectory directory;
    private IndexReader reader;
    private IndexSearcher searcher;

    protected void setUp() throws Exception {
	directory = new RAMDirectory();
	IndexWriter writer = new IndexWriter(directory, new StandardAnalyzer(),
		true, MaxFieldLength.UNLIMITED);

	// Add series of docs with specific information for MoreLikeThis
	addDoc(writer, "lucene");
	addDoc(writer, "lucene release");

	writer.close();
	reader = IndexReader.open(directory, true);
	searcher = new IndexSearcher(reader);

    }

    protected void tearDown() throws Exception {
	reader.close();
	searcher.close();
	directory.close();
    }

    private void addDoc(IndexWriter writer, String text) throws IOException {
	Document doc = new Document();
	doc.add(new Field("text", text, Field.Store.YES, Field.Index.ANALYZED));
	writer.addDocument(doc);
    }

    public void testBoostFactor() throws Throwable {
	Map originalValues = getOriginalValues();

	MoreLikeThis mlt = new MoreLikeThis(
		reader);
	mlt.setMinDocFreq(1);
	mlt.setMinTermFreq(1);
	mlt.setMinWordLen(1);
	mlt.setFieldNames(new String[] { "text" });
	mlt.setBoost(true);

	// this mean that every term boost factor will be multiplied by this
	// number
	float boostFactor = 5;
	mlt.setBoostFactor(boostFactor);

	BooleanQuery query = (BooleanQuery) mlt.like(new StringReader(
		"lucene release"));
	List clauses = query.clauses();

	assertEquals("Expected " + originalValues.size() + " clauses.",
		originalValues.size(), clauses.size());

	for (int i = 0; i < clauses.size(); i++) {
	    BooleanClause clause = (BooleanClause) clauses.get(i);
	    TermQuery tq = (TermQuery) clause.getQuery();
	    Float termBoost = (Float) originalValues.get(tq.getTerm().text());
	    assertNotNull("Expected term " + tq.getTerm().text(), termBoost);

	    float totalBoost = termBoost.floatValue() * boostFactor;
	    assertEquals("Expected boost of " + totalBoost + " for term '"
                         + tq.getTerm().text() + "' got " + tq.getBoost(),
                         totalBoost, tq.getBoost(), 0.0001);
	}
    }

    private Map getOriginalValues() throws IOException {
	Map originalValues = new HashMap();
	MoreLikeThis mlt = new MoreLikeThis(reader);
	mlt.setMinDocFreq(1);
	mlt.setMinTermFreq(1);
	mlt.setMinWordLen(1);
	mlt.setFieldNames(new String[] { "text" });
	mlt.setBoost(true);
	BooleanQuery query = (BooleanQuery) mlt.like(new StringReader(
		"lucene release"));
	List clauses = query.clauses();

	for (int i = 0; i < clauses.size(); i++) {
	    BooleanClause clause = (BooleanClause) clauses.get(i);
	    TermQuery tq = (TermQuery) clause.getQuery();
	    originalValues.put(tq.getTerm().text(), new Float(tq.getBoost()));
	}
	return originalValues;
    }
}
