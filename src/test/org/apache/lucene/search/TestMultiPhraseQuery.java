package org.apache.lucene.search;

/**
 * Copyright 2004 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermEnum;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.analysis.SimpleAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;

import junit.framework.TestCase;

import java.io.IOException;
import java.util.LinkedList;

/**
 * This class tests the MultiPhraseQuery class.
 *
 * @author Otis Gospodnetic, Daniel Naber
 * @version $Id$
 */
public class TestMultiPhraseQuery
    extends TestCase
{
    public TestMultiPhraseQuery(String name)
    {
        super(name);
    }

    /**
     *
     */
    public void testPhrasePrefix()
        throws IOException
    {
        RAMDirectory indexStore = new RAMDirectory();
        IndexWriter writer = new IndexWriter(indexStore, new SimpleAnalyzer(), true);
        Document doc1 = new Document();
        Document doc2 = new Document();
        Document doc3 = new Document();
        Document doc4 = new Document();
        Document doc5 = new Document();
        doc1.add(new Field("body", "blueberry pie", Field.Store.YES, Field.Index.TOKENIZED));
        doc2.add(new Field("body", "blueberry strudel", Field.Store.YES, Field.Index.TOKENIZED));
        doc3.add(new Field("body", "blueberry pizza", Field.Store.YES, Field.Index.TOKENIZED));
        doc4.add(new Field("body", "blueberry chewing gum", Field.Store.YES, Field.Index.TOKENIZED));
        doc4.add(new Field("body", "bluebird pizza", Field.Store.YES, Field.Index.TOKENIZED));
        doc5.add(new Field("body", "piccadilly circus", Field.Store.YES, Field.Index.TOKENIZED));
        writer.addDocument(doc1);
        writer.addDocument(doc2);
        writer.addDocument(doc3);
        writer.addDocument(doc4);
        writer.addDocument(doc5);
        writer.optimize();
        writer.close();

        IndexSearcher searcher = new IndexSearcher(indexStore);

        // search for "blueberry pi*":
        MultiPhraseQuery query1 = new MultiPhraseQuery();
        // search for "strawberry pi*":
        MultiPhraseQuery query2 = new MultiPhraseQuery();
        query1.add(new Term("body", "blueberry"));
        query2.add(new Term("body", "strawberry"));

        LinkedList termsWithPrefix = new LinkedList();
        IndexReader ir = IndexReader.open(indexStore);

        // this TermEnum gives "piccadilly", "pie" and "pizza".
        String prefix = "pi";
        TermEnum te = ir.terms(new Term("body", prefix + "*"));
        do {
            if (te.term().text().startsWith(prefix))
            {
                termsWithPrefix.add(te.term());
            }
        } while (te.next());

        query1.add((Term[])termsWithPrefix.toArray(new Term[0]));
        assertEquals("body:\"blueberry (piccadilly pie pizza)\"", query1.toString());
        query2.add((Term[])termsWithPrefix.toArray(new Term[0]));
        assertEquals("body:\"strawberry (piccadilly pie pizza)\"", query2.toString());

        Hits result;
        result = searcher.search(query1);
        assertEquals(2, result.length());
        result = searcher.search(query2);
        assertEquals(0, result.length());

        // search for "blue* pizza":
        MultiPhraseQuery query3 = new MultiPhraseQuery();
        termsWithPrefix.clear();
        prefix = "blue";
        te = ir.terms(new Term("body", prefix + "*"));
        do {
            if (te.term().text().startsWith(prefix))
            {
                termsWithPrefix.add(te.term());
            }
        } while (te.next());
        query3.add((Term[])termsWithPrefix.toArray(new Term[0]));
        query3.add(new Term("body", "pizza"));

        result = searcher.search(query3);
        assertEquals(2, result.length()); // blueberry pizza, blueberry chewing gum
        assertEquals("body:\"(blueberry bluebird) pizza\"", query3.toString());

        MultiPhraseQuery query4 = new MultiPhraseQuery();
        try {
          query4.add(new Term("field1", "foo"));
          query4.add(new Term("field2", "foobar"));
          fail();
        } catch(IllegalArgumentException e) {
          // okay, all terms must belong to the same field
        }

    }
}
