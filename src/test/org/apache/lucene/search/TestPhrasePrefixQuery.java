package org.apache.lucene.search;

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

import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.analysis.SimpleAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermEnum;
import org.apache.lucene.store.RAMDirectory;

import java.io.IOException;
import java.util.LinkedList;

/**
 * This class tests PhrasePrefixQuery class.
 *
 *
 * @version $Id$
 */
public class TestPhrasePrefixQuery
    extends LuceneTestCase
{
    public TestPhrasePrefixQuery(String name)
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
        IndexWriter writer = new IndexWriter(indexStore, new SimpleAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
        Document doc1 = new Document();
        Document doc2 = new Document();
        Document doc3 = new Document();
        Document doc4 = new Document();
        Document doc5 = new Document();
        doc1.add(new Field("body", "blueberry pie", Field.Store.YES, Field.Index.ANALYZED));
        doc2.add(new Field("body", "blueberry strudel", Field.Store.YES, Field.Index.ANALYZED));
        doc3.add(new Field("body", "blueberry pizza", Field.Store.YES, Field.Index.ANALYZED));
        doc4.add(new Field("body", "blueberry chewing gum", Field.Store.YES, Field.Index.ANALYZED));
        doc5.add(new Field("body", "piccadilly circus", Field.Store.YES, Field.Index.ANALYZED));
        writer.addDocument(doc1);
        writer.addDocument(doc2);
        writer.addDocument(doc3);
        writer.addDocument(doc4);
        writer.addDocument(doc5);
        writer.optimize();
        writer.close();

        IndexSearcher searcher = new IndexSearcher(indexStore);

        //PhrasePrefixQuery query1 = new PhrasePrefixQuery();
        MultiPhraseQuery query1 = new MultiPhraseQuery();
        //PhrasePrefixQuery query2 = new PhrasePrefixQuery();
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
        query2.add((Term[])termsWithPrefix.toArray(new Term[0]));

        ScoreDoc[] result;
        result = searcher.search(query1, null, 1000).scoreDocs;
        assertEquals(2, result.length);

        result = searcher.search(query2, null, 1000).scoreDocs;
        assertEquals(0, result.length);
    }
}
