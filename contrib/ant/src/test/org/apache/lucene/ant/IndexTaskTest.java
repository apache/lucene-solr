package org.apache.lucene.ant;

/**
 * Copyright 2005 The Apache Software Foundation
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

import java.io.File;

import java.io.IOException;

import junit.framework.TestCase;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.StopAnalyzer;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.Hits;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Searcher;

import org.apache.tools.ant.Project;
import org.apache.tools.ant.types.FileSet;

/**
 *  Test cases for index task
 *
 *@author     Erik Hatcher
 */
public class IndexTaskTest extends TestCase {
    private final static String docHandler =
            "org.apache.lucene.ant.FileExtensionDocumentHandler";

    private String docsDir = System.getProperty("docs.dir");
    private String indexDir = System.getProperty("index.dir");

    private Searcher searcher;
    private Analyzer analyzer;


    /**
     *  The JUnit setup method
     *
     *@exception  IOException  Description of Exception
     */
    public void setUp() throws Exception {
        Project project = new Project();

        IndexTask task = new IndexTask();
        FileSet fs = new FileSet();
        fs.setDir(new File(docsDir));
        task.addFileset(fs);
        task.setOverwrite(true);
        task.setDocumentHandler(docHandler);
        task.setIndex(new File(indexDir));
        task.setProject(project);
        task.execute();

        searcher = new IndexSearcher(indexDir);
        analyzer = new StopAnalyzer();
    }


    public void testSearch() throws Exception {
        Query query = new QueryParser("contents",analyzer).parse("test");

        Hits hits = searcher.search(query);

        assertEquals("Find document(s)", 2, hits.length());
    }

    /**
     *  The teardown method for JUnit
     * @todo remove indexDir?
     */
    public void tearDown() throws IOException {
        searcher.close();
    }
}

