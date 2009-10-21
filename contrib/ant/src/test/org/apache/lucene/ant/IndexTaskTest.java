package org.apache.lucene.ant;

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

import java.io.File;
import java.io.IOException;

import junit.framework.TestCase;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.StopAnalyzer;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Searcher;
import org.apache.lucene.store.FSDirectory;
import org.apache.tools.ant.Project;
import org.apache.tools.ant.types.FileSet;

/**
 *  Test cases for index task
 *
 */
public class IndexTaskTest extends TestCase {
    private final static String docHandler =
            "org.apache.lucene.ant.FileExtensionDocumentHandler";

    private String docsDir = System.getProperty("docs.dir");
    private File indexDir = new File(System.getProperty("index.dir"));

    private Searcher searcher;
    private Analyzer analyzer;
    private FSDirectory dir;


    /**
     *  The JUnit setup method
     *
     *@exception  IOException  Description of Exception
     */
    public void setUp() throws Exception {
        Project project = new Project();

        IndexTask task = new IndexTask();
        FileSet fs = new FileSet();
        fs.setProject(project);
        fs.setDir(new File(docsDir));
        task.addFileset(fs);
        task.setOverwrite(true);
        task.setDocumentHandler(docHandler);
        task.setIndex(indexDir);
        task.setProject(project);
        task.execute();

        dir = FSDirectory.open(indexDir);
        searcher = new IndexSearcher(dir, true);
        analyzer = new StopAnalyzer(false);
    }


    public void testSearch() throws Exception {
        Query query = new QueryParser("contents",analyzer).parse("test");

        int numHits = searcher.search(query, null, 1000).totalHits;

        assertEquals("Find document(s)", 2, numHits);
    }

    /**
     *  The teardown method for JUnit
     * TODO: remove indexDir?
     */
    public void tearDown() throws IOException {
        searcher.close();
        dir.close();
    }
}

