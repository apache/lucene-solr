package org.apache.lucene.ant;

import java.io.File;

import java.io.IOException;

import junit.framework.TestCase;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.StopAnalyzer;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.Hits;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Searcher;
import org.apache.lucene.ant.IndexTask;

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
     *  Constructor for the IndexTaskTest object
     *
     *@param  name  Description of Parameter
     */
    public IndexTaskTest(String name) {
        super(name);
    }


    /**
     *  The JUnit setup method
     *
     *@exception  IOException  Description of Exception
     */
    public void setUp() throws IOException {
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


    /**
     *  A unit test for JUnit
     */
    public void testSearch() throws IOException, ParseException {
        System.out.println("sysout");
        System.err.println("syserr");
        Query query = QueryParser.parse("test", "contents", analyzer);

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

