package org.apache.lucene.benchmark.standard;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.benchmark.AbstractBenchmarker;
import org.apache.lucene.benchmark.BenchmarkOptions;
import org.apache.lucene.benchmark.Benchmarker;
import org.apache.lucene.benchmark.stats.QueryData;
import org.apache.lucene.benchmark.stats.TestData;
import org.apache.lucene.benchmark.stats.TestRunData;
import org.apache.lucene.benchmark.stats.TimeData;
import org.apache.lucene.document.DateTools;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.Hits;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.FSDirectory;

import java.io.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
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


/**
 *  Reads in the Reuters Collection, downloaded from http://www.daviddlewis.com/resources/testcollections/reuters21578/reuters21578.tar.gz
 * in the workingDir/reuters and indexes them using the {@link org.apache.lucene.analysis.standard.StandardAnalyzer}
 *<p/>
 * Runs a standard set of documents through an Indexer and then runs a standard set of queries against the index.
 *
 * @see org.apache.lucene.benchmark.standard.StandardBenchmarker#benchmark(java.io.File, org.apache.lucene.benchmark.BenchmarkOptions)
 *
 *
 **/
public class StandardBenchmarker extends AbstractBenchmarker implements Benchmarker
{
    public static final String SOURCE_DIR = "reuters-out";

    public static final String INDEX_DIR = "index";
    //30-MAR-1987 14:22:36.87
    private static DateFormat format = new SimpleDateFormat("dd-MMM-yyyy kk:mm:ss.SSS",Locale.US);
    //DateFormat.getDateTimeInstance(DateFormat.MEDIUM, DateFormat.SHORT);
    static{
        format.setLenient(true);
    }


    public StandardBenchmarker()
    {
    }

    public TestData [] benchmark(File workingDir, BenchmarkOptions opts) throws Exception
    {
        StandardOptions options = (StandardOptions) opts;
        workingDir.mkdirs();
        File sourceDir = getSourceDirectory(workingDir);

        sourceDir.mkdirs();
        File indexDir = new File(workingDir, INDEX_DIR);
        indexDir.mkdirs();
        Analyzer a = new StandardAnalyzer();
        List queryList = new ArrayList(20);
        queryList.addAll(Arrays.asList(ReutersQueries.STANDARD_QUERIES));
        queryList.addAll(Arrays.asList(ReutersQueries.getPrebuiltQueries("body")));
        Query[] qs = createQueries(queryList, a);
        // Here you can limit the set of query benchmarks
        QueryData[] qds = QueryData.getAll(qs);
        // Here you can narrow down the set of test parameters
        TestData[] params = TestData.getTestDataMinMaxMergeAndMaxBuffered(new File[]{sourceDir/*, jumboDir*/}, new Analyzer[]{a});//TestData.getAll(new File[]{sourceDir, jumboDir}, new Analyzer[]{a});
        System.out.println("Testing " + params.length + " different permutations.");
        for (int i = 0; i < params.length; i++)
        {
            try
            {
                reset(indexDir);
                params[i].setDirectory(FSDirectory.getDirectory(indexDir, true));
                params[i].setQueries(qds);
                System.out.println(params[i]);
                runBenchmark(params[i], options);
                // Here you can collect and output the runData for further processing.
                System.out.println(params[i].showRunData(params[i].getId()));
                //bench.runSearchBenchmark(queries, dir);
                params[i].getDirectory().close();
                System.runFinalization();
                System.gc();
            }
            catch (Exception e)
            {
                e.printStackTrace();
                System.out.println("EXCEPTION: " + e.getMessage());
                //break;
            }
        }
        return params;
    }

    protected File getSourceDirectory(File workingDir)
    {
        return new File(workingDir, SOURCE_DIR);
    }

    /**
     * Run benchmark using supplied parameters.
     *
     * @param params benchmark parameters
     * @throws Exception
     */
    protected void runBenchmark(TestData params, StandardOptions options) throws Exception
    {
        System.out.println("Start Time: " + new Date());
        int runCount = options.getRunCount();
        for (int i = 0; i < runCount; i++)
        {
            TestRunData trd = new TestRunData();
            trd.startRun();
            trd.setId(String.valueOf(i));
            IndexWriter iw = new IndexWriter(params.getDirectory(), params.getAnalyzer(), true);
            iw.setMergeFactor(params.getMergeFactor());
            iw.setMaxBufferedDocs(params.getMaxBufferedDocs());

            iw.setUseCompoundFile(params.isCompound());
            makeIndex(trd, params.getSource(), iw, true, true, false, options);
            if (params.isOptimize())
            {
                TimeData td = new TimeData("optimize");
                trd.addData(td);
                td.start();
                iw.optimize();
                td.stop();
                trd.addData(td);
            }
            iw.close();
            QueryData[] queries = params.getQueries();
            if (queries != null)
            {
                IndexReader ir = null;
                IndexSearcher searcher = null;
                for (int k = 0; k < queries.length; k++)
                {
                    QueryData qd = queries[k];
                    if (ir != null && qd.reopen)
                    {
                        searcher.close();
                        ir.close();
                        ir = null;
                        searcher = null;
                    }
                    if (ir == null)
                    {
                        ir = IndexReader.open(params.getDirectory());
                        searcher = new IndexSearcher(ir);
                    }
                    Document doc = null;
                    if (qd.warmup)
                    {
                        TimeData td = new TimeData(qd.id + "-warm");
                        for (int m = 0; m < ir.maxDoc(); m++)
                        {
                            td.start();
                            if (ir.isDeleted(m))
                            {
                                td.stop();
                                continue;
                            }
                            doc = ir.document(m);
                            td.stop();
                        }
                        trd.addData(td);
                    }
                    TimeData td = new TimeData(qd.id + "-srch");
                    td.start();
                    Hits h = searcher.search(qd.q);
                    //System.out.println("Hits Size: " + h.length() + " Query: " + qd.q);
                    td.stop();
                    trd.addData(td);
                    td = new TimeData(qd.id + "-trav");
                    if (h != null && h.length() > 0)
                    {
                        for (int m = 0; m < h.length(); m++)
                        {
                            td.start();
                            int id = h.id(m);
                            if (qd.retrieve)
                            {
                                doc = ir.document(id);
                            }
                            td.stop();
                        }
                    }
                    trd.addData(td);
                }
                try
                {
                    if (searcher != null)
                    {
                        searcher.close();
                    }
                }
                catch (Exception e)
                {
                }
                ;
                try
                {
                    if (ir != null)
                    {
                        ir.close();
                    }
                }
                catch (Exception e)
                {
                }
                ;
            }
            trd.endRun();
            params.getRunData().add(trd);
            //System.out.println(params[i].showRunData(params[i].getId()));
            //params.showRunData(params.getId());
        }
        System.out.println("End Time: " + new Date());
    }

    /**
     * Parse the Reuters SGML and index:
     * Date, Title, Dateline, Body
     *
     *
     *
     * @param in        input file
     * @return Lucene document
     */
    protected Document makeDocument(File in, String[] tags, boolean stored, boolean tokenized, boolean tfv)
            throws Exception
    {
        Document doc = new Document();
        // tag this document
        if (tags != null)
        {
            for (int i = 0; i < tags.length; i++)
            {
                doc.add(new Field("tag" + i, tags[i], stored == true ? Field.Store.YES : Field.Store.NO,
                                  tokenized == true ? Field.Index.TOKENIZED : Field.Index.UN_TOKENIZED, tfv == true ? Field.TermVector.YES : Field.TermVector.NO));
            }
        }
        doc.add(new Field("file", in.getCanonicalPath(), stored == true ? Field.Store.YES : Field.Store.NO,
                          tokenized == true ? Field.Index.TOKENIZED : Field.Index.UN_TOKENIZED, tfv == true ? Field.TermVector.YES : Field.TermVector.NO));
        BufferedReader reader = new BufferedReader(new FileReader(in));
        String line = null;
        //First line is the date, 3rd is the title, rest is body
        String dateStr = reader.readLine();
        reader.readLine();//skip an empty line
        String title = reader.readLine();
        reader.readLine();//skip an empty line
        StringBuffer body = new StringBuffer(1024);
        while ((line = reader.readLine()) != null)
        {
            body.append(line).append(' ');
        }
        Date date = format.parse(dateStr.trim());

        doc.add(new Field("date", DateTools.dateToString(date, DateTools.Resolution.SECOND), Field.Store.YES, Field.Index.UN_TOKENIZED));

        if (title != null)
        {
            doc.add(new Field("title", title, stored == true ? Field.Store.YES : Field.Store.NO,
                              tokenized == true ? Field.Index.TOKENIZED : Field.Index.UN_TOKENIZED, tfv == true ? Field.TermVector.YES : Field.TermVector.NO));
        }
        if (body.length() > 0)
        {
            doc.add(new Field("body", body.toString(), stored == true ? Field.Store.YES : Field.Store.NO,
                              tokenized == true ? Field.Index.TOKENIZED : Field.Index.UN_TOKENIZED, tfv == true ? Field.TermVector.YES : Field.TermVector.NO));
        }

        return doc;
    }

    /**
     * Make index, and collect time data.
     *
     * @param trd       run data to populate
     * @param srcDir    directory with source files
     * @param iw        index writer, already open
     * @param stored    store values of fields
     * @param tokenized tokenize fields
     * @param tfv       store term vectors
     * @throws Exception
     */
    protected void makeIndex(TestRunData trd, File srcDir, IndexWriter iw, boolean stored, boolean tokenized,
                             boolean tfv, StandardOptions options) throws Exception
    {
        //File[] groups = srcDir.listFiles();
        List files = new ArrayList();
        getAllFiles(srcDir, null, files);
        Document doc = null;
        long cnt = 0L;
        TimeData td = new TimeData();
        td.name = "addDocument";
        int scaleUp = options.getScaleUp();
        int logStep = options.getLogStep();
        int max = Math.min(files.size(), options.getMaximumDocumentsToIndex());
        for (int s = 0; s < scaleUp; s++)
        {
            String[] tags = new String[]{srcDir.getName() + "/" + s};
            int i = 0;
            for (Iterator iterator = files.iterator(); iterator.hasNext() && i < max; i++)
            {
                File file = (File) iterator.next();
                doc = makeDocument(file, tags, stored, tokenized, tfv);
                td.start();
                iw.addDocument(doc);
                td.stop();
                cnt++;
                if (cnt % logStep == 0)
                {
                    System.err.println(" - processed " + cnt + ", run id=" + trd.getId());
                    trd.addData(td);
                    td.reset();
                }
            }
        }
        trd.addData(td);
    }

    public static void getAllFiles(File srcDir, FileFilter filter, List allFiles)
    {
        File [] files = srcDir.listFiles(filter);
        for (int i = 0; i < files.length; i++)
        {
            File file = files[i];
            if (file.isDirectory())
            {
                getAllFiles(file, filter, allFiles);
            }
            else
            {
                allFiles.add(file);
            }
        }
    }

    /**
     * Parse the strings containing Lucene queries.
     *
     * @param qs array of strings containing query expressions
     * @param a  analyzer to use when parsing queries
     * @return array of Lucene queries
     */
    public static Query[] createQueries(List qs, Analyzer a)
    {
        QueryParser qp = new QueryParser("body", a);
        List queries = new ArrayList();
        for (int i = 0; i < qs.size(); i++)
        {
            try
            {
                Object query = qs.get(i);
                Query q = null;
                if (query instanceof String)
                {
                    q = qp.parse((String) query);
                }
                else if (query instanceof Query)
                {
                    q = (Query) query;
                }
                else
                {
                    System.err.println("Unsupported Query Type: " + query);
                }
                if (q != null)
                {
                    queries.add(q);
                }

            }
            catch (Exception e)
            {
                e.printStackTrace();
            }
        }
        return (Query[]) queries.toArray(new Query[0]);
    }

    /**
     * Remove existing index.
     *
     * @throws Exception
     */
    protected void reset(File indexDir) throws Exception
    {
        if (indexDir.exists())
        {
            fullyDelete(indexDir);
        }
        indexDir.mkdirs();
    }

    /**
     * Save a stream to a file.
     *
     * @param is         input stream
     * @param out        output file
     * @param closeInput if true, close the input stream when done.
     * @throws Exception
     */
    protected void saveStream(InputStream is, File out, boolean closeInput) throws Exception
    {
        byte[] buf = new byte[4096];
        FileOutputStream fos = new FileOutputStream(out);
        int len = 0;
        long total = 0L;
        long time = System.currentTimeMillis();
        long delta = time;
        while ((len = is.read(buf)) > 0)
        {
            fos.write(buf, 0, len);
            total += len;
            time = System.currentTimeMillis();
            if (time - delta > 5000)
            {
                System.err.println(" - copied " + total / 1024 + " kB...");
                delta = time;
            }
        }
        fos.flush();
        fos.close();
        if (closeInput)
        {
            is.close();
        }
    }
}
