package org.apache.lucene.benchmark.stats;
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
import java.text.NumberFormat;
import java.util.ArrayList;

import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Vector;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.benchmark.Constants;
import org.apache.lucene.store.Directory;


/**
 * This class holds together all parameters related to a test. Single test is
 * performed several times, and all results are averaged.
 *
 */
public class TestData
{
    public static int[] MAX_BUFFERED_DOCS_COUNTS = new int[]{10, 20, 50, 100, 200, 500};
    public static int[] MERGEFACTOR_COUNTS = new int[]{10, 20, 50, 100, 200, 500};

    /**
     * ID of this test data.
     */
    private String id;
    /**
     * Heap size.
     */
    private long heap;
    /**
     * List of results for each test run with these parameters.
     */
    private Vector<TestRunData> runData = new Vector<TestRunData>();
    private int maxBufferedDocs, mergeFactor;
    /**
     * Directory containing source files.
     */
    private File source;
    /**
     * Lucene Directory implementation for creating an index.
     */
    private Directory directory;
    /**
     * Analyzer to use when adding documents.
     */
    private Analyzer analyzer;
    /**
     * If true, use compound file format.
     */
    private boolean compound;
    /**
     * If true, optimize index when finished adding documents.
     */
    private boolean optimize;
    /**
     * Data for search benchmarks.
     */
    private QueryData[] queries;

    public TestData()
    {
        heap = Runtime.getRuntime().maxMemory();
    }

    private static class DCounter
    {
        double total;
        int count, recordCount;
    }

    private static class LCounter
    {
        long total;
        int count;
    }

    private static class LDCounter
    {
      double Dtotal;
      int Dcount, DrecordCount;
      long Ltotal0;
      int Lcount0;
      long Ltotal1;
      int Lcount1;
    }

    /**
     * Get a textual summary of the benchmark results, average from all test runs.
     */
    static final String ID =      "# testData id     ";
    static final String OP =      "operation      ";
    static final String RUNCNT =  "     runCnt";
    static final String RECCNT =  "     recCnt";
    static final String RECSEC =  "          rec/s";
    static final String FREEMEM = "       avgFreeMem";
    static final String TOTMEM =  "      avgTotalMem";
    static final String COLS[] = {
        ID,
        OP,
        RUNCNT,
        RECCNT,
        RECSEC,
        FREEMEM,
        TOTMEM
    };
    public String showRunData(String prefix)
    {
        if (runData.size() == 0)
        {
            return "# [NO RUN DATA]";
        }
        HashMap<String,LDCounter> resByTask = new HashMap<String,LDCounter>(); 
        StringBuilder sb = new StringBuilder();
        String lineSep = System.getProperty("line.separator");
        sb.append("warm = Warm Index Reader").append(lineSep).append("srch = Search Index").append(lineSep).append("trav = Traverse Hits list, optionally retrieving document").append(lineSep).append(lineSep);
        for (int i = 0; i < COLS.length; i++) {
          sb.append(COLS[i]);
        }
        sb.append("\n");
        LinkedHashMap<String,TestData.LCounter[]> mapMem = new LinkedHashMap<String,TestData.LCounter[]>();
        LinkedHashMap<String,DCounter> mapSpeed = new LinkedHashMap<String,DCounter>();
        for (int i = 0; i < runData.size(); i++)
        {
            TestRunData trd = runData.get(i);
            for (final String label : trd.getLabels()) 
            {
                MemUsage mem = trd.getMemUsage(label);
                if (mem != null)
                {
                    TestData.LCounter[] tm = mapMem.get(label);
                    if (tm == null)
                    {
                        tm = new TestData.LCounter[2];
                        tm[0] = new TestData.LCounter();
                        tm[1] = new TestData.LCounter();
                        mapMem.put(label, tm);
                    }
                    tm[0].total += mem.avgFree;
                    tm[0].count++;
                    tm[1].total += mem.avgTotal;
                    tm[1].count++;
                }
                TimeData td = trd.getTotals(label);
                if (td != null)
                {
                    TestData.DCounter dc = mapSpeed.get(label);
                    if (dc == null)
                    {
                        dc = new TestData.DCounter();
                        mapSpeed.put(label, dc);
                    }
                    dc.count++;
                    //dc.total += td.getRate();
                    dc.total += (td.count>0 && td.elapsed<=0 ? 1 : td.elapsed); // assume at least 1ms for any countable op
                    dc.recordCount += td.count;
                }
            }
        }
        LinkedHashMap<String,String> res = new LinkedHashMap<String,String>();
        Iterator<String> it = mapSpeed.keySet().iterator();
        while (it.hasNext())
        {
            String label = it.next();
            TestData.DCounter dc = mapSpeed.get(label);
            res.put(label, 
                format(dc.count, RUNCNT) + 
                format(dc.recordCount / dc.count, RECCNT) +
                format(1,(float) (dc.recordCount * 1000.0 / (dc.total>0 ? dc.total : 1.0)), RECSEC)
                //format((float) (dc.total / (double) dc.count), RECSEC)
                );
            
            // also sum by task
            String task = label.substring(label.lastIndexOf("-")+1);
            LDCounter ldc = resByTask.get(task);
            if (ldc==null) {
              ldc = new LDCounter();
              resByTask.put(task,ldc);
            }
            ldc.Dcount += dc.count;
            ldc.DrecordCount += dc.recordCount;
            ldc.Dtotal += (dc.count>0 && dc.total<=0 ? 1 : dc.total); // assume at least 1ms for any countable op 
        }
        it = mapMem.keySet().iterator();
        while (it.hasNext())
        {
            String label = it.next();
            TestData.LCounter[] lc =  mapMem.get(label);
            String speed = res.get(label);
            boolean makeSpeed = false;
            if (speed == null)
            {
                makeSpeed = true;
                speed =  
                  format(lc[0].count, RUNCNT) + 
                  format(0, RECCNT) + 
                  format(0,(float)0.0, RECSEC);
            }
            res.put(label, speed + 
                format(0, lc[0].total / lc[0].count, FREEMEM) + 
                format(0, lc[1].total / lc[1].count, TOTMEM));
            
            // also sum by task
            String task = label.substring(label.lastIndexOf("-")+1);
            LDCounter ldc = resByTask.get(task);
            if (ldc==null) {
              ldc = new LDCounter();
              resByTask.put(task,ldc);
              makeSpeed = true;
            }
            if (makeSpeed) {
              ldc.Dcount += lc[0].count;
            }
            ldc.Lcount0 += lc[0].count;
            ldc.Lcount1 += lc[1].count;
            ldc.Ltotal0 += lc[0].total;
            ldc.Ltotal1 += lc[1].total;
        }
        it = res.keySet().iterator();
        while (it.hasNext())
        {
            String label = it.next();
            sb.append(format(prefix, ID));
            sb.append(format(label, OP));
            sb.append(res.get(label)).append("\n");
        }
        // show results by task (srch, optimize, etc.) 
        sb.append("\n");
        for (int i = 0; i < COLS.length; i++) {
          sb.append(COLS[i]);
        }
        sb.append("\n");
        it = resByTask.keySet().iterator();
        while (it.hasNext())
        {
            String task = it.next();
            LDCounter ldc = resByTask.get(task);
            sb.append(format("    ", ID));
            sb.append(format(task, OP));
            sb.append(format(ldc.Dcount, RUNCNT)); 
            sb.append(format(ldc.DrecordCount / ldc.Dcount, RECCNT));
            sb.append(format(1,(float) (ldc.DrecordCount * 1000.0 / (ldc.Dtotal>0 ? ldc.Dtotal : 1.0)), RECSEC));
            sb.append(format(0, ldc.Ltotal0 / ldc.Lcount0, FREEMEM)); 
            sb.append(format(0, ldc.Ltotal1 / ldc.Lcount1, TOTMEM));
            sb.append("\n");
        }
        return sb.toString();
    }

    private static NumberFormat numFormat [] = { NumberFormat.getInstance(), NumberFormat.getInstance()};
    private static final String padd = "                                  ";
    static {
      numFormat[0].setMaximumFractionDigits(0);
      numFormat[0].setMinimumFractionDigits(0);
      numFormat[1].setMaximumFractionDigits(1);
      numFormat[1].setMinimumFractionDigits(1);
    }

    // pad number from left
    // numFracDigits must be 0 or 1.
    static String format(int numFracDigits, float f, String col) {
      String res = padd + numFormat[numFracDigits].format(f);
      return res.substring(res.length() - col.length());
    }

    // pad number from left
    static String format(int n, String col) {
      String res = padd + n;
      return res.substring(res.length() - col.length());
    }

    // pad string from right
    static String format(String s, String col) {
      return (s + padd).substring(0,col.length());
    }

    /**
     * Prepare a list of benchmark data, using all possible combinations of
     * benchmark parameters.
     *
     * @param sources   list of directories containing different source document
     *                  collections
     * @param analyzers of analyzers to use.
     */
    public static TestData[] getAll(File[] sources, Analyzer[] analyzers)
    {
        List<TestData> res = new ArrayList<TestData>(50);
        TestData ref = new TestData();
        for (int q = 0; q < analyzers.length; q++)
        {
            for (int m = 0; m < sources.length; m++)
            {
                for (int i = 0; i < MAX_BUFFERED_DOCS_COUNTS.length; i++)
                {
                    for (int k = 0; k < MERGEFACTOR_COUNTS.length; k++)
                    {
                        for (int n = 0; n < Constants.BOOLEANS.length; n++)
                        {
                            for (int p = 0; p < Constants.BOOLEANS.length; p++)
                            {
                                ref.id = "td-" + q + m + i + k + n + p;
                                ref.source = sources[m];
                                ref.analyzer = analyzers[q];
                                ref.maxBufferedDocs = MAX_BUFFERED_DOCS_COUNTS[i];
                                ref.mergeFactor = MERGEFACTOR_COUNTS[k];
                                ref.compound = Constants.BOOLEANS[n].booleanValue();
                                ref.optimize = Constants.BOOLEANS[p].booleanValue();
                                try
                                {
                                    res.add((TestData)ref.clone());
                                }
                                catch (Exception e)
                                {
                                    e.printStackTrace();
                                }
                            }
                        }
                    }
                }
            }
        }
        return res.toArray(new TestData[0]);
    }

    /**
     * Similar to {@link #getAll(java.io.File[], org.apache.lucene.analysis.Analyzer[])} but only uses
     * maxBufferedDocs of 10 and 100 and same for mergeFactor, thus reducing the number of permutations significantly.
     * It also only uses compound file and optimize is always true.
     *
     * @param sources
     * @param analyzers
     * @return An Array of {@link TestData}
     */
    public static TestData[] getTestDataMinMaxMergeAndMaxBuffered(File[] sources, Analyzer[] analyzers)
    {
        List<TestData> res = new ArrayList<TestData>(50);
        TestData ref = new TestData();
        for (int q = 0; q < analyzers.length; q++)
        {
            for (int m = 0; m < sources.length; m++)
            {
                ref.id = "td-" + q + m + "_" + 10 + "_" + 10;
                ref.source = sources[m];
                ref.analyzer = analyzers[q];
                ref.maxBufferedDocs = 10;
                ref.mergeFactor = 10;//MERGEFACTOR_COUNTS[k];
                ref.compound = true;
                ref.optimize = true;
                try
                {
                    res.add((TestData)ref.clone());
                }
                catch (Exception e)
                {
                    e.printStackTrace();
                }
                ref.id = "td-" + q + m  + "_" + 10 + "_" + 100;
                ref.source = sources[m];
                ref.analyzer = analyzers[q];
                ref.maxBufferedDocs = 10;
                ref.mergeFactor = 100;//MERGEFACTOR_COUNTS[k];
                ref.compound = true;
                ref.optimize = true;
                try
                {
                    res.add((TestData)ref.clone());
                }
                catch (Exception e)
                {
                    e.printStackTrace();
                }
                ref.id = "td-" + q + m + "_" + 100 + "_" + 10;
                ref.source = sources[m];
                ref.analyzer = analyzers[q];
                ref.maxBufferedDocs = 100;
                ref.mergeFactor = 10;//MERGEFACTOR_COUNTS[k];
                ref.compound = true;
                ref.optimize = true;
                try
                {
                    res.add((TestData)ref.clone());
                }
                catch (Exception e)
                {
                    e.printStackTrace();
                }
                ref.id = "td-" + q + m + "_" + 100 + "_" + 100;
                ref.source = sources[m];
                ref.analyzer = analyzers[q];
                ref.maxBufferedDocs = 100;
                ref.mergeFactor = 100;//MERGEFACTOR_COUNTS[k];
                ref.compound = true;
                ref.optimize = true;
                try
                {
                    res.add((TestData)ref.clone());
                }
                catch (Exception e)
                {
                    e.printStackTrace();
                }
            }
        }
        return res.toArray(new TestData[0]);
    }

    @Override
    protected Object clone()
    {
        TestData cl = new TestData();
        cl.id = id;
        cl.compound = compound;
        cl.heap = heap;
        cl.mergeFactor = mergeFactor;
        cl.maxBufferedDocs = maxBufferedDocs;
        cl.optimize = optimize;
        cl.source = source;
        cl.directory = directory;
        cl.analyzer = analyzer;
        // don't clone runData
        return cl;
    }

    @Override
    public String toString()
    {
        StringBuilder res = new StringBuilder();
        res.append("#-- ID: ").append(id).append(", ").append(new Date()).append(", heap=").append(heap).append(" --\n");
        res.append("# source=").append(source).append(", directory=").append(directory).append("\n");
        res.append("# maxBufferedDocs=").append(maxBufferedDocs).append(", mergeFactor=").append(mergeFactor);
        res.append(", compound=").append(compound).append(", optimize=").append(optimize).append("\n");
        if (queries != null)
        {
            res.append(QueryData.getLabels()).append("\n");
            for (int i = 0; i < queries.length; i++)
            {
                res.append("# ").append(queries[i].toString()).append("\n");
            }
        }
        return res.toString();
    }

    public Analyzer getAnalyzer()
    {
        return analyzer;
    }

    public void setAnalyzer(Analyzer analyzer)
    {
        this.analyzer = analyzer;
    }

    public boolean isCompound()
    {
        return compound;
    }

    public void setCompound(boolean compound)
    {
        this.compound = compound;
    }

    public Directory getDirectory()
    {
        return directory;
    }

    public void setDirectory(Directory directory)
    {
        this.directory = directory;
    }

    public long getHeap()
    {
        return heap;
    }

    public void setHeap(long heap)
    {
        this.heap = heap;
    }

    public String getId()
    {
        return id;
    }

    public void setId(String id)
    {
        this.id = id;
    }

    public int getMaxBufferedDocs()
    {
        return maxBufferedDocs;
    }

    public void setMaxBufferedDocs(int maxBufferedDocs)
    {
        this.maxBufferedDocs = maxBufferedDocs;
    }

    public int getMergeFactor()
    {
        return mergeFactor;
    }

    public void setMergeFactor(int mergeFactor)
    {
        this.mergeFactor = mergeFactor;
    }

    public boolean isOptimize()
    {
        return optimize;
    }

    public void setOptimize(boolean optimize)
    {
        this.optimize = optimize;
    }

    public QueryData[] getQueries()
    {
        return queries;
    }

    public void setQueries(QueryData[] queries)
    {
        this.queries = queries;
    }

    public Vector<TestRunData> getRunData()
    {
        return runData;
    }

    public void setRunData(Vector<TestRunData> runData)
    {
        this.runData = runData;
    }

    public File getSource()
    {
        return source;
    }

    public void setSource(File source)
    {
        this.source = source;
    }
}
