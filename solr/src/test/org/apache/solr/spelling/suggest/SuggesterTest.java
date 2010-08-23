package org.apache.solr.spelling.suggest;

import java.io.StringWriter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.lucene.util.RamUsageEstimator;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SpellingParams;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.response.QueryResponseWriter;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.spelling.suggest.Lookup.LookupResult;
import org.apache.solr.spelling.suggest.jaspell.JaspellLookup;
import org.apache.solr.spelling.suggest.tst.TSTLookup;
import org.apache.solr.util.AbstractSolrTestCase;
import org.apache.solr.util.RefCounted;
import org.apache.solr.util.TermFreqIterator;
import org.apache.solr.util.TestHarness;

public class SuggesterTest extends AbstractSolrTestCase {
  SolrRequestHandler handler;

  @Override
  public String getSchemaFile() {
    return "schema-spellchecker.xml";
  }

  @Override
  public String getSolrConfigFile() {
    return "solrconfig-spellchecker.xml";
  }
  
  @Override
  public void setUp() throws Exception {
    super.setUp();
    // empty
    h.validateUpdate("<delete><query>*:*</query></delete>");
    // populate
    h.validateAddDoc(
            "id", "1",
            "text", "acceptable accidentally accommodate acquire"
            );
    h.validateAddDoc(
            "id", "2",
            "text", "believe bellwether accommodate acquire"
            );
    h.validateAddDoc(
            "id", "3",
            "text", "cemetery changeable conscientious consensus acquire bellwether"
            );
    h.validateUpdate("<commit/>");
    handler = h.getCore().getRequestHandler("/suggest");
    // build
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set(SpellingParams.SPELLCHECK_BUILD, true);
    LocalSolrQueryRequest req = new LocalSolrQueryRequest(h.getCore(), params);
    SolrQueryResponse rsp = new SolrQueryResponse();
    handler.handleRequest(req, rsp);
  }
  
  private String assertXPath(SolrCore core, SolrQueryRequest req, SolrQueryResponse rsp, String... tests) throws Exception {
    StringWriter sw = new StringWriter(32000);
    QueryResponseWriter responseWriter = core.getQueryResponseWriter(req);
    responseWriter.write(sw,req,rsp);
    req.close();
    System.out.println(sw.toString());
    return h.validateXPath(sw.toString(), tests);
  }

  public void testSuggestions() throws Exception {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set(CommonParams.Q, "ac");
    params.set(SpellingParams.SPELLCHECK_COUNT, 2);
    params.set(SpellingParams.SPELLCHECK_ONLY_MORE_POPULAR, true);
    LocalSolrQueryRequest req = new LocalSolrQueryRequest(h.getCore(), params);
    SolrQueryResponse rsp = new SolrQueryResponse();
    handler.handleRequest(req, rsp);
    String res = assertXPath(h.getCore(), req, rsp, 
            "//lst[@name='spellcheck']/lst[@name='suggestions']/lst[@name='ac']/int[@name='numFound'][.='2']",
            "//lst[@name='spellcheck']/lst[@name='suggestions']/lst[@name='ac']/arr[@name='suggestion']/str[1][.='acquire']",
            "//lst[@name='spellcheck']/lst[@name='suggestions']/lst[@name='ac']/arr[@name='suggestion']/str[2][.='accommodate']"
            );
    assertNull(res, res);
  }
  
  public void testReload() throws Exception {
    String coreName = h.getCore().getName();
    RefCounted<SolrIndexSearcher> searcher = h.getCore().getSearcher();
    SolrIndexSearcher indexSearcher = searcher.get();
    log.info("Core " + coreName + ", NumDocs before reload: " + indexSearcher.getIndexReader().numDocs());
    log.info("Directory: " + indexSearcher.getIndexDir());
    searcher.decref();
    h.close();
    solrConfig = TestHarness.createConfig(getSolrConfigFile());
    h = new TestHarness( dataDir.getAbsolutePath(),
            solrConfig,
            getSchemaFile());
    searcher = h.getCore().getSearcher();
    indexSearcher = searcher.get();
    log.info("Core " + coreName + ", NumDocs now: " + indexSearcher.getIndexReader().numDocs());
    log.info("Directory: " + indexSearcher.getIndexDir());
    searcher.decref();
    // rebuilds on commit
    h.validateUpdate("<commit/>");
    handler = h.getCore().getRequestHandler("/suggest");
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set(CommonParams.Q, "ac");
    params.set(SpellingParams.SPELLCHECK_COUNT, 2);
    params.set(SpellingParams.SPELLCHECK_ONLY_MORE_POPULAR, true);
    LocalSolrQueryRequest req = new LocalSolrQueryRequest(h.getCore(), params);
    SolrQueryResponse rsp = new SolrQueryResponse();
    handler.handleRequest(req, rsp);
    String res = assertXPath(h.getCore(), req, rsp, 
            "//lst[@name='spellcheck']/lst[@name='suggestions']/lst[@name='ac']/int[@name='numFound'][.='2']",
            "//lst[@name='spellcheck']/lst[@name='suggestions']/lst[@name='ac']/arr[@name='suggestion']/str[1][.='acquire']",
            "//lst[@name='spellcheck']/lst[@name='suggestions']/lst[@name='ac']/arr[@name='suggestion']/str[2][.='accommodate']"
            );
    assertNull(res, res);
  }
  
  private TermFreqIterator getTFIT() {
    final int count = 100000;
    TermFreqIterator tfit = new TermFreqIterator() {
      Random r = new Random(1234567890L);
      Random r1 = new Random(1234567890L);
      int pos;

      @Override
      public float freq() {
        return r1.nextInt(4);
      }

      @Override
      public boolean hasNext() {
        return pos < count;
      }

      @Override
      public String next() {
        pos++;
        return Long.toString(r.nextLong());
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException();
      }
      
    };
    return tfit;
  }
  
  private void _benchmark(Lookup lookup, Map<String,Integer> ref, boolean estimate, Bench bench) throws Exception {
    long start = System.currentTimeMillis();
    lookup.build(getTFIT());
    long buildTime = System.currentTimeMillis() - start;
    TermFreqIterator tfit = getTFIT();
    long elapsed = 0;
    while (tfit.hasNext()) {
      String key = tfit.next();
      // take only the first part of the key
      int len = key.length() > 4 ? key.length() / 3 : 2;
      String prefix = key.substring(0, len);
      start = System.nanoTime();
      List<LookupResult> res = lookup.lookup(prefix, true, 10);
      elapsed += System.nanoTime() - start;
      assertTrue(res.size() > 0);
      for (LookupResult lr : res) {
        assertTrue(lr.key.startsWith(prefix));
      }
      if (ref != null) { // verify the counts
        Integer Cnt = ref.get(key);
        if (Cnt == null) { // first pass
          ref.put(key, res.size());
        } else {
          assertEquals(key + ", prefix: " + prefix, Cnt.intValue(), res.size());
        }
      }
    }
    if (estimate) {
      RamUsageEstimator rue = new RamUsageEstimator();
      long size = rue.estimateRamUsage(lookup);
      System.err.println(lookup.getClass().getSimpleName() + " - size=" + size);
    }
    if (bench != null) {
      bench.buildTime += buildTime;
      bench.lookupTime +=  elapsed;
    }
  }
  
  class Bench {
    long buildTime;
    long lookupTime;
  }
  
  public void testBenchmark() throws Exception {
    // this benchmark is very time consuming
    boolean doTest = false;
    if (!doTest) {
      return;
    }
    Map<String,Integer> ref = new HashMap<String,Integer>();
    JaspellLookup jaspell = new JaspellLookup();
    TSTLookup tst = new TSTLookup();
    
    _benchmark(tst, ref, true, null);
    _benchmark(jaspell, ref, true, null);
    jaspell = null;
    tst = null;
    int count = 100;
    Bench b = runBenchmark(JaspellLookup.class, count);
    System.err.println(JaspellLookup.class.getSimpleName() + ": buildTime[ms]=" + (b.buildTime / count) +
            " lookupTime[ms]=" + (b.lookupTime / count / 1000000));
    b = runBenchmark(TSTLookup.class, count);
    System.err.println(TSTLookup.class.getSimpleName() + ": buildTime[ms]=" + (b.buildTime / count) +
            " lookupTime[ms]=" + (b.lookupTime / count / 1000000));
  }
  
  private Bench runBenchmark(Class<? extends Lookup> cls, int count) throws Exception {
    System.err.println("* Running " + count + " iterations for " + cls.getSimpleName() + " ...");
    System.err.println("  - warm-up 10 iterations...");
    for (int i = 0; i < 10; i++) {
      System.runFinalization();
      System.gc();
      Lookup lookup = cls.newInstance();
      _benchmark(lookup, null, false, null);
      lookup = null;
    }
    Bench b = new Bench();
    System.err.print("  - main iterations:"); System.err.flush();
    for (int i = 0; i < count; i++) {
      System.runFinalization();
      System.gc();
      Lookup lookup = cls.newInstance();
      _benchmark(lookup, null, false, b);
      lookup = null;
      if (i > 0 && (i % 10 == 0)) {
        System.err.print(" " + i);
        System.err.flush();
      }
    }
    System.err.println();
    return b;
  }
}
