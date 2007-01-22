package org.apache.solr.search;

import junit.framework.TestCase;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.Query;
import org.apache.lucene.index.Term;
import org.apache.solr.util.AbstractSolrTestCase;

/**
 * @author yonik
 * @version $Id$
 */
public class TestQueryUtils extends AbstractSolrTestCase {

  public String getSchemaFile() { return "schema.xml"; }
  public String getSolrConfigFile() { return "solrconfig.xml"; }

  public void setUp() throws Exception {
    super.setUp();
  }
  public void tearDown() throws Exception {
    super.tearDown();
  }

  public void positive(Query q) {
    assertFalse(QueryUtils.isNegative(q));
    assertTrue(QueryUtils.getAbs(q)==q);
    BooleanClause[] clauses = (q instanceof BooleanQuery) ? ((BooleanQuery)q).getClauses() : null;
    if (clauses != null) {
      if (clauses.length != 0) {
        assertTrue(QueryUtils.makeQueryable(q)==q);
      }
    } else {
      assertTrue(QueryUtils.makeQueryable(q)==q);
    }
  }

  public void negative(Query q) {
    assertTrue(QueryUtils.isNegative(q));
    Query abs = QueryUtils.getAbs(q);
    assertTrue(q != abs);
    Query neg2 = QueryUtils.fixNegativeQuery(q);

    assertFalse(abs.equals(q));
    assertFalse(neg2.equals(q));
  }

  public void testNegativeQueries() {
    TermQuery tq = new TermQuery(new Term("hi","there"));
    TermQuery tq2 = new TermQuery(new Term("wow","dude"));
    BooleanQuery bq = new BooleanQuery();

    positive(tq);
    // positive(bq);
    bq.add(tq, BooleanClause.Occur.SHOULD);
    positive(bq);
    bq.add(tq2, BooleanClause.Occur.MUST_NOT);
    positive(bq);

    bq = new BooleanQuery();
    bq.add(tq,BooleanClause.Occur.MUST_NOT);
    negative(bq);

    bq.add(tq2,BooleanClause.Occur.MUST_NOT);
    negative(bq);


    String f = "name";  // name is whitespace tokenized

    assertU(adoc("id", "1",  f, "A"));
    assertU(adoc("id", "2",  f, "B"));
    assertU(adoc("id", "3",  f, "C"));
    assertU(adoc("id", "4",  f, "C"));
    assertU(adoc("id", "5",  f, "D"));
    assertU(adoc("id", "6",  f, "E"));
    assertU(adoc("id", "7",  f, "E"));
    assertU(adoc("id", "8",  f, "E W"));
    assertU(adoc("id", "9",  f, "F W"));
    assertU(adoc("id", "10", f, "G W"));
    assertU(adoc("id", "11", f, "G X "));
    assertU(adoc("id", "12", f, "G X Y"));
    assertU(adoc("id", "13", f, "G X Y Z"));
    assertU(adoc("id", "14", f, "G Y Z"));
    assertU(adoc("id", "15", f, "G Z"));
    assertU(adoc("id", "16", f, "G"));
    assertU(commit());

    assertQ("test negative base q matching nothing",
            req("-qlkciyopsbgzyvkylsjhchghjrdf")
            ,"//result[@numFound='16']"
            );

    assertQ("test negative base q matching something",
            req("-name:E")
            ,"//result[@numFound='13']"
            );

    assertQ("test negative base q with two terms",
            req("-name:G -name:W")
            ,"//result[@numFound='7']"
            );

    assertQ("test negative base q with three terms",
            req("-name:G -name:W -name:E")
            ,"//result[@numFound='5']"
            );

    assertQ("test negative boolean query",
            req("-(name:G OR name:W)")
            ,"//result[@numFound='7']"
            );

    assertQ("test non negative q",
            req("-name:G -name:W -name:E id:[* TO *]")
            ,"//result[@numFound='5']"
            );

    assertQ("test non negative q",
            req("-name:G -name:W -name:E +id:[* TO *]")
            ,"//result[@numFound='5']"
            );

    // now for the filters...
    assertQ("test negative base q matching nothing, with filters",
            req("q","-qlkciyopsbgzyvkylsjhchghjrdf"
                ,"fq","name:A"
            )
            ,"//result[@numFound='1']"
            );

    assertQ("test negative filters",
            req("q","name:A"
                ,"fq","-name:A"
            )
            ,"//result[@numFound='0']"
            );
    assertQ("test negative filters",
            req("q","name:A"
                ,"fq","-name:A"
            )
            ,"//result[@numFound='0']"
            );
    assertQ("test negative filters",
            req("q","-name:E"
                ,"fq","name:E"
            )
            ,"//result[@numFound='0']"
            );
    assertQ("test negative filters",
            req("q","-name:E"
                ,"fq","name:W"
            )
            ,"//result[@numFound='2']"
            );
    assertQ("test negative filters",
            req("q","-name:E"
                ,"fq","name:W"
            )
            ,"//result[@numFound='2']"
            );
    assertQ("one pos filter, one neg",
            req("q","-name:E"
                ,"fq","name:W"
                ,"fq","-name:G"
            )
            ,"//result[@numFound='1']"
            );
        assertQ("two neg filters",
            req("q","-name:E"
                ,"fq","-name:W"
                ,"fq","-name:G"
            )
            ,"//result[@numFound='5']"  // ABCCD
            );

        assertQ("three neg filters",
            req("q","-name:E"
                ,"fq","-name:W"
                ,"fq","-name:G"
                ,"fq","-name:C"
            )
            ,"//result[@numFound='3']"  // ABD
            );

        assertQ("compound neg filters",
            req("q","-name:E"
                ,"fq","-name:W -name:G"
                ,"fq","-name:C"
            )
            ,"//result[@numFound='3']"  // ABD
            );

         assertQ("compound neg filters",
            req("q","-name:E"
                ,"fq","-name:W -name:G -name:C"
            )
            ,"//result[@numFound='3']"  // ABD
            );

        assertQ("compound neg filters",
            req("q","-name:E"
                ,"fq","-(name:W name:G name:C)"
            )
            ,"//result[@numFound='3']"  // ABD
            );

        assertQ("three neg filters + pos",
            req("q","-name:E"
                ,"fq","-name:W"
                ,"fq","-name:G"
                ,"fq","-name:C"
                ,"fq","name:G"
            )
            ,"//result[@numFound='0']"
            );
        assertQ("three neg filters + pos",
            req("q","-name:E"
                ,"fq","-name:W"
                ,"fq","-name:G"
                ,"fq","-name:C"
                ,"fq","+id:1"
            )
            ,"//result[@numFound='1']"  // A
            );
         assertQ("three neg filters + pos",
            req("q","-name:E"
                ,"fq","-name:W"
                ,"fq","-name:G"
                ,"fq","-name:C"
                ,"fq","id:[* TO *]"
            )
            ,"//result[@numFound='3']"  // ABD
            );

         // QueryParser turns term queries on stopwords into a BooleanQuery with
         // zero clauses.
         assertQ("neg base query on stopword",
            req("q","-text:stopworda")
            ,"//result[@numFound='16']"  // ABD
            );

         assertQ("negative filter on stopword",
            req("q","id:[* TO *]"
                ,"fq","-text:stopworda"
            )
            ,"//result[@numFound='16']"  // ABD
            );
         assertQ("two negative filters on stopword",
            req("q","id:[* TO *]"
                ,"fq","-text:stopworda"
                ,"fq","-text:stopworda"
            )
            ,"//result[@numFound='16']"  // ABD
            );
         assertQ("compound negative filters with stopword",
            req("q","id:[* TO *]"
                ,"fq","-text:stopworda -id:1"
            )
            ,"//result[@numFound='15']"  // ABD
            );
  }


}
