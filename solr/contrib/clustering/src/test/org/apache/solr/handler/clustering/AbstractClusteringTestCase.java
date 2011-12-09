package org.apache.solr.handler.clustering;
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

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrInputDocument;
import org.junit.BeforeClass;


/**
 *
 */
public abstract class AbstractClusteringTestCase extends SolrTestCaseJ4 {
  protected static int numberOfDocs = 0;

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig.xml", "schema.xml", "clustering/solr");
    numberOfDocs = 0;
    for (String[] doc : DOCUMENTS) {
      assertNull(h.validateUpdate(adoc("id", Integer.toString(numberOfDocs), "url", doc[0], "title", doc[1], "snippet", doc[2])));
      numberOfDocs++;
    }
    
    // Add a multi-valued snippet
    final SolrInputDocument multiValuedSnippet = new SolrInputDocument();
    multiValuedSnippet.addField("id", numberOfDocs++);
    multiValuedSnippet.addField("title", "Title");
    multiValuedSnippet.addField("url", "URL");
    multiValuedSnippet.addField("snippet", "First value of multi field. Some more text. And still more.");
    multiValuedSnippet.addField("snippet", "Second value of multi field. Some more text. And still more.");
    multiValuedSnippet.addField("snippet", "Third value of multi field. Some more text. And still more.");
    assertNull(h.validateUpdate(adoc(multiValuedSnippet)));

    assertNull(h.validateUpdate(commit()));
  }

  final static String[][] DOCUMENTS = new String[][]{
          {"http://en.wikipedia.org/wiki/Data_mining",
                  "Data Mining - Wikipedia",
                  "Article about knowledge-discovery in databases (KDD), the practice of automatically searching large stores of data for patterns."},


          {"http://en.wikipedia.org/wiki/Datamining",
                  "Data mining - Wikipedia, the free encyclopedia",
                  "Data mining is the entire process of applying computer-based methodology, ... Moreover, some data-mining systems such as neural networks are inherently geared ..."},


          {"http://www.statsoft.com/textbook/stdatmin.html",
                  "Electronic Statistics Textbook: Data Mining Techniques",
                  "Outlines the crucial concepts in data mining, defines the data warehousing process, and offers examples of computational and graphical exploratory data analysis techniques."},


          {"http://www.thearling.com/text/dmwhite/dmwhite.htm",
                  "An Introduction to Data Mining",
                  "Data mining, the extraction of hidden predictive information from large ... Data mining tools predict future trends and behaviors, allowing businesses to ..."},


          {"http://www.anderson.ucla.edu/faculty/jason.frand/teacher/technologies/palace/datamining.htm",
                  "Data Mining: What is Data Mining?",
                  "Outlines what knowledge discovery, the process of analyzing data from different perspectives and summarizing it into useful information, can do and how it works."},


          {"http://www.spss.com/datamine",
                  "Data Mining Software, Data Mining Applications and Data Mining Solutions",
                  "The patterns uncovered using data mining help organizations make better and ... data mining customer ... Data mining applications, on the other hand, embed ..."},


          {"http://www.kdnuggets.com/",
                  "KD Nuggets",
                  "Newsletter on the data mining and knowledge industries, offering information on data mining, knowledge discovery, text mining, and web mining software, courses, jobs, publications, and meetings."},


          {"http://www.answers.com/topic/data-mining",
                  "data mining: Definition from Answers.com",
                  "data mining n. The automatic extraction of useful, often previously unknown information from large databases or data ... Data Mining For Investing ..."},


          {"http://www.statsoft.com/products/dataminer.htm",
                  "STATISTICA Data Mining and Predictive Modeling Solutions",
                  "GRC site-wide menuing system research and development. ... Contact a Data Mining Solutions Consultant. News and Success Stories. Events ..."},


          {"http://datamining.typepad.com/",
                  "Data Mining: Text Mining, Visualization and Social Media",
                  "Commentary on text mining, data mining, social media and data visualization. ... While mining Twitter data for business and marketing intelligence (trend/buzz ..."},


          {"http://www.twocrows.com/",
                  "Two Crows Corporation",
                  "Dedicated to the development, marketing, sales and support of tools for knowledge discovery to make data mining accessible and easy to use."},


          {"http://www.thearling.com/",
                  "Thearling.com",
                  "Kurt Thearling's site dedicated to sharing information about data mining, the automated extraction of hidden predictive information from databases, and other analytic technologies."},


          {"http://www.ccsu.edu/datamining/",
                  "CCSU - Data Mining",
                  "Offers degrees and certificates in data mining. Allows students to explore cutting-edge data mining techniques and applications: market basket analysis, decision trees, neural networks, machine learning, web mining, and data modeling."},


          {"http://www.oracle.com/technology/products/bi/odm",
                  "Oracle Data Mining",
                  "Oracle Data Mining Product Center ... New Oracle Data Mining Powers New Social CRM Application (more information ... Mining High-Dimensional Data for ..."},


          {"http://databases.about.com/od/datamining/a/datamining.htm",
                  "Data Mining: An Introduction",
                  "About.com article on how businesses are discovering new trends and patterns of behavior that previously went unnoticed through data mining, automated statistical analysis techniques."},


          {"http://www.dmoz.org/Computers/Software/Databases/Data_Mining/",
                  "Open Directory - Computers: Software: Databases: Data Mining",
                  "Data Mining and Knowledge Discovery - A peer-reviewed journal publishing ... Data mining creates information assets that an organization can leverage to ..."},


          {"http://www.cs.wisc.edu/dmi/",
                  "DMI:Data Mining Institute",
                  "Data Mining Institute at UW-Madison ... The Data Mining Institute (DMI) was started on June 1, 1999 at the Computer ... of the Data Mining Group of Microsoft ..."},


          {"http://www.the-data-mine.com/",
                  "The Data Mine",
                  "Provides information about data mining also known as knowledge discovery in databases (KDD) or simply knowledge discovery. List software, events, organizations, and people working in data mining."},


          {"http://www.statserv.com/datamining.html",
                  "St@tServ - About Data Mining",
                  "St@tServ Data Mining page ... Data mining in molecular biology, by Alvis Brazma. Graham Williams page. Knowledge Discovery and Data Mining Resources, ..."},


          {"http://ocw.mit.edu/OcwWeb/Sloan-School-of-Management/15-062Data-MiningSpring2003/CourseHome/index.htm",
                  "MIT OpenCourseWare | Sloan School of Management | 15.062 Data Mining ...",
                  "Introduces students to a class of methods known as data mining that assists managers in recognizing patterns and making intelligent use of massive amounts of ..."},


          {"http://www.pentaho.com/products/data_mining/",
                  "Pentaho Commercial Open Source Business Intelligence: Data Mining",
                  "For example, data mining can warn you there's a high probability a specific ... Pentaho Data Mining is differentiated by its open, standards-compliant nature, ..."},


          {"http://www.investorhome.com/mining.htm",
                  "Investor Home - Data Mining",
                  "Data Mining or Data Snooping is the practice of searching for relationships and ... Data mining involves searching through databases for correlations and patterns ..."},


          {"http://www.datamining.com/",
                  "Predictive Modeling and Predictive Analytics Solutions | Enterprise ...",
                  "Insightful Enterprise Miner - Enterprise data mining for predictive modeling and predictive analytics."},


          {"http://www.sourcewatch.org/index.php?title=Data_mining",
                  "Data mining - SourceWatch",
                  "These agencies reported 199 data mining projects, of which 68 ... Office, \"DATA MINING. ... powerful technology known as data mining -- and how, in the ..."},


          {"http://www.autonlab.org/tutorials/",
                  "Statistical Data Mining Tutorials",
                  "Includes a set of tutorials on many aspects of statistical data mining, including the foundations of probability, the foundations of statistical data analysis, and most of the classic machine learning and data mining algorithms."},


          {"http://www.microstrategy.com/data-mining/index.asp",
                  "Data Mining",
                  "With MicroStrategy, data mining scoring is fully integrated into mainstream ... The integration of data mining models from other applications is accomplished by ..."},


          {"http://www.datamininglab.com/",
                  "Elder Research",
                  "Provides consulting and short courses in data mining and pattern discovery patterns in data."},


          {"http://www.sqlserverdatamining.com/",
                  "SQL Server Data Mining > Home",
                  "SQL Server Data Mining Portal ... Data Mining as an Application Platform (Whitepaper) Creating a Web Cross-sell Application with SQL Server 2005 Data Mining (Article) ..."},


          {"http://databases.about.com/cs/datamining/g/dmining.htm",
                  "Data Mining",
                  "What is data mining? Find out here! ... Book Review: Data Mining and Statistical Analysis Using SQL. What is Data Mining, and What Does it Have to Do with ..."},


          {"http://www.sas.com/technologies/analytics/datamining/index.html",
                  "Data Mining Software and Text Mining | SAS",
                  "... raw data to smarter ... Data Mining is an iterative process of creating ... The knowledge gleaned from data and text mining can be used to fuel ..."}
  };
}
