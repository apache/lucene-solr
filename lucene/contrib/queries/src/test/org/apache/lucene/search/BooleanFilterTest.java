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

import java.io.IOException;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexReader.AtomicReaderContext;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.SlowMultiReaderWrapper;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;

public class BooleanFilterTest extends LuceneTestCase {
	private Directory directory;
	private IndexReader reader;

	@Override
	public void setUp() throws Exception {
	  super.setUp();
		directory = newDirectory();
		RandomIndexWriter writer = new RandomIndexWriter(random, directory, new MockAnalyzer(random, MockTokenizer.WHITESPACE, false));
		
		//Add series of docs with filterable fields : acces rights, prices, dates and "in-stock" flags
		addDoc(writer, "admin guest", "010", "20040101","Y");
		addDoc(writer, "guest", "020", "20040101","Y");
		addDoc(writer, "guest", "020", "20050101","Y");
		addDoc(writer, "admin", "020", "20050101","Maybe");
		addDoc(writer, "admin guest", "030", "20050101","N");
		reader = new SlowMultiReaderWrapper(writer.getReader());
		writer.close();	
	}
	
	@Override
	public void tearDown() throws Exception {
	  reader.close();
	  directory.close();
	  super.tearDown();
	}
	
	private void addDoc(RandomIndexWriter writer, String accessRights, String price, String date, String inStock) throws IOException
	{
		Document doc=new Document();
		doc.add(newField("accessRights",accessRights,Field.Store.YES,Field.Index.ANALYZED));
		doc.add(newField("price",price,Field.Store.YES,Field.Index.ANALYZED));
		doc.add(newField("date",date,Field.Store.YES,Field.Index.ANALYZED));
		doc.add(newField("inStock",inStock,Field.Store.YES,Field.Index.ANALYZED));
		writer.addDocument(doc);
	}
	
  private Filter getRangeFilter(String field,String lowerPrice, String upperPrice)
	{
    Filter f = TermRangeFilter.newStringRange(field,lowerPrice,upperPrice,true,true);
    return f;
	}
  private Filter getTermsFilter(String field,String text)
	{
		TermsFilter tf=new TermsFilter();
		tf.addTerm(new Term(field,text));
    
		return tf;
	}
        
        private void tstFilterCard(String mes, int expected, Filter filt)
        throws Throwable
        {
          DocIdSetIterator disi = filt.getDocIdSet(new AtomicReaderContext(reader)).iterator();
          int actual = 0;
          while (disi.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
            actual++;
          }
          assertEquals(mes, expected, actual);
        }
          
		
	public void testShould() throws Throwable
	{
    BooleanFilter booleanFilter = new BooleanFilter();
    booleanFilter.add(new FilterClause(getTermsFilter("price","030"),BooleanClause.Occur.SHOULD));
    tstFilterCard("Should retrieves only 1 doc",1,booleanFilter);
	}
	
	public void testShoulds() throws Throwable
	{
    BooleanFilter booleanFilter = new BooleanFilter();
    booleanFilter.add(new FilterClause(getRangeFilter("price","010", "020"),BooleanClause.Occur.SHOULD));
    booleanFilter.add(new FilterClause(getRangeFilter("price","020", "030"),BooleanClause.Occur.SHOULD));
    tstFilterCard("Shoulds are Ored together",5,booleanFilter);
	}
	public void testShouldsAndMustNot() throws Throwable
	{
    BooleanFilter booleanFilter = new BooleanFilter();
    booleanFilter.add(new FilterClause(getRangeFilter("price","010", "020"),BooleanClause.Occur.SHOULD));
    booleanFilter.add(new FilterClause(getRangeFilter("price","020", "030"),BooleanClause.Occur.SHOULD));
    booleanFilter.add(new FilterClause(getTermsFilter("inStock", "N"),BooleanClause.Occur.MUST_NOT));
    tstFilterCard("Shoulds Ored but AndNot",4,booleanFilter);

    booleanFilter.add(new FilterClause(getTermsFilter("inStock", "Maybe"),BooleanClause.Occur.MUST_NOT));
    tstFilterCard("Shoulds Ored but AndNots",3,booleanFilter);
	}
	public void testShouldsAndMust() throws Throwable
	{
    BooleanFilter booleanFilter = new BooleanFilter();
    booleanFilter.add(new FilterClause(getRangeFilter("price","010", "020"),BooleanClause.Occur.SHOULD));
    booleanFilter.add(new FilterClause(getRangeFilter("price","020", "030"),BooleanClause.Occur.SHOULD));
    booleanFilter.add(new FilterClause(getTermsFilter("accessRights", "admin"),BooleanClause.Occur.MUST));
    tstFilterCard("Shoulds Ored but MUST",3,booleanFilter);
	}
	public void testShouldsAndMusts() throws Throwable
	{
    BooleanFilter booleanFilter = new BooleanFilter();
    booleanFilter.add(new FilterClause(getRangeFilter("price","010", "020"),BooleanClause.Occur.SHOULD));
    booleanFilter.add(new FilterClause(getRangeFilter("price","020", "030"),BooleanClause.Occur.SHOULD));
    booleanFilter.add(new FilterClause(getTermsFilter("accessRights", "admin"),BooleanClause.Occur.MUST));
    booleanFilter.add(new FilterClause(getRangeFilter("date","20040101", "20041231"),BooleanClause.Occur.MUST));
    tstFilterCard("Shoulds Ored but MUSTs ANDED",1,booleanFilter);
	}
	public void testShouldsAndMustsAndMustNot() throws Throwable
	{
    BooleanFilter booleanFilter = new BooleanFilter();
    booleanFilter.add(new FilterClause(getRangeFilter("price","030", "040"),BooleanClause.Occur.SHOULD));
    booleanFilter.add(new FilterClause(getTermsFilter("accessRights", "admin"),BooleanClause.Occur.MUST));
    booleanFilter.add(new FilterClause(getRangeFilter("date","20050101", "20051231"),BooleanClause.Occur.MUST));
    booleanFilter.add(new FilterClause(getTermsFilter("inStock","N"),BooleanClause.Occur.MUST_NOT));
    tstFilterCard("Shoulds Ored but MUSTs ANDED and MustNot",0,booleanFilter);
	}
	
	public void testJustMust() throws Throwable
	{
    BooleanFilter booleanFilter = new BooleanFilter();
    booleanFilter.add(new FilterClause(getTermsFilter("accessRights", "admin"),BooleanClause.Occur.MUST));
    tstFilterCard("MUST",3,booleanFilter);
	}
	public void testJustMustNot() throws Throwable
	{
    BooleanFilter booleanFilter = new BooleanFilter();
    booleanFilter.add(new FilterClause(getTermsFilter("inStock","N"),BooleanClause.Occur.MUST_NOT));
    tstFilterCard("MUST_NOT",4,booleanFilter);
	}
	public void testMustAndMustNot() throws Throwable
	{
    BooleanFilter booleanFilter = new BooleanFilter();
    booleanFilter.add(new FilterClause(getTermsFilter("inStock","N"),BooleanClause.Occur.MUST));
    booleanFilter.add(new FilterClause(getTermsFilter("price","030"),BooleanClause.Occur.MUST_NOT));
    tstFilterCard("MUST_NOT wins over MUST for same docs",0,booleanFilter);
	}
}
