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
import java.util.BitSet;

import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanFilter;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.FilterClause;
import org.apache.lucene.search.RangeFilter;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.DocIdBitSet;

import junit.framework.TestCase;

public class BooleanFilterTest extends TestCase
{
	private RAMDirectory directory;
	private IndexReader reader;

	protected void setUp() throws Exception
	{
		directory = new RAMDirectory();
		IndexWriter writer = new IndexWriter(directory, new WhitespaceAnalyzer(), true);
		
		//Add series of docs with filterable fields : acces rights, prices, dates and "in-stock" flags
		addDoc(writer, "admin guest", "010", "20040101","Y");
		addDoc(writer, "guest", "020", "20040101","Y");
		addDoc(writer, "guest", "020", "20050101","Y");
		addDoc(writer, "admin", "020", "20050101","Maybe");
		addDoc(writer, "admin guest", "030", "20050101","N");
		
		writer.close();
		reader=IndexReader.open(directory);			
	}
	
	private void addDoc(IndexWriter writer, String accessRights, String price, String date, String inStock) throws IOException
	{
		Document doc=new Document();
		doc.add(new Field("accessRights",accessRights,Field.Store.YES,Field.Index.ANALYZED));
		doc.add(new Field("price",price,Field.Store.YES,Field.Index.ANALYZED));
		doc.add(new Field("date",date,Field.Store.YES,Field.Index.ANALYZED));
		doc.add(new Field("inStock",inStock,Field.Store.YES,Field.Index.ANALYZED));
		writer.addDocument(doc);
	}
	
  private Filter getOldBitSetFilter(final Filter filter) {
    
    return new Filter() {
      public BitSet bits(IndexReader reader) throws IOException {
        BitSet bits = new BitSet(reader.maxDoc());
        DocIdSetIterator it = filter.getDocIdSet(reader).iterator();
        while(it.next()) {
          bits.set(it.doc());
        }
        return bits;
      }
    };
  }

	
  private Filter getRangeFilter(String field,String lowerPrice, String upperPrice, boolean old)
	{
    Filter f = new RangeFilter(field,lowerPrice,upperPrice,true,true);
    if (old) {
      return getOldBitSetFilter(f);
    }
    
    return f;
	}
  private Filter getTermsFilter(String field,String text, boolean old)
	{
		TermsFilter tf=new TermsFilter();
		tf.addTerm(new Term(field,text));
    if (old) {
      return getOldBitSetFilter(tf);
    }
    
		return tf;
	}
        
        private void tstFilterCard(String mes, int expected, Filter filt)
        throws Throwable
        {
          DocIdSetIterator disi = filt.getDocIdSet(reader).iterator();
          int actual = 0;
          while (disi.next()) {
            actual++;
          }
          assertEquals(mes, expected, actual);
        }
          
		
	public void testShould() throws Throwable
	{
    for (int i = 0; i < 2; i++) {
      boolean old = (i==0);
      BooleanFilter booleanFilter = new BooleanFilter();
      booleanFilter.add(new FilterClause(getTermsFilter("price","030", old),BooleanClause.Occur.SHOULD));
      tstFilterCard("Should retrieves only 1 doc",1,booleanFilter);
    }
	}
	
	public void testShoulds() throws Throwable
	{
    for (int i = 0; i < 2; i++) {
      boolean old = (i==0);
      BooleanFilter booleanFilter = new BooleanFilter();
      booleanFilter.add(new FilterClause(getRangeFilter("price","010", "020", old),BooleanClause.Occur.SHOULD));
      booleanFilter.add(new FilterClause(getRangeFilter("price","020", "030", old),BooleanClause.Occur.SHOULD));
      tstFilterCard("Shoulds are Ored together",5,booleanFilter);
    }
	}
	public void testShouldsAndMustNot() throws Throwable
	{
    for (int i = 0; i < 2; i++) {
      boolean old = (i==0);

      BooleanFilter booleanFilter = new BooleanFilter();
      booleanFilter.add(new FilterClause(getRangeFilter("price","010", "020", old),BooleanClause.Occur.SHOULD));
      booleanFilter.add(new FilterClause(getRangeFilter("price","020", "030", old),BooleanClause.Occur.SHOULD));
      booleanFilter.add(new FilterClause(getTermsFilter("inStock", "N", old),BooleanClause.Occur.MUST_NOT));
      tstFilterCard("Shoulds Ored but AndNot",4,booleanFilter);
  
      booleanFilter.add(new FilterClause(getTermsFilter("inStock", "Maybe", old),BooleanClause.Occur.MUST_NOT));
      tstFilterCard("Shoulds Ored but AndNots",3,booleanFilter);
    }
		
	}
	public void testShouldsAndMust() throws Throwable
	{
    for (int i = 0; i < 2; i++) {
      boolean old = (i==0);
      BooleanFilter booleanFilter = new BooleanFilter();
      booleanFilter.add(new FilterClause(getRangeFilter("price","010", "020", old),BooleanClause.Occur.SHOULD));
      booleanFilter.add(new FilterClause(getRangeFilter("price","020", "030", old),BooleanClause.Occur.SHOULD));
      booleanFilter.add(new FilterClause(getTermsFilter("accessRights", "admin", old),BooleanClause.Occur.MUST));
      tstFilterCard("Shoulds Ored but MUST",3,booleanFilter);
    }
	}
	public void testShouldsAndMusts() throws Throwable
	{
    for (int i = 0; i < 2; i++) {
      boolean old = (i==0);

      BooleanFilter booleanFilter = new BooleanFilter();
      booleanFilter.add(new FilterClause(getRangeFilter("price","010", "020", old),BooleanClause.Occur.SHOULD));
      booleanFilter.add(new FilterClause(getRangeFilter("price","020", "030", old),BooleanClause.Occur.SHOULD));
      booleanFilter.add(new FilterClause(getTermsFilter("accessRights", "admin", old),BooleanClause.Occur.MUST));
      booleanFilter.add(new FilterClause(getRangeFilter("date","20040101", "20041231", old),BooleanClause.Occur.MUST));
      tstFilterCard("Shoulds Ored but MUSTs ANDED",1,booleanFilter);
    }
	}
	public void testShouldsAndMustsAndMustNot() throws Throwable
	{
    for (int i = 0; i < 2; i++) {
      boolean old = (i==0);

      BooleanFilter booleanFilter = new BooleanFilter();
      booleanFilter.add(new FilterClause(getRangeFilter("price","030", "040", old),BooleanClause.Occur.SHOULD));
      booleanFilter.add(new FilterClause(getTermsFilter("accessRights", "admin", old),BooleanClause.Occur.MUST));
      booleanFilter.add(new FilterClause(getRangeFilter("date","20050101", "20051231", old),BooleanClause.Occur.MUST));
      booleanFilter.add(new FilterClause(getTermsFilter("inStock","N", old),BooleanClause.Occur.MUST_NOT));
      tstFilterCard("Shoulds Ored but MUSTs ANDED and MustNot",0,booleanFilter);
    }
	}
	
	public void testJustMust() throws Throwable
	{
    for (int i = 0; i < 2; i++) {
      boolean old = (i==0);

      BooleanFilter booleanFilter = new BooleanFilter();
      booleanFilter.add(new FilterClause(getTermsFilter("accessRights", "admin", old),BooleanClause.Occur.MUST));
      tstFilterCard("MUST",3,booleanFilter);
    }
	}
	public void testJustMustNot() throws Throwable
	{
    for (int i = 0; i < 2; i++) {
      boolean old = (i==0);

      BooleanFilter booleanFilter = new BooleanFilter();
      booleanFilter.add(new FilterClause(getTermsFilter("inStock","N", old),BooleanClause.Occur.MUST_NOT));
      tstFilterCard("MUST_NOT",4,booleanFilter);
    }
	}
	public void testMustAndMustNot() throws Throwable
	{
    for (int i = 0; i < 2; i++) {
      boolean old = (i==0);

      BooleanFilter booleanFilter = new BooleanFilter();
      booleanFilter.add(new FilterClause(getTermsFilter("inStock","N", old),BooleanClause.Occur.MUST));
      booleanFilter.add(new FilterClause(getTermsFilter("price","030", old),BooleanClause.Occur.MUST_NOT));
      tstFilterCard("MUST_NOT wins over MUST for same docs",0,booleanFilter);
    }
	}
}
