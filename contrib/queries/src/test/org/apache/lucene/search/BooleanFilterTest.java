package org.apache.lucene.search;

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
		doc.add(new Field("accessRights",accessRights,Field.Store.YES,Field.Index.TOKENIZED));
		doc.add(new Field("price",price,Field.Store.YES,Field.Index.TOKENIZED));
		doc.add(new Field("date",date,Field.Store.YES,Field.Index.TOKENIZED));
		doc.add(new Field("inStock",inStock,Field.Store.YES,Field.Index.TOKENIZED));
		writer.addDocument(doc);
	}
	
	private Filter getRangeFilter(String field,String lowerPrice, String upperPrice)
	{
		return new RangeFilter(field,lowerPrice,upperPrice,true,true);
	}
	private TermsFilter getTermsFilter(String field,String text)
	{
		TermsFilter tf=new TermsFilter();
		tf.addTerm(new Term(field,text));
		return tf;
	}
		
	public void testShould() throws Throwable
	{
		BooleanFilter booleanFilter = new BooleanFilter();
		booleanFilter.add(new FilterClause(getTermsFilter("price","030"),BooleanClause.Occur.SHOULD));
		BitSet bits = booleanFilter.bits(reader);
		assertEquals("Should retrieves only 1 doc",1,bits.cardinality());
	}
	
	public void testShoulds() throws Throwable
	{
		BooleanFilter booleanFilter = new BooleanFilter();
		booleanFilter.add(new FilterClause(getRangeFilter("price","010", "020"),BooleanClause.Occur.SHOULD));
		booleanFilter.add(new FilterClause(getRangeFilter("price","020", "030"),BooleanClause.Occur.SHOULD));
		BitSet bits = booleanFilter.bits(reader);
		assertEquals("Shoulds are Ored together",5,bits.cardinality());
	}
	public void testShouldsAndMustNot() throws Throwable
	{
		BooleanFilter booleanFilter = new BooleanFilter();
		booleanFilter.add(new FilterClause(getRangeFilter("price","010", "020"),BooleanClause.Occur.SHOULD));
		booleanFilter.add(new FilterClause(getRangeFilter("price","020", "030"),BooleanClause.Occur.SHOULD));
		booleanFilter.add(new FilterClause(getTermsFilter("inStock", "N"),BooleanClause.Occur.MUST_NOT));
		BitSet bits = booleanFilter.bits(reader);
		assertEquals("Shoulds Ored but AndNot",4,bits.cardinality());

		booleanFilter.add(new FilterClause(getTermsFilter("inStock", "Maybe"),BooleanClause.Occur.MUST_NOT));
		bits = booleanFilter.bits(reader);
		assertEquals("Shoulds Ored but AndNots",3,bits.cardinality());
		
	}
	public void testShouldsAndMust() throws Throwable
	{
		BooleanFilter booleanFilter = new BooleanFilter();
		booleanFilter.add(new FilterClause(getRangeFilter("price","010", "020"),BooleanClause.Occur.SHOULD));
		booleanFilter.add(new FilterClause(getRangeFilter("price","020", "030"),BooleanClause.Occur.SHOULD));
		booleanFilter.add(new FilterClause(getTermsFilter("accessRights", "admin"),BooleanClause.Occur.MUST));
		BitSet bits = booleanFilter.bits(reader);
		assertEquals("Shoulds Ored but MUST",3,bits.cardinality());
	}
	public void testShouldsAndMusts() throws Throwable
	{
		BooleanFilter booleanFilter = new BooleanFilter();
		booleanFilter.add(new FilterClause(getRangeFilter("price","010", "020"),BooleanClause.Occur.SHOULD));
		booleanFilter.add(new FilterClause(getRangeFilter("price","020", "030"),BooleanClause.Occur.SHOULD));
		booleanFilter.add(new FilterClause(getTermsFilter("accessRights", "admin"),BooleanClause.Occur.MUST));
		booleanFilter.add(new FilterClause(getRangeFilter("date","20040101", "20041231"),BooleanClause.Occur.MUST));
		BitSet bits = booleanFilter.bits(reader);
		assertEquals("Shoulds Ored but MUSTs ANDED",1,bits.cardinality());
	}
	public void testShouldsAndMustsAndMustNot() throws Throwable
	{
		BooleanFilter booleanFilter = new BooleanFilter();
		booleanFilter.add(new FilterClause(getRangeFilter("price","030", "040"),BooleanClause.Occur.SHOULD));
		booleanFilter.add(new FilterClause(getTermsFilter("accessRights", "admin"),BooleanClause.Occur.MUST));
		booleanFilter.add(new FilterClause(getRangeFilter("date","20050101", "20051231"),BooleanClause.Occur.MUST));
		booleanFilter.add(new FilterClause(getTermsFilter("inStock","N"),BooleanClause.Occur.MUST_NOT));
		BitSet bits = booleanFilter.bits(reader);
		assertEquals("Shoulds Ored but MUSTs ANDED and MustNot",0,bits.cardinality());
	}
	
	public void testJustMust() throws Throwable
	{
		BooleanFilter booleanFilter = new BooleanFilter();
		booleanFilter.add(new FilterClause(getTermsFilter("accessRights", "admin"),BooleanClause.Occur.MUST));
		BitSet bits = booleanFilter.bits(reader);
		assertEquals("MUST",3,bits.cardinality());
	}
	public void testJustMustNot() throws Throwable
	{
		BooleanFilter booleanFilter = new BooleanFilter();
		booleanFilter.add(new FilterClause(getTermsFilter("inStock","N"),BooleanClause.Occur.MUST_NOT));
		BitSet bits = booleanFilter.bits(reader);
		assertEquals("MUST_NOT",4,bits.cardinality());
	}
	public void testMustAndMustNot() throws Throwable
	{
		BooleanFilter booleanFilter = new BooleanFilter();
		booleanFilter.add(new FilterClause(getTermsFilter("inStock","N"),BooleanClause.Occur.MUST));
		booleanFilter.add(new FilterClause(getTermsFilter("price","030"),BooleanClause.Occur.MUST_NOT));
		BitSet bits = booleanFilter.bits(reader);
		assertEquals("MUST_NOT wins over MUST for same docs",0,bits.cardinality());
	}

	
	
}
