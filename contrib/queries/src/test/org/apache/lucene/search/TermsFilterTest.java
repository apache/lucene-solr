package org.apache.lucene.search;

import java.util.HashSet;

import junit.framework.TestCase;

import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.IndexWriter.MaxFieldLength;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.OpenBitSet;

public class TermsFilterTest extends TestCase
{
	public void testCachability() throws Exception
	{
		TermsFilter a=new TermsFilter();
		a.addTerm(new Term("field1","a"));
		a.addTerm(new Term("field1","b"));
		HashSet cachedFilters=new HashSet();
		cachedFilters.add(a);
		TermsFilter b=new TermsFilter();
		b.addTerm(new Term("field1","a"));
		b.addTerm(new Term("field1","b"));
		
		assertTrue("Must be cached",cachedFilters.contains(b));
		b.addTerm(new Term("field1","a")); //duplicate term
		assertTrue("Must be cached",cachedFilters.contains(b));
		b.addTerm(new Term("field1","c"));
		assertFalse("Must not be cached",cachedFilters.contains(b));
		
	}
	public void testMissingTerms() throws Exception
	{
		String fieldName="field1";
		RAMDirectory rd=new RAMDirectory();
		IndexWriter w=new IndexWriter(rd,new WhitespaceAnalyzer(),MaxFieldLength.UNLIMITED);
		for (int i = 0; i < 100; i++)
		{
			Document doc=new Document();
			int term=i*10; //terms are units of 10;
			doc.add(new Field(fieldName,""+term,Field.Store.YES,Field.Index.NOT_ANALYZED));
			w.addDocument(doc);			
		}
		w.close();
		IndexReader reader = IndexReader.open(rd);
		
		TermsFilter tf=new TermsFilter();
		tf.addTerm(new Term(fieldName,"19"));
		OpenBitSet bits = (OpenBitSet)tf.getDocIdSet(reader);
		assertEquals("Must match nothing", 0, bits.cardinality());

		tf.addTerm(new Term(fieldName,"20"));
		bits = (OpenBitSet)tf.getDocIdSet(reader);
		assertEquals("Must match 1", 1, bits.cardinality());
		
		tf.addTerm(new Term(fieldName,"10"));
		bits = (OpenBitSet)tf.getDocIdSet(reader);
		assertEquals("Must match 2", 2, bits.cardinality());
		
		tf.addTerm(new Term(fieldName,"00"));
		bits = (OpenBitSet)tf.getDocIdSet(reader);
		assertEquals("Must match 2", 2, bits.cardinality());
				
	}
}
