package org.apache.lucene.search;


import java.io.IOException;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;

/**
 * A basic 'positive' Unit test class for the RangeFilter class.
 *
 * <p>
 * NOTE: at the moment, this class only tests for 'positive' results,
 * it does not verify the results to ensure their are no 'false positives',
 * nor does it adequately test 'negative' results.  It also does not test
 * that garbage in results in an Exception.
 */
public class TestRangeFilter extends BaseTestRangeFilter {

    public TestRangeFilter(String name) {
	super(name);
    }
    public TestRangeFilter() {
        super();
    }

    public void testRangeFilterId() throws IOException {

        IndexReader reader = IndexReader.open(index);
	IndexSearcher search = new IndexSearcher(reader);

        int medId = ((maxId - minId) / 2);
        
        String minIP = pad(minId);
        String maxIP = pad(maxId);
        String medIP = pad(medId);
    
        int numDocs = reader.numDocs();
        
        assertEquals("num of docs", numDocs, 1+ maxId - minId);
        
	Hits result;
        Query q = new TermQuery(new Term("body","body"));

        // test id, bounded on both ends
        
	result = search.search(q,new RangeFilter("id",minIP,maxIP,T,T));
	assertEquals("find all", numDocs, result.length());

	result = search.search(q,new RangeFilter("id",minIP,maxIP,T,F));
	assertEquals("all but last", numDocs-1, result.length());

	result = search.search(q,new RangeFilter("id",minIP,maxIP,F,T));
	assertEquals("all but first", numDocs-1, result.length());
        
	result = search.search(q,new RangeFilter("id",minIP,maxIP,F,F));
        assertEquals("all but ends", numDocs-2, result.length());
    
        result = search.search(q,new RangeFilter("id",medIP,maxIP,T,T));
        assertEquals("med and up", 1+ maxId-medId, result.length());
        
        result = search.search(q,new RangeFilter("id",minIP,medIP,T,T));
        assertEquals("up to med", 1+ medId-minId, result.length());

        // unbounded id

	result = search.search(q,new RangeFilter("id",minIP,null,T,F));
	assertEquals("min and up", numDocs, result.length());

	result = search.search(q,new RangeFilter("id",null,maxIP,F,T));
	assertEquals("max and down", numDocs, result.length());

	result = search.search(q,new RangeFilter("id",minIP,null,F,F));
	assertEquals("not min, but up", numDocs-1, result.length());
        
	result = search.search(q,new RangeFilter("id",null,maxIP,F,F));
	assertEquals("not max, but down", numDocs-1, result.length());
        
        result = search.search(q,new RangeFilter("id",medIP,maxIP,T,F));
        assertEquals("med and up, not max", maxId-medId, result.length());
        
        result = search.search(q,new RangeFilter("id",minIP,medIP,F,T));
        assertEquals("not min, up to med", medId-minId, result.length());

        // very small sets

	result = search.search(q,new RangeFilter("id",minIP,minIP,F,F));
	assertEquals("min,min,F,F", 0, result.length());
	result = search.search(q,new RangeFilter("id",medIP,medIP,F,F));
	assertEquals("med,med,F,F", 0, result.length());
	result = search.search(q,new RangeFilter("id",maxIP,maxIP,F,F));
	assertEquals("max,max,F,F", 0, result.length());
                     
	result = search.search(q,new RangeFilter("id",minIP,minIP,T,T));
	assertEquals("min,min,T,T", 1, result.length());
	result = search.search(q,new RangeFilter("id",null,minIP,F,T));
	assertEquals("nul,min,F,T", 1, result.length());

	result = search.search(q,new RangeFilter("id",maxIP,maxIP,T,T));
	assertEquals("max,max,T,T", 1, result.length());
	result = search.search(q,new RangeFilter("id",maxIP,null,T,F));
	assertEquals("max,nul,T,T", 1, result.length());

	result = search.search(q,new RangeFilter("id",medIP,medIP,T,T));
	assertEquals("med,med,T,T", 1, result.length());
        
    }

    public void testRangeFilterRand() throws IOException {

        IndexReader reader = IndexReader.open(index);
	IndexSearcher search = new IndexSearcher(reader);

        String minRP = pad(minR);
        String maxRP = pad(maxR);
    
        int numDocs = reader.numDocs();
        
        assertEquals("num of docs", numDocs, 1+ maxId - minId);
        
	Hits result;
        Query q = new TermQuery(new Term("body","body"));

        // test extremes, bounded on both ends
        
	result = search.search(q,new RangeFilter("rand",minRP,maxRP,T,T));
	assertEquals("find all", numDocs, result.length());

	result = search.search(q,new RangeFilter("rand",minRP,maxRP,T,F));
	assertEquals("all but biggest", numDocs-1, result.length());

	result = search.search(q,new RangeFilter("rand",minRP,maxRP,F,T));
	assertEquals("all but smallest", numDocs-1, result.length());
        
	result = search.search(q,new RangeFilter("rand",minRP,maxRP,F,F));
        assertEquals("all but extremes", numDocs-2, result.length());
    
        // unbounded

	result = search.search(q,new RangeFilter("rand",minRP,null,T,F));
	assertEquals("smallest and up", numDocs, result.length());

	result = search.search(q,new RangeFilter("rand",null,maxRP,F,T));
	assertEquals("biggest and down", numDocs, result.length());

	result = search.search(q,new RangeFilter("rand",minRP,null,F,F));
	assertEquals("not smallest, but up", numDocs-1, result.length());
        
	result = search.search(q,new RangeFilter("rand",null,maxRP,F,F));
	assertEquals("not biggest, but down", numDocs-1, result.length());
        
        // very small sets

	result = search.search(q,new RangeFilter("rand",minRP,minRP,F,F));
	assertEquals("min,min,F,F", 0, result.length());
	result = search.search(q,new RangeFilter("rand",maxRP,maxRP,F,F));
	assertEquals("max,max,F,F", 0, result.length());
                     
	result = search.search(q,new RangeFilter("rand",minRP,minRP,T,T));
	assertEquals("min,min,T,T", 1, result.length());
	result = search.search(q,new RangeFilter("rand",null,minRP,F,T));
	assertEquals("nul,min,F,T", 1, result.length());

	result = search.search(q,new RangeFilter("rand",maxRP,maxRP,T,T));
	assertEquals("max,max,T,T", 1, result.length());
	result = search.search(q,new RangeFilter("rand",maxRP,null,T,F));
	assertEquals("max,nul,T,T", 1, result.length());
        
    }

}
