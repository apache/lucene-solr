package org.apache.lucene.search;

/**
 * Copyright 2004 The Apache Software Foundation
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

import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.index.*;
import org.apache.lucene.analysis.SimpleAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;

import java.rmi.Naming;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.io.IOException;
import java.io.Serializable;
import java.util.regex.Pattern;
import java.util.HashMap;
import java.util.Iterator;

import junit.framework.TestCase;
import junit.framework.Test;
import junit.framework.TestSuite;
import junit.textui.TestRunner;

/**
 * Unit tests for sorting code.
 *
 * <p>Created: Feb 17, 2004 4:55:10 PM
 *
 * @author  Tim Jones (Nacimiento Software)
 * @since   lucene 1.4
 * @version $Id$
 */

public class TestSort
extends TestCase
implements Serializable {

	private Searcher full;
	private Searcher searchX;
	private Searcher searchY;
	private Query queryX;
	private Query queryY;
	private Query queryA;
	private Sort sort;


	public TestSort (String name) {
		super (name);
	}

	public static void main (String[] argv) {
		if (argv == null || argv.length < 1)
			TestRunner.run (suite());
		else if ("server".equals (argv[0])) {
			TestSort test = new TestSort (null);
			try {
				test.startServer();
				Thread.sleep (500000);
			} catch (Exception e) {
				System.out.println (e);
				e.printStackTrace();
			}
		}
	}

	public static Test suite() {
		return new TestSuite (TestSort.class);
	}


	// document data:
	// the tracer field is used to determine which document was hit
	// the contents field is used to search and sort by relevance
	// the int field to sort by int
	// the float field to sort by float
	// the string field to sort by string
	private String[][] data = new String[][] {
	// tracer  contents         int            float           string   custom
	{   "A",   "x a",           "5",           "4f",           "c",     "A-3"   },
	{   "B",   "y a",           "5",           "3.4028235E38", "i",     "B-10"  },
	{   "C",   "x a b c",       "2147483647",  "1.0",          "j",     "A-2"   },
	{   "D",   "y a b c",       "-1",          "0.0f",         "a",     "C-0"   },
	{   "E",   "x a b c d",     "5",           "2f",           "h",     "B-8"   },
	{   "F",   "y a b c d",     "2",           "3.14159f",     "g",     "B-1"   },
	{   "G",   "x a b c d",     "3",           "-1.0",         "f",     "C-100" },
	{   "H",   "y a b c d",     "0",           "1.4E-45",      "e",     "C-88"  },
	{   "I",   "x a b c d e f", "-2147483648", "1.0e+0",       "d",     "A-10"  },
	{   "J",   "y a b c d e f", "4",           ".5",           "b",     "C-7"   },
	};

	// create an index of all the documents, or just the x, or just the y documents
	private Searcher getIndex (boolean even, boolean odd)
	throws IOException {
		RAMDirectory indexStore = new RAMDirectory ();
		IndexWriter writer = new IndexWriter (indexStore, new SimpleAnalyzer(), true);
		for (int i=0; i<data.length; ++i) {
			if (((i%2)==0 && even) || ((i%2)==1 && odd)) {
				Document doc = new Document();          // store, index, token
				doc.add (new Field ("tracer",   data[i][0], true, false, false));
				doc.add (new Field ("contents", data[i][1], false, true, true));
				doc.add (new Field ("int",      data[i][2], false, true, false));
				doc.add (new Field ("float",    data[i][3], false, true, false));
				doc.add (new Field ("string",   data[i][4], false, true, false));
				doc.add (new Field ("custom",   data[i][5], false, true, false));
				writer.addDocument (doc);
			}
		}
		writer.optimize ();
		writer.close ();
		return new IndexSearcher (indexStore);
	}

	private Searcher getFullIndex()
	throws IOException {
		return getIndex (true, true);
	}

	private Searcher getXIndex()
	throws IOException {
		return getIndex (true, false);
	}

	private Searcher getYIndex()
	throws IOException {
		return getIndex (false, true);
	}

	private Searcher getEmptyIndex()
	throws IOException {
		return getIndex (false, false);
	}

	public void setUp() throws Exception {
		full = getFullIndex();
		searchX = getXIndex();
		searchY = getYIndex();
		queryX = new TermQuery (new Term ("contents", "x"));
		queryY = new TermQuery (new Term ("contents", "y"));
		queryA = new TermQuery (new Term ("contents", "a"));
		sort = new Sort();
	}

	// test the sorts by score and document number
	public void testBuiltInSorts() throws Exception {
		sort = new Sort();
		assertMatches (full, queryX, sort, "ACEGI");
		assertMatches (full, queryY, sort, "BDFHJ");

		sort.setSort(SortField.FIELD_DOC);
		assertMatches (full, queryX, sort, "ACEGI");
		assertMatches (full, queryY, sort, "BDFHJ");
	}

	// test sorts where the type of field is specified
	public void testTypedSort() throws Exception {
		sort.setSort (new SortField[] { new SortField ("int", SortField.INT), SortField.FIELD_DOC });
		assertMatches (full, queryX, sort, "IGAEC");
		assertMatches (full, queryY, sort, "DHFJB");

		sort.setSort (new SortField[] { new SortField ("float", SortField.FLOAT), SortField.FIELD_DOC });
		assertMatches (full, queryX, sort, "GCIEA");
		assertMatches (full, queryY, sort, "DHJFB");

		sort.setSort (new SortField[] { new SortField ("string", SortField.STRING), SortField.FIELD_DOC });
		assertMatches (full, queryX, sort, "AIGEC");
		assertMatches (full, queryY, sort, "DJHFB");
	}

	// test sorts when there's nothing in the index
	public void testEmptyIndex() throws Exception {
		Searcher empty = getEmptyIndex();

		sort = new Sort();
		assertMatches (empty, queryX, sort, "");

		sort.setSort(SortField.FIELD_DOC);
		assertMatches (empty, queryX, sort, "");

		sort.setSort (new SortField[] { new SortField ("int", SortField.INT), SortField.FIELD_DOC });
		assertMatches (empty, queryX, sort, "");

		sort.setSort (new SortField[] { new SortField ("string", SortField.STRING, true), SortField.FIELD_DOC });
		assertMatches (empty, queryX, sort, "");

		sort.setSort (new SortField[] { new SortField ("float", SortField.FLOAT), new SortField ("string", SortField.STRING) });
		assertMatches (empty, queryX, sort, "");
	}

	// test sorts where the type of field is determined dynamically
	public void testAutoSort() throws Exception {
		sort.setSort("int");
		assertMatches (full, queryX, sort, "IGAEC");
		assertMatches (full, queryY, sort, "DHFJB");

		sort.setSort("float");
		assertMatches (full, queryX, sort, "GCIEA");
		assertMatches (full, queryY, sort, "DHJFB");

		sort.setSort("string");
		assertMatches (full, queryX, sort, "AIGEC");
		assertMatches (full, queryY, sort, "DJHFB");
	}

	// test sorts in reverse
	public void testReverseSort() throws Exception {
		sort.setSort (new SortField[] { new SortField (null, SortField.SCORE, true), SortField.FIELD_DOC });
		assertMatches (full, queryX, sort, "IEGCA");
		assertMatches (full, queryY, sort, "JFHDB");

		sort.setSort (new SortField (null, SortField.DOC, true));
		assertMatches (full, queryX, sort, "IGECA");
		assertMatches (full, queryY, sort, "JHFDB");

		sort.setSort ("int", true);
		assertMatches (full, queryX, sort, "CAEGI");
		assertMatches (full, queryY, sort, "BJFHD");

		sort.setSort ("float", true);
		assertMatches (full, queryX, sort, "AECIG");
		assertMatches (full, queryY, sort, "BFJHD");

		sort.setSort ("string", true);
		assertMatches (full, queryX, sort, "CEGIA");
		assertMatches (full, queryY, sort, "BFHJD");
	}

	// test sorts using a series of fields
	public void testSortCombos() throws Exception {
		sort.setSort (new String[] {"int","float"});
		assertMatches (full, queryX, sort, "IGEAC");

		sort.setSort (new SortField[] { new SortField ("int", true), new SortField (null, SortField.DOC, true) });
		assertMatches (full, queryX, sort, "CEAGI");

		sort.setSort (new String[] {"float","string"});
		assertMatches (full, queryX, sort, "GICEA");
	}


	public void testCustomSorts() throws Exception {
		sort.setSort (new SortField ("custom", SampleComparable.getComparatorSource()));
		assertMatches (full, queryX, sort, "CAIEG");
		sort.setSort (new SortField ("custom", SampleComparable.getComparatorSource(), true));
		assertMatches (full, queryY, sort, "HJDBF");
		SortComparator custom = SampleComparable.getComparator();
		sort.setSort (new SortField ("custom", custom));
		assertMatches (full, queryX, sort, "CAIEG");
		sort.setSort (new SortField ("custom", custom, true));
		assertMatches (full, queryY, sort, "HJDBF");
	}

	// test a variety of sorts using more than one searcher
	public void testMultiSort() throws Exception {
		MultiSearcher searcher = new MultiSearcher (new Searchable[] { searchX, searchY });
		runMultiSorts (searcher);
	}

	// test a variety of sorts using a parallel multisearcher
	public void testParallelMultiSort() throws Exception {
		Searcher searcher = new ParallelMultiSearcher (new Searchable[] { searchX, searchY });
		runMultiSorts (searcher);
	}

	// test a variety of sorts using a remote searcher
	public void testRemoteSort() throws Exception {
		Searchable searcher = getRemote();
		MultiSearcher multi = new MultiSearcher (new Searchable[] { searcher });
		runMultiSorts (multi);
	}

	public void testRemoteCustomSort() throws Exception {
		Searchable searcher = getRemote();
		MultiSearcher multi = new MultiSearcher (new Searchable[] { searcher });
		sort.setSort (new SortField ("custom", SampleComparable.getComparatorSource()));
		assertMatches (multi, queryX, sort, "CAIEG");
		sort.setSort (new SortField ("custom", SampleComparable.getComparatorSource(), true));
		assertMatches (multi, queryY, sort, "HJDBF");
		SortComparator custom = SampleComparable.getComparator();
		sort.setSort (new SortField ("custom", custom));
		assertMatches (multi, queryX, sort, "CAIEG");
		sort.setSort (new SortField ("custom", custom, true));
		assertMatches (multi, queryY, sort, "HJDBF");
	}

	// test that the relevancy scores are the same even if
	// hits are sorted
	public void testNormalizedScores() throws Exception {

		// capture relevancy scores
		HashMap scoresX = getScores (full.search (queryX));
		HashMap scoresY = getScores (full.search (queryY));
		HashMap scoresA = getScores (full.search (queryA));

		// we'll test searching locally, remote and multi
		// note: the multi test depends on each separate index containing
		// the same documents as our local index, so the computed normalization
		// will be the same.  so we make a multi searcher over two equal document
		// sets - not realistic, but necessary for testing.
		MultiSearcher remote = new MultiSearcher (new Searchable[] { getRemote() });
		MultiSearcher multi  = new MultiSearcher (new Searchable[] { full, full });

		// change sorting and make sure relevancy stays the same

		sort = new Sort();
		assertSameValues (scoresX, getScores(full.search(queryX,sort)));
		assertSameValues (scoresX, getScores(remote.search(queryX,sort)));
		assertSameValues (scoresX, getScores(multi.search(queryX,sort)));
		assertSameValues (scoresY, getScores(full.search(queryY,sort)));
		assertSameValues (scoresY, getScores(remote.search(queryY,sort)));
		assertSameValues (scoresY, getScores(multi.search(queryY,sort)));
		assertSameValues (scoresA, getScores(full.search(queryA,sort)));
		assertSameValues (scoresA, getScores(remote.search(queryA,sort)));
		assertSameValues (scoresA, getScores(multi.search(queryA,sort)));

		sort.setSort(SortField.FIELD_DOC);
		assertSameValues (scoresX, getScores(full.search(queryX,sort)));
		assertSameValues (scoresX, getScores(remote.search(queryX,sort)));
		assertSameValues (scoresX, getScores(multi.search(queryX,sort)));
		assertSameValues (scoresY, getScores(full.search(queryY,sort)));
		assertSameValues (scoresY, getScores(remote.search(queryY,sort)));
		assertSameValues (scoresY, getScores(multi.search(queryY,sort)));
		assertSameValues (scoresA, getScores(full.search(queryA,sort)));
		assertSameValues (scoresA, getScores(remote.search(queryA,sort)));
		assertSameValues (scoresA, getScores(multi.search(queryA,sort)));

		sort.setSort ("int");
		assertSameValues (scoresX, getScores(full.search(queryX,sort)));
		assertSameValues (scoresX, getScores(remote.search(queryX,sort)));
		assertSameValues (scoresX, getScores(multi.search(queryX,sort)));
		assertSameValues (scoresY, getScores(full.search(queryY,sort)));
		assertSameValues (scoresY, getScores(remote.search(queryY,sort)));
		assertSameValues (scoresY, getScores(multi.search(queryY,sort)));
		assertSameValues (scoresA, getScores(full.search(queryA,sort)));
		assertSameValues (scoresA, getScores(remote.search(queryA,sort)));
		assertSameValues (scoresA, getScores(multi.search(queryA,sort)));

		sort.setSort ("float");
		assertSameValues (scoresX, getScores(full.search(queryX,sort)));
		assertSameValues (scoresX, getScores(remote.search(queryX,sort)));
		assertSameValues (scoresX, getScores(multi.search(queryX,sort)));
		assertSameValues (scoresY, getScores(full.search(queryY,sort)));
		assertSameValues (scoresY, getScores(remote.search(queryY,sort)));
		assertSameValues (scoresY, getScores(multi.search(queryY,sort)));
		assertSameValues (scoresA, getScores(full.search(queryA,sort)));
		assertSameValues (scoresA, getScores(remote.search(queryA,sort)));
		assertSameValues (scoresA, getScores(multi.search(queryA,sort)));

		sort.setSort ("string");
		assertSameValues (scoresX, getScores(full.search(queryX,sort)));
		assertSameValues (scoresX, getScores(remote.search(queryX,sort)));
		assertSameValues (scoresX, getScores(multi.search(queryX,sort)));
		assertSameValues (scoresY, getScores(full.search(queryY,sort)));
		assertSameValues (scoresY, getScores(remote.search(queryY,sort)));
		assertSameValues (scoresY, getScores(multi.search(queryY,sort)));
		assertSameValues (scoresA, getScores(full.search(queryA,sort)));
		assertSameValues (scoresA, getScores(remote.search(queryA,sort)));
		assertSameValues (scoresA, getScores(multi.search(queryA,sort)));

		sort.setSort (new String[] {"int","float"});
		assertSameValues (scoresX, getScores(full.search(queryX,sort)));
		assertSameValues (scoresX, getScores(remote.search(queryX,sort)));
		assertSameValues (scoresX, getScores(multi.search(queryX,sort)));
		assertSameValues (scoresY, getScores(full.search(queryY,sort)));
		assertSameValues (scoresY, getScores(remote.search(queryY,sort)));
		assertSameValues (scoresY, getScores(multi.search(queryY,sort)));
		assertSameValues (scoresA, getScores(full.search(queryA,sort)));
		assertSameValues (scoresA, getScores(remote.search(queryA,sort)));
		assertSameValues (scoresA, getScores(multi.search(queryA,sort)));

		sort.setSort (new SortField[] { new SortField ("int", true), new SortField (null, SortField.DOC, true) });
		assertSameValues (scoresX, getScores(full.search(queryX,sort)));
		assertSameValues (scoresX, getScores(remote.search(queryX,sort)));
		assertSameValues (scoresX, getScores(multi.search(queryX,sort)));
		assertSameValues (scoresY, getScores(full.search(queryY,sort)));
		assertSameValues (scoresY, getScores(remote.search(queryY,sort)));
		assertSameValues (scoresY, getScores(multi.search(queryY,sort)));
		assertSameValues (scoresA, getScores(full.search(queryA,sort)));
		assertSameValues (scoresA, getScores(remote.search(queryA,sort)));
		assertSameValues (scoresA, getScores(multi.search(queryA,sort)));

		sort.setSort (new String[] {"float","string"});
		assertSameValues (scoresX, getScores(full.search(queryX,sort)));
		assertSameValues (scoresX, getScores(remote.search(queryX,sort)));
		assertSameValues (scoresX, getScores(multi.search(queryX,sort)));
		assertSameValues (scoresY, getScores(full.search(queryY,sort)));
		assertSameValues (scoresY, getScores(remote.search(queryY,sort)));
		assertSameValues (scoresY, getScores(multi.search(queryY,sort)));
		assertSameValues (scoresA, getScores(full.search(queryA,sort)));
		assertSameValues (scoresA, getScores(remote.search(queryA,sort)));
		assertSameValues (scoresA, getScores(multi.search(queryA,sort)));

	}

	// runs a variety of sorts useful for multisearchers
	private void runMultiSorts (Searcher multi) throws Exception {
		sort.setSort (SortField.FIELD_DOC);
		assertMatchesPattern (multi, queryA, sort, "[AB]{2}[CD]{2}[EF]{2}[GH]{2}[IJ]{2}");

		sort.setSort (new SortField ("int", SortField.INT));
		assertMatchesPattern (multi, queryA, sort, "IDHFGJ[ABE]{3}C");

		sort.setSort (new SortField[] {new SortField ("int", SortField.INT), SortField.FIELD_DOC});
		assertMatchesPattern (multi, queryA, sort, "IDHFGJ[AB]{2}EC");

		sort.setSort ("int");
		assertMatchesPattern (multi, queryA, sort, "IDHFGJ[AB]{2}EC");

		sort.setSort (new SortField[] {new SortField ("float", SortField.FLOAT), SortField.FIELD_DOC});
		assertMatchesPattern (multi, queryA, sort, "GDHJ[CI]{2}EFAB");

		sort.setSort ("float");
		assertMatchesPattern (multi, queryA, sort, "GDHJ[CI]{2}EFAB");

		sort.setSort ("string");
		assertMatches (multi, queryA, sort, "DJAIHGFEBC");

		sort.setSort ("int", true);
		assertMatchesPattern (multi, queryA, sort, "C[AB]{2}EJGFHDI");

		sort.setSort ("float", true);
		assertMatchesPattern (multi, queryA, sort, "BAFE[IC]{2}JHDG");

		sort.setSort ("string", true);
		assertMatches (multi, queryA, sort, "CBEFGHIAJD");

		sort.setSort (new String[] {"int","float"});
		assertMatches (full, queryA, sort, "IDHFGJEABC");

		sort.setSort (new String[] {"float","string"});
		assertMatches (full, queryA, sort, "GDHJICEFAB");
	}

	// make sure the documents returned by the search match the expected list
	private void assertMatches (Searcher searcher, Query query, Sort sort, String expectedResult)
	throws IOException {
		Hits result = searcher.search (query, sort);
		StringBuffer buff = new StringBuffer(10);
		int n = result.length();
		for (int i=0; i<n; ++i) {
			Document doc = result.doc(i);
			String[] v = doc.getValues("tracer");
			for (int j=0; j<v.length; ++j) {
				buff.append (v[j]);
			}
		}
		assertEquals (expectedResult, buff.toString());
	}

	// make sure the documents returned by the search match the expected list pattern
	private void assertMatchesPattern (Searcher searcher, Query query, Sort sort, String pattern)
	throws IOException {
		Hits result = searcher.search (query, sort);
		StringBuffer buff = new StringBuffer(10);
		int n = result.length();
		for (int i=0; i<n; ++i) {
			Document doc = result.doc(i);
			String[] v = doc.getValues("tracer");
			for (int j=0; j<v.length; ++j) {
				buff.append (v[j]);
			}
		}
		// System.out.println ("matching \""+buff+"\" against pattern \""+pattern+"\"");
		assertTrue (Pattern.compile(pattern).matcher(buff.toString()).matches());
	}

	private HashMap getScores (Hits hits)
	throws IOException {
		HashMap scoreMap = new HashMap();
		int n = hits.length();
		for (int i=0; i<n; ++i) {
			Document doc = hits.doc(i);
			String[] v = doc.getValues("tracer");
			assertEquals (v.length, 1);
			scoreMap.put (v[0], new Float(hits.score(i)));
		}
		return scoreMap;
	}

	// make sure all the values in the maps match
	private void assertSameValues (HashMap m1, HashMap m2) {
		int n = m1.size();
		int m = m2.size();
		assertEquals (n, m);
		Iterator iter = m1.keySet().iterator();
		while (iter.hasNext()) {
			Object key = iter.next();
			assertEquals (m1.get(key), m2.get(key));
		}
	}

	private Searchable getRemote () throws Exception {
		try {
			return lookupRemote ();
		} catch (Throwable e) {
			startServer ();
			return lookupRemote ();
		}
	}

	private Searchable lookupRemote () throws Exception {
		return (Searchable) Naming.lookup ("//localhost/SortedSearchable");
	}

	private void startServer () throws Exception {
		// construct an index
		Searcher local = getFullIndex();
		// local.search (queryA, new Sort());

		// publish it
		Registry reg = LocateRegistry.createRegistry (1099);
		RemoteSearchable impl = new RemoteSearchable (local);
		Naming.rebind ("//localhost/SortedSearchable", impl);
	}

}
