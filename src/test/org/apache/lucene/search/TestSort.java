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
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.analysis.SimpleAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;

import java.rmi.Naming;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.io.IOException;
import java.util.regex.Pattern;

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
extends TestCase {

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
	// tracer  contents         int            float           string
	{   "A",   "x a",           "5",           "4f",           "c" },
	{   "B",   "y a",           "5",           "3.4028235E38", "i" },
	{   "C",   "x a b c",       "2147483647",  "1.0",          "j" },
	{   "D",   "y a b c",       "-1",          "0.0f",         "a" },
	{   "E",   "x a b c d",     "5",           "2f",           "h" },
	{   "F",   "y a b c d",     "2",           "3.14159f",     "g" },
	{   "G",   "x a b c d",     "3",           "-1.0",         "f" },
	{   "H",   "y a b c d",     "0",           "1.4E-45",      "e" },
	{   "I",   "x a b c d e f", "-2147483648", "1.0e+0",       "d" },
	{   "J",   "y a b c d e f", "4",           ".5",           "b" },
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
