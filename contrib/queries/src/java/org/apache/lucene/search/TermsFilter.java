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
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Iterator;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermDocs;

/**
 * Constructs a filter for docs matching any of the terms added to this class. 
 * Unlike a RangeFilter this can be used for filtering on multiple terms that are not necessarily in 
 * a sequence. An example might be a collection of primary keys from a database query result or perhaps 
 * a choice of "category" labels picked by the end user. As a filter, this is much faster than the 
 * equivalent query (a BooleanQuery with many "should" TermQueries)
 * 
 * @author maharwood
 */
public class TermsFilter extends Filter
{
	ArrayList termsList=new ArrayList();
	
	/**
	 * Adds a term to the list of acceptable terms   
	 * @param term
	 */
	public void addTerm(Term term)
	{
		termsList.add(term);
	}

	/* (non-Javadoc)
	 * @see org.apache.lucene.search.Filter#bits(org.apache.lucene.index.IndexReader)
	 */
	public BitSet bits(IndexReader reader) throws IOException
	{
		BitSet result=new BitSet(reader.maxDoc());
		for (Iterator iter = termsList.iterator(); iter.hasNext();)
		{
			Term term = (Term) iter.next();
			TermDocs td=reader.termDocs(term);
	        while (td.next())
	        {
	            result.set(td.doc());
	        }						
		}
		return result;
	}
	
	public boolean equals(Object obj)
	{
		if(this == obj)
			return true;
		if((obj == null) || (obj.getClass() != this.getClass()))
				return false;
		TermsFilter test = (TermsFilter)obj;
		return (termsList == test.termsList|| 
					 (termsList!= null && termsList.equals(test.termsList)));
	}

	public int hashCode()
	{
		int hash=9;
		for (Iterator iter = termsList.iterator(); iter.hasNext();)
		{
			Term term = (Term) iter.next();
			hash = 31 * hash + term.hashCode();			
		}
		return hash;
	}
	
}
