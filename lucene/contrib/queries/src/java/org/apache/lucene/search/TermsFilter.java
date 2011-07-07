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
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexReader.AtomicReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.index.Fields;
import org.apache.lucene.util.OpenBitSet;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Bits;

/**
 * Constructs a filter for docs matching any of the terms added to this class. 
 * Unlike a RangeFilter this can be used for filtering on multiple terms that are not necessarily in 
 * a sequence. An example might be a collection of primary keys from a database query result or perhaps 
 * a choice of "category" labels picked by the end user. As a filter, this is much faster than the 
 * equivalent query (a BooleanQuery with many "should" TermQueries)
 *
 */
public class TermsFilter extends Filter
{
	Set<Term> terms=new TreeSet<Term>();
	
	/**
	 * Adds a term to the list of acceptable terms   
	 * @param term
	 */
	public void addTerm(Term term)
	{
		terms.add(term);
	}
	
/* (non-Javadoc)
   * @see org.apache.lucene.search.Filter#getDocIdSet(org.apache.lucene.index.IndexReader)
	 */
  @Override
  public DocIdSet getDocIdSet(AtomicReaderContext context) throws IOException {
    IndexReader reader = context.reader;
    OpenBitSet result=new OpenBitSet(reader.maxDoc());
    Fields fields = reader.fields();
    BytesRef br = new BytesRef();
    Bits liveDocs = reader.getLiveDocs();
    if (fields != null) {
      String lastField = null;
      Terms termsC = null;
      TermsEnum termsEnum = null;
      DocsEnum docs = null;
      for (Iterator<Term> iter = terms.iterator(); iter.hasNext();) {
        Term term = iter.next();
        if (!term.field().equals(lastField)) {
          termsC = fields.terms(term.field());
          termsEnum = termsC.iterator();
          lastField = term.field();
        }

        if (terms != null) {
          br.copy(term.bytes());
          if (termsEnum.seekCeil(br) == TermsEnum.SeekStatus.FOUND) {
            docs = termsEnum.docs(liveDocs, docs);
            while(docs.nextDoc() != DocsEnum.NO_MORE_DOCS) {
              result.set(docs.docID());
            }
          }
        }
      }
    }
    return result;
  }
	
	@Override
	public boolean equals(Object obj)
	{
		if(this == obj)
			return true;
		if((obj == null) || (obj.getClass() != this.getClass()))
				return false;
		TermsFilter test = (TermsFilter)obj;
		return (terms == test.terms ||
					 (terms != null && terms.equals(test.terms)));
	}

	@Override
	public int hashCode()
	{
		int hash=9;
		for (Iterator<Term> iter = terms.iterator(); iter.hasNext();)
		{
			Term term = iter.next();
			hash = 31 * hash + term.hashCode();			
		}
		return hash;
	}
	
}
