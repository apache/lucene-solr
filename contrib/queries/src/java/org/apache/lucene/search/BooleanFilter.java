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

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.BooleanClause.Occur;

/**
 * A container Filter that allows Boolean composition of Filters.
 * Filters are allocated into one of three logical constructs;
 * SHOULD, MUST NOT, MUST
 * The results Filter BitSet is constructed as follows:
 * SHOULD Filters are OR'd together
 * The resulting Filter is NOT'd with the NOT Filters
 * The resulting Filter is AND'd with the MUST Filters
 * @author BPDThebault
 */

public class BooleanFilter extends Filter
{
	//ArrayList of SHOULD filters
	ArrayList shouldFilters = null;
	//ArrayList of NOT filters
	ArrayList notFilters = null;
	//ArrayList of MUST filters
	ArrayList mustFilters = null;

	/**
	 * Returns the a BitSet representing the Boolean composition
	 * of the filters that have been added.
	 */
	
	public BitSet bits(IndexReader reader) throws IOException
	{
		//create a new bitSet
		BitSet returnBits = null;
		
		//SHOULD filters
		if (shouldFilters!=null)
		{
			returnBits = ((Filter)shouldFilters.get(0)).bits(reader);
//			avoid changing the original bitset - it may be cached
			returnBits=(BitSet) returnBits.clone(); 
			if (shouldFilters.size() > 1)
			{
				for (int i = 1; i < shouldFilters.size(); i++)
				{
					returnBits.or(((Filter)shouldFilters.get(i)).bits(reader));
				}
			}
		}
		
		//NOT filters
		if (notFilters!=null)
		{
			for (int i = 0; i < notFilters.size(); i++)
			{
				BitSet notBits=((Filter)notFilters.get(i)).bits(reader);
				if(returnBits==null)
				{
					returnBits=(BitSet) notBits.clone();					
					returnBits.flip(0,reader.maxDoc());
				}
				else
				{
					returnBits.andNot(notBits);
				}
			}
		}
		
		//MUST filters
		if (mustFilters!=null)
		{
			for (int i = 0; i < mustFilters.size(); i++)
			{
				BitSet mustBits=((Filter)mustFilters.get(i)).bits(reader);
				if(returnBits==null)
				{
					if(mustFilters.size()==1)
					{
						returnBits=mustBits;
						
					}
					else
					{
						//don't mangle the bitset
						returnBits=(BitSet) mustBits.clone();						
					}
				}
				else
				{
					returnBits.and(mustBits);
				}
			}
		}
		if(returnBits==null)
		{
			returnBits=new BitSet(reader.maxDoc());
		}
		return returnBits;
	}
	
	/**
	 * Adds a new FilterClause to the Boolean Filter container
	 * @param filterClause A FilterClause object containing a Filter and an Occur parameter
	 */
	
	public void add(FilterClause filterClause)
	{
		if (filterClause.getOccur().equals(Occur.MUST))
		{
			if(mustFilters==null)
			{
				mustFilters=new ArrayList();
			}
			mustFilters.add(filterClause.getFilter());
		}
		if (filterClause.getOccur().equals(Occur.SHOULD))
		{
			if(shouldFilters==null)
			{
				shouldFilters=new ArrayList();
			}
			shouldFilters.add(filterClause.getFilter());
		}
		if (filterClause.getOccur().equals(Occur.MUST_NOT))
		{
			if(notFilters==null)
			{
				notFilters=new ArrayList();
			}
			notFilters.add(filterClause.getFilter());
		}
	}

	public boolean equals(Object obj)
	{
		if(this == obj)
			return true;
		if((obj == null) || (obj.getClass() != this.getClass()))
				return false;
		BooleanFilter test = (BooleanFilter)obj;
		return (notFilters == test.notFilters|| 
					 (notFilters!= null && notFilters.equals(test.notFilters)))
				&&
			   (mustFilters == test.mustFilters|| 
					 (mustFilters!= null && mustFilters.equals(test.mustFilters)))				 
					 &&
			   (shouldFilters == test.shouldFilters|| 
					 (shouldFilters!= null && shouldFilters.equals(test.shouldFilters)));
	}

	public int hashCode()
	{
		int hash=7;
		hash = 31 * hash + (null == mustFilters ? 0 : mustFilters.hashCode());
		hash = 31 * hash + (null == notFilters ? 0 : notFilters.hashCode());
		hash = 31 * hash + (null == shouldFilters ? 0 : shouldFilters.hashCode());
		return hash;
	}
	
	
		/** Prints a user-readable version of this query. */
	public String toString()
	{
		StringBuffer buffer = new StringBuffer();

		buffer.append("BooleanFilter(");

		appendFilters(shouldFilters, null, buffer);
		appendFilters(mustFilters, "+", buffer);
		appendFilters(notFilters, "-", buffer);

		buffer.append(")");

		return buffer.toString();
	}
	
	private void appendFilters(ArrayList filters, String occurString,
			StringBuffer buffer)
	{
		if (filters == null)
			return;

		for (int i = 0; i < filters.size(); i++)
		{
			Filter filter = (Filter) filters.get(i);
			if (occurString != null)
			{
				buffer.append(occurString);
			}

			buffer.append(filter);

			if (i < filters.size() - 1)
			{
				buffer.append(' ');
			}
		}
	}		
}
