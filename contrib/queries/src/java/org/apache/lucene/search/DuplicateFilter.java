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

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermDocs;
import org.apache.lucene.index.TermEnum;
import org.apache.lucene.util.OpenBitSet;

public class DuplicateFilter extends Filter
{
	
	String fieldName;
	
	/**
	 * KeepMode determines which document id to consider as the master, all others being 
	 * identified as duplicates. Selecting the "first occurrence" can potentially save on IO.
	 */
	int keepMode=KM_USE_FIRST_OCCURRENCE;
	public static final int KM_USE_FIRST_OCCURRENCE=1;
	public static final int KM_USE_LAST_OCCURRENCE=2;
	
	/**
	 * "Full" processing mode starts by setting all bits to false and only setting bits
	 * for documents that contain the given field and are identified as none-duplicates. 

	 * "Fast" processing sets all bits to true then unsets all duplicate docs found for the
	 * given field. This approach avoids the need to read TermDocs for terms that are seen 
	 * to have a document frequency of exactly "1" (i.e. no duplicates). While a potentially 
	 * faster approach , the downside is that bitsets produced will include bits set for 
	 * documents that do not actually contain the field given.
	 * 
	 */
	int processingMode=PM_FULL_VALIDATION;
	public static final int PM_FULL_VALIDATION=1;
	public static final int PM_FAST_INVALIDATION=2;
	

	
	public DuplicateFilter(String fieldName)
	{
		this(fieldName, KM_USE_LAST_OCCURRENCE,PM_FULL_VALIDATION);
	}
	

	public DuplicateFilter(String fieldName, int keepMode, int processingMode)
	{
		this.fieldName = fieldName;
		this.keepMode = keepMode;
		this.processingMode = processingMode;
	}

  public DocIdSet getDocIdSet(IndexReader reader) throws IOException
	{
		if(processingMode==PM_FAST_INVALIDATION)
		{
			return fastBits(reader);
		}
		else
		{
			return correctBits(reader);
		}
	}
	
  private OpenBitSet correctBits(IndexReader reader) throws IOException
	{
		
    OpenBitSet bits=new OpenBitSet(reader.maxDoc()); //assume all are INvalid
		Term startTerm=new Term(fieldName);
		TermEnum te = reader.terms(startTerm);
		if(te!=null)
		{
			Term currTerm=te.term();
			while((currTerm!=null)&&(currTerm.field()==startTerm.field())) //term fieldnames are interned
			{
				int lastDoc=-1;
				//set non duplicates
				TermDocs td = reader.termDocs(currTerm);
				if(td.next())
				{
					if(keepMode==KM_USE_FIRST_OCCURRENCE)
					{
						bits.set(td.doc());
					}
					else
					{
						do
						{
							lastDoc=td.doc();
						}while(td.next());
						bits.set(lastDoc);
					}
				}
				if(!te.next())
				{
					break;
				}
				currTerm=te.term();
			}
		}
		return bits;
	}
	
  private OpenBitSet fastBits(IndexReader reader) throws IOException
	{
		
    OpenBitSet bits=new OpenBitSet(reader.maxDoc());
		bits.set(0,reader.maxDoc()); //assume all are valid
		Term startTerm=new Term(fieldName);
		TermEnum te = reader.terms(startTerm);
		if(te!=null)
		{
			Term currTerm=te.term();
			
			while((currTerm!=null)&&(currTerm.field()==startTerm.field())) //term fieldnames are interned
			{
				if(te.docFreq()>1)
				{
					int lastDoc=-1;
					//unset potential duplicates
					TermDocs td = reader.termDocs(currTerm);
					td.next();
					if(keepMode==KM_USE_FIRST_OCCURRENCE)
					{
						td.next();
					}
					do
					{
						lastDoc=td.doc();
            bits.clear(lastDoc);
					}while(td.next());
					if(keepMode==KM_USE_LAST_OCCURRENCE)
					{
						//restore the last bit
						bits.set(lastDoc);
					}					
				}
				if(!te.next())
				{
					break;
				}
				currTerm=te.term();
			}
		}
		return bits;
	}

	/**
	 * @param args
	 * @throws IOException 
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception
	{
		IndexReader r=IndexReader.open("/indexes/personCentricAnon");
//		IndexReader r=IndexReader.open("/indexes/enron");
		long start=System.currentTimeMillis();
//		DuplicateFilter df = new DuplicateFilter("threadId",KM_USE_FIRST_OCCURRENCE, PM_FAST_INVALIDATION);
//		DuplicateFilter df = new DuplicateFilter("threadId",KM_USE_LAST_OCCURRENCE, PM_FAST_INVALIDATION);
		DuplicateFilter df = new DuplicateFilter("vehicle.vrm",KM_USE_LAST_OCCURRENCE, PM_FAST_INVALIDATION);
//		DuplicateFilter df = new DuplicateFilter("title",USE_LAST_OCCURRENCE);
//		df.setProcessingMode(PM_SLOW_VALIDATION);
		BitSet b = df.bits(r);
		long end=System.currentTimeMillis()-start;
		System.out.println(b.cardinality()+" in "+end+" ms ");

	}


	public String getFieldName()
	{
		return fieldName;
	}


	public void setFieldName(String fieldName)
	{
		this.fieldName = fieldName;
	}


	public int getKeepMode()
	{
		return keepMode;
	}


	public void setKeepMode(int keepMode)
	{
		this.keepMode = keepMode;
	}


	public boolean equals(Object obj)
	{
		if(this == obj)
			return true;
		if((obj == null) || (obj.getClass() != this.getClass()))
			return false;
		DuplicateFilter other = (DuplicateFilter)obj;
		return keepMode == other.keepMode &&
		processingMode == other.processingMode &&
			(fieldName == other.fieldName || (fieldName != null && fieldName.equals(other.fieldName)));
	}



	public int hashCode()
	{
		int hash = 217;
		hash = 31 * hash + keepMode;
		hash = 31 * hash + processingMode;
		hash = 31 * hash + fieldName.hashCode();
		return hash;	
	}


	public int getProcessingMode()
	{
		return processingMode;
	}


	public void setProcessingMode(int processingMode)
	{
		this.processingMode = processingMode;
	}
	
	

}
