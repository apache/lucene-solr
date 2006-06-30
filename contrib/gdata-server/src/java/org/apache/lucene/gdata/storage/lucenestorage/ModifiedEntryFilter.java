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
 
package org.apache.lucene.gdata.storage.lucenestorage; 
 
import java.io.IOException;
import java.util.BitSet;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermDocs;
import org.apache.lucene.search.Filter;
 
/** 
 * The {@link ModifiedEntryFilter} filters the given entryIds from the lucene 
 * {@link org.apache.lucene.search.Hits} set. This filter is used to prevent the 
 * storage from retrieving already deleted or updated entries still remainig in 
 * the {@link org.apache.lucene.gdata.storage.lucenestorage.StorageBuffer}. 
 *  
 * @see org.apache.lucene.search.Filter 
 *  
 * @author Simon Willnauer 
 *  
 */ 
public class ModifiedEntryFilter extends Filter { 
    /** 
     * impl Serializable 
     */ 
    private static final long serialVersionUID = -1551686287704213591L; 
 
    private final String[] entyIds; 
 
    /** 
     * Creates a new {@link ModifiedEntryFilter} 
     * @param entryIds the entry id's to filter  
     *  
     */ 
    public ModifiedEntryFilter(final String[] entryIds) { 
        super(); 
        this.entyIds = entryIds; 
    } 
 
    /** 
     * @see org.apache.lucene.search.Filter#bits(org.apache.lucene.index.IndexReader) 
     */ 
    @Override 
    public BitSet bits(IndexReader reader) throws IOException { 
        BitSet bitSet = new BitSet(reader.maxDoc()); 
        bitSet.flip(0, reader.maxDoc()); // set all docs  
        int[] docs = new int[1]; 
        int[] freq = new int[1]; 
        for (String id : this.entyIds) { 
            if (id != null) { 
                TermDocs termDocs = reader.termDocs(new Term( 
                        StorageEntryWrapper.FIELD_ENTRY_ID, id)); 
                int count = termDocs.read(docs, freq); 
                if (count == 1) 
                    bitSet.flip(docs[0]); 
 
            } 
        } 
 
        return bitSet; 
    } 
 
} 
