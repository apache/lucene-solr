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

import java.io.IOException;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermEnum;

/** Abstract class for enumerating a subset of all terms. 

  <p>Term enumerations are always ordered by Term.compareTo().  Each term in
  the enumeration is greater than all that precede it.  */
public abstract class FilteredTermEnum extends TermEnum {
    private Term currentTerm = null;
    private TermEnum actualEnum = null;
    
    public FilteredTermEnum() {}

    /** Equality compare on the term */
    protected abstract boolean termCompare(Term term);
    
    /** Equality measure on the term */
    public abstract float difference();

    /** Indicates the end of the enumeration has been reached */
    protected abstract boolean endEnum();
    
    protected void setEnum(TermEnum actualEnum) throws IOException {
        this.actualEnum = actualEnum;
        // Find the first term that matches
        Term term = actualEnum.term();
        if (term != null && termCompare(term)) 
            currentTerm = term;
        else next();
    }
    
    /** 
     * Returns the docFreq of the current Term in the enumeration.
     * Returns -1 if no Term matches or all terms have been enumerated.
     */
    public int docFreq() {
        if (actualEnum == null) return -1;
        return actualEnum.docFreq();
    }
    
    /** Increments the enumeration to the next element.  True if one exists. */
    public boolean next() throws IOException {
        if (actualEnum == null) return false; // the actual enumerator is not initialized!
        currentTerm = null;
        while (currentTerm == null) {
            if (endEnum()) return false;
            if (actualEnum.next()) {
                Term term = actualEnum.term();
                if (termCompare(term)) {
                    currentTerm = term;
                    return true;
                }
            }
            else return false;
        }
        currentTerm = null;
        return false;
    }
    
    /** Returns the current Term in the enumeration.
     * Returns null if no Term matches or all terms have been enumerated. */
    public Term term() {
        return currentTerm;
    }
    
    /** Closes the enumeration to further activity, freeing resources.  */
    public void close() throws IOException {
        actualEnum.close();
        currentTerm = null;
        actualEnum = null;
    }
}
