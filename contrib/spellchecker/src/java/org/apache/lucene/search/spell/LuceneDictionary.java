package org.apache.lucene.search.spell;

/**
 * Copyright 2002-2004 The Apache Software Foundation
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

import org.apache.lucene.index.IndexReader;
import java.util.Iterator;
import org.apache.lucene.index.TermEnum;
import org.apache.lucene.index.Term;
import java.io.*;

/**
 *  Lucene Dictionnary
 * @author Nicolas Maisonneuve
 */
public class LuceneDictionary
implements Dictionary {
    IndexReader reader;
    String field;

    public LuceneDictionary (IndexReader reader, String field) {
        this.reader=reader;
        this.field=field;

    }


    public final Iterator getWordsIterator () {
        return new LuceneIterator();
    }


final  class LuceneIterator    implements Iterator {
      private  TermEnum enum;
      private  Term actualTerm;
      private  boolean has_next_called;

        public LuceneIterator () {
            try {
                enum=reader.terms(new Term(field, ""));
            }
            catch (IOException ex) {
                ex.printStackTrace();
            }
        }


        public Object next () {
            if (!has_next_called)  {hasNext();}
             has_next_called=false;
            return (actualTerm!=null) ? actualTerm.text(): null;
        }


        public boolean hasNext () {
             has_next_called=true;
            try {
                // if there is still words
                if (!enum.next()) {
                    actualTerm=null;
                    return false;
                }
                //  if the next word are in the field
                actualTerm=enum.term();
                String fieldt=actualTerm.field();
                if (fieldt!=field) {
                    actualTerm=null;
                    return false;
                }
                return true;
            }
            catch (IOException ex) {
                ex.printStackTrace();
                return false;
            }
        }


        public void remove () {};
    }
}
