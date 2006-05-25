package org.apache.lucene.misc;

/**
  * Copyright 2006 The Apache Software Foundation
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

import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermEnum;
import org.apache.lucene.index.TermDocs;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Similarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.io.File;
import java.io.IOException;
import java.util.Date;

/**
 * Given a directory, a Similarity, and a list of fields, updates the
 * fieldNorms in place for every document using the Similarity.lengthNorm.
 *
 * <p>
 * NOTE: This only works if you do <b>not</b> use field/document boosts in your
 * index.
 * </p>
 *
 * @version $Id:$
 */
public class LengthNormModifier {

    /**
     * Command Line Execution method
     *
     * <pre>
     * Usage: LengthNormModifier /path/index package.SimilarityClassName field1 field2 ...
     * </pre>
     */
    public static void main(String[] args) throws IOException {
	if (args.length < 3) {
	    System.err.println("Usage: LengthNormModifier <index> <package.SimilarityClassName> <field1> [field2] ...");
	    System.exit(1);
	}

	Similarity s = null;
	try {
	    Class simClass = Class.forName(args[1]);
	    s = (Similarity)simClass.newInstance();
	} catch (Exception e) {
	    System.err.println("Couldn't instantiate similarity with empty constructor: " + args[1]);
	    e.printStackTrace(System.err);
	}
	
	File index = new File(args[0]);
	Directory d = FSDirectory.getDirectory(index,false);

	LengthNormModifier lnm = new LengthNormModifier(d, s);
	
	for (int i = 2; i < args.length; i++) {
	    System.out.print("Updating field: " + args[i] + " " +
			     (new Date()).toString() + " ... ");
	    lnm.reSetNorms(args[i]);
	    System.out.println(new Date().toString());
	}
	
	d.close();
    }


    private Directory dir;
    private Similarity sim;
    
    /**
     * Constructor for code that wishes to use this class progromaticaly
     *
     * @param d The Directory to modify
     * @param s The Similarity to use in <code>reSetNorms</code>
     */
    public LengthNormModifier(Directory d, Similarity s) {
	dir = d;
	sim = s;
    }

    /**
     * Re-Set the norms for the specified field.
     *
     * <p>
     * Opens a new IndexReader on the Directory given to this instance,
     * modifies the norms using the Similarity given to this instance,
     * and closes the IndexReader.
     * </p>
     *
     * @param field the field whose norms
     */
    public void reSetNorms(String field) throws IOException {
	String fieldName = field.intern();
	int[] termCounts = new int[0];

	IndexReader reader = null;
	TermEnum termEnum = null;
	TermDocs termDocs = null;
	try {
	    reader = IndexReader.open(dir);
	    termCounts = new int[reader.maxDoc()];
	    try {
		termEnum = reader.terms(new Term(field,""));
		try {
		    termDocs = reader.termDocs();
		    do {
			Term term = termEnum.term();
			if (term != null && term.field().equals(fieldName)) {
			    termDocs.seek(termEnum.term());
			    while (termDocs.next()) {
				termCounts[termDocs.doc()] += termDocs.freq();
			    }
			}
		    } while (termEnum.next());
	    
		} finally {
		    if (null != termDocs) termDocs.close();
		}
	    } finally {
		if (null != termEnum) termEnum.close();
	    }
	} finally {
	    if (null != reader) reader.close();
	}

	try {
	    reader = IndexReader.open(dir); 
	    for (int d = 0; d < termCounts.length; d++) {
		if (! reader.isDeleted(d)) {
		    byte norm = sim.encodeNorm
			(sim.lengthNorm(fieldName, termCounts[d]));
		    reader.setNorm(d, fieldName, norm);
		}
	    }
	    
	} finally {
	    if (null != reader) reader.close();
	}
    }
    
}
