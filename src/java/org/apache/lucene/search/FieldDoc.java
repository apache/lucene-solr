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


/**
 * Expert: A ScoreDoc which also contains information about
 * how to sort the referenced document.
 *
 * <p>Created: Feb 11, 2004 1:23:38 PM 
 * 
 * @author  Tim Jones (Nacimiento Software)
 * @since   lucene 1.4
 * @version $Id$
 * @see TopFieldDocs
 */
public class FieldDoc
extends ScoreDoc {

	/** The values which are used to sort the referenced document.
	 * The order of these will match the original sort criteria given by an
	 * Sort object.
	 * @see Sort
	 * @see Searchable#search(Query,Filter,int,Sort)
	 */
	public Object[] fields;

	/** Creates one of these objects with empty sort information. */
	public FieldDoc (int doc, float score) {
		super (doc, score);
	}

	/** Creates one of these objects with the given sort information. */
	public FieldDoc (int doc, float score, Object[] fields) {
		super (doc, score);
		this.fields = fields;
	}
}
