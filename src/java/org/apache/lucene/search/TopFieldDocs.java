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
 * Expert: Returned by low-level sorted search implementations.
 *
 * <p>Created: Feb 12, 2004 8:58:46 AM 
 * 
 * @author  Tim Jones (Nacimiento Software)
 * @since   lucene 1.4
 * @version $Id$
 * @see Searcher#search(Query,Filter,int,Sort)
 */
public class TopFieldDocs
extends TopDocs {

	/** The fields which were used to sort results by. */
	public SortField[] fields;
        
	/** Creates one of these objects.
	 * @param totalHits  Total number of hits for the query.
	 * @param scoreDocs  The top hits for the query.
	 * @param fields     The sort criteria used to find the top hits.
	 * @param maxScore   The maximum score encountered.
	 */
	TopFieldDocs (int totalHits, ScoreDoc[] scoreDocs, SortField[] fields, float maxScore) {
	  super (totalHits, scoreDocs, maxScore);
	  this.fields = fields;
	}
}