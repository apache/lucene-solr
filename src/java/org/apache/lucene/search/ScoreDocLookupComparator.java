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
 * Expert: Compares two ScoreDoc objects for sorting using a lookup table.
 *
 * <p>Created: Feb 3, 2004 9:59:14 AM 
 * 
 * @author  Tim Jones (Nacimiento Software)
 * @since   lucene 1.4
 * @version $Id$
 */
public interface ScoreDocLookupComparator
extends ScoreDocComparator {

	/**
	 * Verifies that the internal lookup table is the correct size.  This
	 * comparator uses a lookup table, so it is important to that the
	 * table matches the number of documents in the index.
	 * @param n  Expected size of table.
	 * @return   True if internal table matches expected size; false otherwise
	 */
	boolean sizeMatches (int n);
}