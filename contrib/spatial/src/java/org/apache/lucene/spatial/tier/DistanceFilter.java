/** Licensed to the Apache Software Foundation (ASF) under one or more
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

package org.apache.lucene.spatial.tier;

import java.io.IOException;
import java.util.BitSet;
import java.util.Map;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.spatial.ISerialChainFilter;


public abstract class DistanceFilter extends ISerialChainFilter {

	public DistanceFilter() {
		super();
	}

	public abstract Map<Integer,Double> getDistances();

	public abstract Double getDistance(int docid);

	@Override
	public abstract BitSet bits(IndexReader reader) throws IOException;

	@Override
	public abstract BitSet bits(IndexReader reader, BitSet bits) throws Exception;

	/** Returns true if <code>o</code> is equal to this. */
	@Override
	public abstract boolean equals(Object o);

	/** Returns a hash code value for this object.*/
	@Override
	public abstract int hashCode();

	public abstract void setDistances(Map<Integer, Double> distances);

}