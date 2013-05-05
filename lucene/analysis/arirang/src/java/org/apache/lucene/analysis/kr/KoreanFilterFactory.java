package org.apache.lucene.analysis.kr;

/*
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

import java.util.Map;

import org.apache.lucene.analysis.TokenStream;


import org.apache.lucene.analysis.kr.KoreanFilter;
import org.apache.lucene.analysis.util.TokenFilterFactory;

public class KoreanFilterFactory extends TokenFilterFactory {

	private boolean bigrammable = true;
	
	private boolean hasOrigin = true;
	
	private boolean hasCNoun = true;
	
	private boolean exactMatch = false;

  /**
   * Initialize this factory via a set of key-value pairs.
   */
  protected KoreanFilterFactory(Map<String, String> args) {
    super(args);
    init(args);
  }


  public void init(Map<String, String> args) {
//	    bigrammable = getBoolean("bigrammable", true);
//	    hasOrigin = getBoolean("hasOrigin", true);
//	    exactMatch = getBoolean("exactMatch", false);
//	    hasCNoun = getBoolean("hasCNoun", true);
	}
	  
	public TokenStream create(TokenStream tokenstream) {
		return new KoreanFilter(tokenstream, bigrammable, hasOrigin, exactMatch, hasCNoun);
	}

	public void setBigrammable(boolean bool) {
		this.bigrammable = bool;
	}
	
	public void setHasOrigin(boolean bool) {
		this.hasOrigin = bool;
	}
	
	public void setHasCNoun(boolean bool) {
		this.hasCNoun = bool;
	}	
	
	/**
	 * determin whether the original compound noun is returned or not if a input word is analyzed morphically.
//	 * @param has
	 */
	public void setExactMatch(boolean bool) {
		exactMatch = bool;
	}	
}
