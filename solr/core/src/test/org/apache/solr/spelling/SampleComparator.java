package org.apache.solr.spelling;
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

import org.apache.lucene.search.spell.SuggestWord;

import java.util.Comparator;


/**
 * Comparator for testing purposes
 *
 **/
public class SampleComparator implements Comparator<SuggestWord> {


  @Override
  public int compare(SuggestWord suggestWord, SuggestWord suggestWord1) {
    return suggestWord.string.compareTo(suggestWord1.string);
  }

}
