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
package org.apache.solr.search.function;
import org.apache.lucene.util.LuceneTestCase.SuppressCodecs;



/**
 * Split out from SortByFunctionTest due to codec support limitations for SortedSetSelector
 *
 * @see SortByFunctionTest
 **/
@SuppressCodecs({"SimpleText"}) // see TestSortedSetSelector
public class TestSortByMinMaxFunction extends SortByFunctionTest {

  @Override
  public String[] getFieldFunctionClausesToTest() {
    return new String[] { "primary_tl1", "field(primary_tl1)",
                          "field(multi_l_dv,max)", "field(multi_l_dv,min)" };
  }
}
