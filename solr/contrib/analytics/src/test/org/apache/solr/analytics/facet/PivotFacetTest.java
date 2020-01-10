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
package org.apache.solr.analytics.facet;

import java.util.ArrayList;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;

public class PivotFacetTest extends SolrAnalyticsFacetTestCase {

  @BeforeClass
  public static void populate() throws Exception {
    for (int j = 0; j < NUM_LOOPS; ++j) {
      int i = j%INT;
      long l = j%LONG;
      float f = j%FLOAT;
      double d = j%DOUBLE;
      String dt = (1800+j%DATE) + "-12-31T23:59:59Z";
      String dtm = (1800+j%DATE + 10) + "-12-31T23:59:59Z";
      String s = "str" + (j%STRING);
      List<String> fields = new ArrayList<>();
      fields.add("id"); fields.add("1000"+j);

      if ( i != 0 ) {
        fields.add("int_i"); fields.add("" + i);
        fields.add("int_im"); fields.add("" + i);
        fields.add("int_im"); fields.add("" + (i+10));
      }

      if ( l != 0l ) {
        fields.add("long_l"); fields.add("" + l);
        fields.add("long_lm"); fields.add("" + l);
        fields.add("long_lm"); fields.add("" + (l+10));
      }

      if ( f != 0.0f ) {
        fields.add("float_f"); fields.add("" + f);
        fields.add("float_fm"); fields.add("" + f);
        fields.add("float_fm"); fields.add("" + (f+10));
      }

      if ( d != 0.0d ) {
        fields.add("double_d"); fields.add("" + d);
        fields.add("double_dm"); fields.add("" + d);
        fields.add("double_dm"); fields.add("" + (d+10));
      }

      if ( (j%DATE) != 0 ) {
        fields.add("date_dt"); fields.add(dt);
        fields.add("date_dtm"); fields.add(dt);
        fields.add("date_dtm"); fields.add(dtm);
      }

      if ( (j%STRING) != 0 ) {
        fields.add("string_s"); fields.add(s);
        fields.add("string_sm"); fields.add(s);
        fields.add("string_sm"); fields.add(s + "_second");
      }

      addDoc(fields);
    }
    commitDocs();
  }

  static public final int INT = 7;
  static public final int LONG = 2;
  static public final int FLOAT = 6;
  static public final int DOUBLE = 5;
  static public final int DATE = 3;
  static public final int STRING = 4;
  static public final int NUM_LOOPS = 20;

  @Test
  public void pivotFacetTest() throws Exception {
    String analyticsRequest = "{"
        + "\n 'groupings': { "
        + "\n   'grouping' : { "
        + "\n     'expressions' : { "
        + "\n       'mean' : 'mean(fill_missing(int_i,0))', "
        + "\n       'count' : 'count(long_lm)' "
        + "\n     }, "
        + "\n     'facets' : { "
        + "\n       'pivoting' : { "
        + "\n         'type' : 'pivot', "
        + "\n         'pivots' : [ "
        + "\n           { "
        + "\n             'name' : 'strings', "
        + "\n             'expression' : 'string_sm', "
        + "\n             'sort' : { "
        + "\n               'criteria' : [ "
        + "\n                 { "
        + "\n                   'type' : 'expression', "
        + "\n                   'expression' : 'mean', "
        + "\n                   'direction' : 'ascending' "
        + "\n                 }, "
        + "\n                 { "
        + "\n                   'type' : 'facetvalue', "
        + "\n                   'direction' : 'descending' "
        + "\n                 } "
        + "\n               ], "
        + "\n               'limit' : 3, "
        + "\n               'offset' : 1 "
        + "\n             } "
        + "\n           }, "
        + "\n           { "
        + "\n             'name' : 'date', "
        + "\n             'expression' : 'fill_missing(date_dt, \\'No Date\\')', "
        + "\n             'sort' : { "
        + "\n               'criteria' : [ "
        + "\n                 { "
        + "\n                   'type' : 'expression', "
        + "\n                   'expression' : 'count', "
        + "\n                   'direction' : 'ascending' "
        + "\n                 }, "
        + "\n                 { "
        + "\n                   'type' : 'expression', "
        + "\n                   'expression' : 'mean', "
        + "\n                   'direction' : 'ascending' "
        + "\n                 } "
        + "\n               ], "
        + "\n               'limit' : 2 "
        + "\n             } "
        + "\n           } "
        + "\n         ] "
        + "\n       } "
        + "\n     } "
        + "\n   } "
        + "\n } "
        + "\n} ";

    String test = "groupings=={'grouping':{'pivoting':["
        + "\n { "
        + "\n   'pivot' : 'strings', "
        + "\n   'value' : 'str3', "
        + "\n   'results' : { "
        + "\n     'mean' : 2.6, "
        + "\n     'count' : 10 "
        + "\n   }, "
        + "\n   'children' : [ "
        + "\n     { "
        + "\n       'pivot' : 'date', "
        + "\n       'value' : '1802-12-31T23:59:59Z', "
        + "\n       'results' : { "
        + "\n         'mean' : 4.0, "
        + "\n         'count' : 2 "
        + "\n       } "
        + "\n     }, "
        + "\n     { "
        + "\n       'pivot' : 'date', "
        + "\n       'value' : 'No Date', "
        + "\n       'results' : { "
        + "\n         'mean' : 2.0, "
        + "\n         'count' : 4 "
        + "\n       } "
        + "\n     } "
        + "\n   ]"
        + "\n }, "
        + "\n { "
        + "\n   'pivot' : 'strings', "
        + "\n   'value' : 'str2_second', "
        + "\n   'results' : { "
        + "\n     'mean' : 3.0, "
        + "\n     'count' : 0 "
        + "\n   }, "
        + "\n   'children' : [ "
        + "\n     { "
        + "\n       'pivot' : 'date', "
        + "\n       'value' : '1802-12-31T23:59:59Z', "
        + "\n       'results' : { "
        + "\n         'mean' : 1.0, "
        + "\n         'count' : 0 "
        + "\n       } "
        + "\n     }, "
        + "\n     { "
        + "\n       'pivot' : 'date', "
        + "\n       'value' : '1801-12-31T23:59:59Z', "
        + "\n       'results' : { "
        + "\n         'mean' : 3.0, "
        + "\n         'count' : 0 "
        + "\n       } "
        + "\n     } "
        + "\n   ]"
        + "\n }, "
        + "\n { "
        + "\n   'pivot' : 'strings', "
        + "\n   'value' : 'str2', "
        + "\n   'results' : { "
        + "\n     'mean' : 3.0, "
        + "\n     'count' : 0 "
        + "\n   }, "
        + "\n   'children' : [ "
        + "\n     { "
        + "\n       'pivot' : 'date', "
        + "\n       'value' : '1802-12-31T23:59:59Z', "
        + "\n       'results' : { "
        + "\n         'mean' : 1.0, "
        + "\n         'count' : 0 "
        + "\n       } "
        + "\n     }, "
        + "\n     { "
        + "\n       'pivot' : 'date', "
        + "\n       'value' : '1801-12-31T23:59:59Z', "
        + "\n       'results' : { "
        + "\n         'mean' : 3.0, "
        + "\n         'count' : 0 "
        + "\n       } "
        + "\n     } "
        + "\n   ]"
        + "\n } "
        + "\n]}}";

    testAnalytics(analyticsRequest, test);
  }
}
