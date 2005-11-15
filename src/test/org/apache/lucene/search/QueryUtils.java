package org.apache.lucene.search;

import junit.framework.TestCase;

/**
 * Copyright 2005 Apache Software Foundation
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
 * @author yonik
 */
public class QueryUtils {

  /** Check the types of things query objects should be able to do. */
  public static void check(Query q) {
    checkHashEquals(q);
  }

  /** check very basic hashCode and equals */
  public static void checkHashEquals(Query q) {
    Query q2 = (Query)q.clone();
    checkEqual(q,q2);

    Query q3 = (Query)q.clone();
    q3.setBoost(7.21792348f);
    checkUnequal(q,q3);

    // test that a class check is done so that no exception is thrown
    // in the implementation of equals()
    Query whacky = new Query() {
      public String toString(String field) {
        return "My Whacky Query";
      }
    };
    whacky.setBoost(q.getBoost());
    checkUnequal(q, whacky);
  }

  public static void checkEqual(Query q1, Query q2) {
    TestCase.assertEquals(q1, q2);
    TestCase.assertEquals(q1.hashCode(), q2.hashCode());
  }

  public static void checkUnequal(Query q1, Query q2) {
    TestCase.assertTrue(!q1.equals(q2));
    TestCase.assertTrue(!q2.equals(q1));

    // possible this test can fail on a hash collision... if that
    // happens, please change test to use a different example.
    TestCase.assertTrue(q1.hashCode() != q2.hashCode());
  }

}
