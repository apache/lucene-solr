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
package org.apache.solr.search;

import org.junit.BeforeClass;
import org.junit.Test;
import java.util.Random;

public class TestReload extends TestRTGBase {

  @BeforeClass
  public static void beforeClass() throws Exception {
    // useFactory(null);   // force FS directory
    initCore("solrconfig-tlog.xml","schema15.xml");
  }

  @Test
  public void testGetRealtimeReload() throws Exception {
    clearIndex();
    assertU(commit());
    long version = addAndGetVersion(sdoc("id","1") , null);

    assertU(commit("softCommit","true"));   // should cause a RTG searcher to be opened

    assertJQ(req("qt","/get","id","1", "fl", "id,_version_")
        ,"=={'doc':{'id':'1','_version_':" + version + "}}"
    );

    h.reload();

    assertJQ(req("qt","/get","id","1", "fl", "id,_version_")
        ,"=={'doc':{'id':'1','_version_':" + version + "}}"
    );

    assertU(commit("softCommit","true"));   // open a normal (caching) NRT searcher

    assertJQ(req("q","id:1")
        ,"/response/numFound==1"
    );


    Random rand = random();
    int iter = atLeast(20);

    for (int i=0; i<iter; i++) {
      if (rand.nextBoolean()) {
        // System.out.println("!!! add");
        version = addAndGetVersion(sdoc("id","1") , null);
      }

      if (rand.nextBoolean()) {
        if (rand.nextBoolean()) {
          // System.out.println("!!! flush");
          assertU(commit("openSearcher","false"));   // should cause a RTG searcher to be opened as well
        } else {
          boolean softCommit = rand.nextBoolean();
          System.out.println("!!! softCommit" + softCommit);
          // assertU(commit("softCommit", ""+softCommit));
        }
      }

      if (rand.nextBoolean()) {
        // RTG should always be able to see the last version
        // System.out.println("!!! rtg");
        assertJQ(req("qt","/get","id","1", "fl", "id,_version_")
            ,"=={'doc':{'id':'1','_version_':" + version + "}}"
        );
      }

      if (rand.nextBoolean()) {
        // a normal search should always find 1 doc
        // System.out.println("!!! q");
        assertJQ(req("q","id:1")
            ,"/response/numFound==1"
        );
      }

      if (rand.nextBoolean()) {
        // System.out.println("!!! reload");
        h.reload();
      }
    }

    // test framework should ensure that all searchers opened have been closed.
  }

}
