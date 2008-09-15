package org.apache.lucene.swing.models;

/**
 * Copyright 2005 The Apache Software Foundation
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

import javax.swing.ListModel;

import junit.framework.TestCase;


 
public class TestSearchingList extends TestCase {
    private ListModel baseListModel;
    private ListSearcher listSearcher;

    protected void setUp() throws Exception {
        baseListModel = new BaseListModel(DataStore.getRestaurants());
        listSearcher = new ListSearcher(baseListModel);
    }

    public void testSearch(){
        //make sure data is there
        assertEquals(baseListModel.getSize(), listSearcher.getSize());
        //search for pino's
        listSearcher.search("pino's");
        assertEquals(1, listSearcher.getSize());
        //clear search and check that
        listSearcher.search(null);
        assertEquals(baseListModel.getSize(), listSearcher.getSize());
    }

}
