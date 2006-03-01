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

import junit.framework.TestCase;

/**
 * @author Jonathan Simon - jonathan_s_simon@yahoo.com
 */
public class TestUpdatingList extends TestCase {
    private BaseListModel baseListModel;
    private ListSearcher listSearcher;

    RestaurantInfo infoToAdd1, infoToAdd2;

    protected void setUp() throws Exception {
        baseListModel = new BaseListModel(DataStore.getRestaurants());
        listSearcher = new ListSearcher(baseListModel);

        infoToAdd1 = new RestaurantInfo();
        infoToAdd1.setName("Pino's");

        infoToAdd2 = new RestaurantInfo();
        infoToAdd2.setName("Pino's");
        infoToAdd2.setType("Italian");
    }

    public void testAddWithoutSearch(){
        assertEquals(baseListModel.getSize(), listSearcher.getSize());
        int count = listSearcher.getSize();
        baseListModel.addRow(infoToAdd1);
        count++;
        assertEquals(count, listSearcher.getSize());
    }

    public void testRemoveWithoutSearch(){
        assertEquals(baseListModel.getSize(), listSearcher.getSize());
        baseListModel.addRow(infoToAdd1);
        int count = listSearcher.getSize();
        baseListModel.removeRow(infoToAdd1);
        count--;
        assertEquals(count, listSearcher.getSize());
    }

    public void testAddWithSearch(){
        assertEquals(baseListModel.getSize(), listSearcher.getSize());
        listSearcher.search("pino's");
        int count = listSearcher.getSize();
        baseListModel.addRow(infoToAdd2);
        count++;
        assertEquals(count, listSearcher.getSize());
    }

    public void testRemoveWithSearch(){
        assertEquals(baseListModel.getSize(), listSearcher.getSize());
        baseListModel.addRow(infoToAdd1);
        listSearcher.search("pino's");
        int count = listSearcher.getSize();
        baseListModel.removeRow(infoToAdd1);
        count--;
        assertEquals(count, listSearcher.getSize());
    }


}
