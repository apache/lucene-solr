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


public class TestUpdatingTable extends TestCase {
    private BaseTableModel baseTableModel;
    private TableSearcher tableSearcher;

    RestaurantInfo infoToAdd1, infoToAdd2;

    protected void setUp() throws Exception {
        baseTableModel = new BaseTableModel(DataStore.getRestaurants());
        tableSearcher = new TableSearcher(baseTableModel);

        infoToAdd1 = new RestaurantInfo();
        infoToAdd1.setName("Pino's");
        infoToAdd1.setType("Italian");

        infoToAdd2 = new RestaurantInfo();
        infoToAdd2.setName("Pino's");
        infoToAdd2.setType("Italian");
    }

    public void testAddWithoutSearch(){
        assertEquals(baseTableModel.getRowCount(), tableSearcher.getRowCount());
        int count = tableSearcher.getRowCount();
        baseTableModel.addRow(infoToAdd1);
        count++;
        assertEquals(count, tableSearcher.getRowCount());
    }

    public void testRemoveWithoutSearch(){
        assertEquals(baseTableModel.getRowCount(), tableSearcher.getRowCount());
        int count = tableSearcher.getRowCount();
        baseTableModel.addRow(infoToAdd1);
        baseTableModel.removeRow(infoToAdd1);
        assertEquals(count, tableSearcher.getRowCount());
    }

    public void testAddWithSearch(){
        assertEquals(baseTableModel.getRowCount(), tableSearcher.getRowCount());
        tableSearcher.search("pino's");
        int count = tableSearcher.getRowCount();
        baseTableModel.addRow(infoToAdd2);
        count++;
        assertEquals(count, tableSearcher.getRowCount());
    }

    public void testRemoveWithSearch(){
        assertEquals(baseTableModel.getRowCount(), tableSearcher.getRowCount());
        baseTableModel.addRow(infoToAdd1);
        tableSearcher.search("pino's");
        int count = tableSearcher.getRowCount();
        baseTableModel.removeRow(infoToAdd1);
        count--;
        assertEquals(count, tableSearcher.getRowCount());
    }


}
