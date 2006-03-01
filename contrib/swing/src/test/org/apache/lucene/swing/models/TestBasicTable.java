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

import java.util.ArrayList;

import javax.swing.table.TableModel;

import junit.framework.TestCase;

/**
 * @author Jonathan Simon - jonathan_s_simon@yahoo.com
 */
public class TestBasicTable extends TestCase {
    private TableModel baseTableModel;
    private TableSearcher tableSearcher;
    private ArrayList list;

    protected void setUp() throws Exception {
        list = new ArrayList();
        list.add(DataStore.canolis);
        list.add(DataStore.chris);

        baseTableModel = new BaseTableModel(list.iterator());
        tableSearcher = new TableSearcher(baseTableModel);
    }

    public void testColumns(){

        assertEquals(baseTableModel.getColumnCount(), tableSearcher.getColumnCount());
        assertEquals(baseTableModel.getColumnName(0), tableSearcher.getColumnName(0));
        assertNotSame(baseTableModel.getColumnName(0), tableSearcher.getColumnName(1));
        assertEquals(baseTableModel.getColumnClass(0), tableSearcher.getColumnClass(0));
    }

    public void testRows(){
        assertEquals(list.size(), tableSearcher.getRowCount());
    }

    public void testValueAt(){
        assertEquals(baseTableModel.getValueAt(0,0), tableSearcher.getValueAt(0,0));
        assertEquals(baseTableModel.getValueAt(0,3), tableSearcher.getValueAt(0,3));
    }

}
