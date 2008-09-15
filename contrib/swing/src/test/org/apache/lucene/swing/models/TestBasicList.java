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
import java.util.List;

import javax.swing.ListModel;

import junit.framework.TestCase;

/**
 **/
public class TestBasicList extends TestCase {
    private ListModel baseListModel;
    private ListSearcher listSearcher;
    private List list;

    protected void setUp() throws Exception {
        list = new ArrayList();
        list.add(DataStore.canolis);
        list.add(DataStore.chris);

        baseListModel = new BaseListModel(list.iterator());
        listSearcher = new ListSearcher(baseListModel);
    }


    public void testRows(){
        assertEquals(list.size(), listSearcher.getSize());
    }

    public void testValueAt(){
        assertEquals(baseListModel.getElementAt(0), listSearcher.getElementAt(0));
        assertNotSame(baseListModel.getElementAt(1), listSearcher.getElementAt(0));
    }

}
