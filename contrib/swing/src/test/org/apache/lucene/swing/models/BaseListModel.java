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
import java.util.Iterator;
import java.util.List;

import javax.swing.AbstractListModel;


public class BaseListModel extends AbstractListModel {
    private List data = new ArrayList();

    public BaseListModel(Iterator iterator) {
        while (iterator.hasNext()) {
            data.add(iterator.next());
        }
    }

    public int getSize() {
        return data.size();
    }

    public Object getElementAt(int index) {
        return data.get(index);
    }

    public void addRow(Object toAdd) {
        data.add(toAdd);
        fireContentsChanged(this, 0, getSize());
    }

    public void removeRow(Object toRemove) {
        data.remove(toRemove);
        fireContentsChanged(this, 0, getSize());
    }



}
