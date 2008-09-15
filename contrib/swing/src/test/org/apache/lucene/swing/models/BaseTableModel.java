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

import javax.swing.table.AbstractTableModel;


public class BaseTableModel extends AbstractTableModel {
    private List columnNames = new ArrayList();
    private List rows = new ArrayList();

    public BaseTableModel(Iterator data) {
        columnNames.add("Name");
        columnNames.add("Type");
        columnNames.add("Phone");
        columnNames.add("Street");
        columnNames.add("City");
        columnNames.add("State");
        columnNames.add("Zip");

        while (data.hasNext()) {
            Object nextRow = (Object) data.next();
            rows.add(nextRow);
        }
    }

    public int getColumnCount() {
        return columnNames.size();
    }

    public int getRowCount() {
        return rows.size();
    }

    public void addRow(RestaurantInfo info){
        rows.add(info);
        fireTableDataChanged();
    }

    public void removeRow(RestaurantInfo info){
        rows.remove(info);
        fireTableDataChanged();
    }

    public boolean isCellEditable(int rowIndex, int columnIndex) {
        return false;
    }

    public Class getColumnClass(int columnIndex) {
        return String.class;
    }

    public Object getValueAt(int rowIndex, int columnIndex) {
        RestaurantInfo restaurantInfo = (RestaurantInfo) rows.get(rowIndex);
        if (columnIndex == 0){ // name
            return restaurantInfo.getName();
        } else if (columnIndex == 1){ // category
            return restaurantInfo.getType();
        } else if (columnIndex == 2){ // phone
            return restaurantInfo.getPhone();
        } else if (columnIndex == 3){ // street
            return restaurantInfo.getStreet();
        } else if (columnIndex == 4){ // city
            return restaurantInfo.getCity();
        } else if (columnIndex == 5){ // state
            return restaurantInfo.getState();
        } else if (columnIndex == 6){ // zip
            return restaurantInfo.getZip();
        } else {
            return "";
        }
    }

    public void setValueAt(Object aValue, int rowIndex, int columnIndex) {
        //no op
    }

    public String getColumnName(int columnIndex) {
        return columnNames.get(columnIndex).toString();
    }

}
