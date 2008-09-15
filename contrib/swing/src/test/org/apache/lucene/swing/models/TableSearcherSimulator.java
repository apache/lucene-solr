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

import javax.swing.*;
import java.awt.*;
import java.awt.event.ActionListener;
import java.awt.event.ActionEvent;


public class TableSearcherSimulator {

    public TableSearcherSimulator() {
        JFrame frame = new JFrame();
        frame.setBounds(200,200, 400,250);

        JTable table = new JTable();
        final BaseTableModel tableModel = new BaseTableModel(DataStore.getRestaurants());
        final TableSearcher searchTableModel = new TableSearcher(tableModel);

        table.setModel(searchTableModel);
        JScrollPane scrollPane = new JScrollPane(table);

        final JTextField searchField = new JTextField();
        JButton searchButton = new JButton("Go");

        ActionListener searchListener = new ActionListener() {
            public void actionPerformed(ActionEvent e) {
               searchTableModel.search(searchField.getText().trim().toLowerCase());
                searchField.requestFocus();
            }
        };

        searchButton.addActionListener(searchListener);
        searchField.addActionListener(searchListener);



        frame.getContentPane().setLayout(new BorderLayout());
        frame.getContentPane().add(scrollPane, BorderLayout.CENTER);

        JPanel searchPanel = new JPanel();
        searchPanel.setLayout(new BorderLayout(10,10));
        searchPanel.add(searchField, BorderLayout.CENTER);
        searchPanel.add(searchButton, BorderLayout.EAST);

        JPanel topPanel = new JPanel(new BorderLayout());
        topPanel.add(searchPanel, BorderLayout.CENTER);
        topPanel.add(new JPanel(), BorderLayout.EAST);
        topPanel.add(new JPanel(), BorderLayout.WEST);
        topPanel.add(new JPanel(), BorderLayout.NORTH);
        topPanel.add(new JPanel(), BorderLayout.SOUTH);

        frame.getContentPane().add(topPanel, BorderLayout.NORTH);

        frame.setTitle("Lucene powered table searching");
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        frame.show();

    }


    public static void main(String[] args) {
        new TableSearcherSimulator();
    }

}
