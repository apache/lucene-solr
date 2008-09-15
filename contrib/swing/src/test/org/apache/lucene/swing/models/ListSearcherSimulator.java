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

import java.awt.BorderLayout;

import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JList;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTextField;
import javax.swing.event.DocumentEvent;
import javax.swing.event.DocumentListener;


public class ListSearcherSimulator {

    public ListSearcherSimulator() {
        JFrame frame = new JFrame();
        frame.setBounds(200,200, 400,250);

        JList list = new JList();
        JScrollPane scrollPane = new JScrollPane(list);

        final BaseListModel listModel = new BaseListModel(DataStore.getRestaurants());
        final ListSearcher listSearcher = new ListSearcher(listModel);

        list.setModel(listSearcher);

        final JTextField searchField = new JTextField();
        searchField.getDocument().addDocumentListener(
                new DocumentListener(){
                    public void changedUpdate(DocumentEvent e) {
                        listSearcher.search(searchField.getText().trim().toLowerCase());
                    }

                    public void insertUpdate(DocumentEvent e) {
                        listSearcher.search(searchField.getText().trim().toLowerCase());
                    }

                    public void removeUpdate(DocumentEvent e) {
                        listSearcher.search(searchField.getText().trim().toLowerCase());
                    }
                }
        );

        frame.getContentPane().setLayout(new BorderLayout());
        frame.getContentPane().add(scrollPane, BorderLayout.CENTER);

        JPanel searchPanel = new JPanel();
        searchPanel.setLayout(new BorderLayout(10,10));
        searchPanel.add(searchField, BorderLayout.CENTER);
        searchPanel.add(new JLabel("Search: "), BorderLayout.WEST);

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
        new ListSearcherSimulator();
    }

}
