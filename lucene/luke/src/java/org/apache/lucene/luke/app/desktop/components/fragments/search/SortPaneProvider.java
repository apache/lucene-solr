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

package org.apache.lucene.luke.app.desktop.components.fragments.search;

import javax.swing.BorderFactory;
import javax.swing.JButton;
import javax.swing.JComboBox;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.GridLayout;
import java.awt.event.ActionEvent;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.lucene.luke.app.desktop.components.ComponentOperatorRegistry;
import org.apache.lucene.luke.app.desktop.components.SearchTabOperator;
import org.apache.lucene.luke.app.desktop.util.MessageUtils;
import org.apache.lucene.luke.app.desktop.util.StringUtils;
import org.apache.lucene.luke.models.search.Search;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSortField;

/** Provider of the Sort pane */
public final class SortPaneProvider implements SortTabOperator {

  private static final String COMMAND_FIELD_COMBO1 = "fieldCombo1";

  private static final String COMMAND_FIELD_COMBO2 = "fieldCombo2";

  private final JComboBox<String> fieldCombo1 = new JComboBox<>();

  private final JComboBox<String> typeCombo1 = new JComboBox<>();

  private final JComboBox<String> orderCombo1 = new JComboBox<>(Order.names());

  private final JComboBox<String> fieldCombo2 = new JComboBox<>();

  private final JComboBox<String> typeCombo2 = new JComboBox<>();

  private final JComboBox<String> orderCombo2 = new JComboBox<>(Order.names());

  private final ListenerFunctions listeners = new ListenerFunctions();

  private final ComponentOperatorRegistry operatorRegistry;

  private Search searchModel;

  public SortPaneProvider() {
    this.operatorRegistry = ComponentOperatorRegistry.getInstance();
    operatorRegistry.register(SortTabOperator.class, this);
  }

  public JScrollPane get() {
    JPanel panel = new JPanel(new GridLayout(1, 1));
    panel.setOpaque(false);
    panel.setBorder(BorderFactory.createEmptyBorder(5, 5, 5, 5));

    panel.add(initSortConfigsPanel());

    JScrollPane scrollPane = new JScrollPane(panel);
    scrollPane.setOpaque(false);
    scrollPane.getViewport().setOpaque(false);
    return scrollPane;
  }

  private JPanel initSortConfigsPanel() {
    JPanel panel = new JPanel(new GridLayout(5, 1));
    panel.setOpaque(false);
    panel.setMaximumSize(new Dimension(500, 200));

    panel.add(new JLabel(MessageUtils.getLocalizedMessage("search_sort.label.primary")));

    JPanel primary = new JPanel(new FlowLayout(FlowLayout.LEADING));
    primary.setOpaque(false);
    primary.setBorder(BorderFactory.createEmptyBorder(0, 10, 0, 0));
    primary.add(new JLabel(MessageUtils.getLocalizedMessage("search_sort.label.field")));
    fieldCombo1.setPreferredSize(new Dimension(150, 30));
    fieldCombo1.setActionCommand(COMMAND_FIELD_COMBO1);
    fieldCombo1.addActionListener(listeners::changeField);
    primary.add(fieldCombo1);
    primary.add(new JLabel(MessageUtils.getLocalizedMessage("search_sort.label.type")));
    typeCombo1.setPreferredSize(new Dimension(130, 30));
    typeCombo1.addItem("");
    typeCombo1.setEnabled(false);
    primary.add(typeCombo1);
    primary.add(new JLabel(MessageUtils.getLocalizedMessage("search_sort.label.order")));
    orderCombo1.setPreferredSize(new Dimension(100, 30));
    orderCombo1.setEnabled(false);
    primary.add(orderCombo1);
    panel.add(primary);

    panel.add(new JLabel(MessageUtils.getLocalizedMessage("search_sort.label.secondary")));

    JPanel secondary = new JPanel(new FlowLayout(FlowLayout.LEADING));
    secondary.setOpaque(false);
    secondary.setBorder(BorderFactory.createEmptyBorder(0, 10, 0, 0));
    secondary.add(new JLabel(MessageUtils.getLocalizedMessage("search_sort.label.field")));
    fieldCombo2.setPreferredSize(new Dimension(150, 30));
    fieldCombo2.setActionCommand(COMMAND_FIELD_COMBO2);
    fieldCombo2.addActionListener(listeners::changeField);
    secondary.add(fieldCombo2);
    secondary.add(new JLabel(MessageUtils.getLocalizedMessage("search_sort.label.type")));
    typeCombo2.setPreferredSize(new Dimension(130, 30));
    typeCombo2.addItem("");
    typeCombo2.setEnabled(false);
    secondary.add(typeCombo2);
    secondary.add(new JLabel(MessageUtils.getLocalizedMessage("search_sort.label.order")));
    orderCombo2.setPreferredSize(new Dimension(100, 30));
    orderCombo2.setEnabled(false);
    secondary.add(orderCombo2);
    panel.add(secondary);

    JPanel clear = new JPanel(new FlowLayout(FlowLayout.LEADING));
    clear.setOpaque(false);
    JButton clearBtn = new JButton(MessageUtils.getLocalizedMessage("button.clear"));
    clearBtn.addActionListener(listeners::clear);
    clear.add(clearBtn);
    panel.add(clear);

    return panel;
  }

  @Override
  public void setSearchModel(Search model) {
    searchModel = model;
  }

  @Override
  public void setSortableFields(Collection<String> sortableFields) {
    fieldCombo1.removeAllItems();
    fieldCombo2.removeAllItems();

    fieldCombo1.addItem("");
    fieldCombo2.addItem("");

    for (String field : sortableFields) {
      fieldCombo1.addItem(field);
      fieldCombo2.addItem(field);
    }
  }

  @Override
  public Sort getSort() {
    if (StringUtils.isNullOrEmpty((String) fieldCombo1.getSelectedItem())
        && StringUtils.isNullOrEmpty((String) fieldCombo2.getSelectedItem())) {
      return null;
    }

    List<SortField> li = new ArrayList<>();
    if (!StringUtils.isNullOrEmpty((String) fieldCombo1.getSelectedItem())) {
      searchModel.getSortType((String) fieldCombo1.getSelectedItem(), (String) typeCombo1.getSelectedItem(), isReverse(orderCombo1)).ifPresent(li::add);
    }
    if (!StringUtils.isNullOrEmpty((String) fieldCombo2.getSelectedItem())) {
      searchModel.getSortType((String) fieldCombo2.getSelectedItem(), (String) typeCombo2.getSelectedItem(), isReverse(orderCombo2)).ifPresent(li::add);
    }
    return new Sort(li.toArray(new SortField[0]));
  }

  private boolean isReverse(JComboBox<String> order) {
    return Order.valueOf((String) order.getSelectedItem()) == Order.DESC;
  }

  private class ListenerFunctions {

    void changeField(ActionEvent e) {
      if (e.getActionCommand().equalsIgnoreCase(COMMAND_FIELD_COMBO1)) {
        resetField(fieldCombo1, typeCombo1, orderCombo1);
      } else if (e.getActionCommand().equalsIgnoreCase(COMMAND_FIELD_COMBO2)) {
        resetField(fieldCombo2, typeCombo2, orderCombo2);
      }
      resetExactHitsCnt();
    }

    private void resetField(JComboBox<String> fieldCombo, JComboBox<String> typeCombo, JComboBox<String> orderCombo) {
      typeCombo.removeAllItems();
      if (StringUtils.isNullOrEmpty((String) fieldCombo.getSelectedItem())) {
        typeCombo.addItem("");
        typeCombo.setEnabled(false);
        orderCombo.setEnabled(false);
      } else {
        List<SortField> sortFields = searchModel.guessSortTypes((String) fieldCombo.getSelectedItem());
        sortFields.stream()
            .map(sf -> {
              if (sf instanceof SortedNumericSortField) {
                return ((SortedNumericSortField) sf).getNumericType().name();
              } else {
                return sf.getType().name();
              }
            }).forEach(typeCombo::addItem);
        typeCombo.setEnabled(true);
        orderCombo.setEnabled(true);
      }
    }

    void clear(ActionEvent e) {
      fieldCombo1.setSelectedIndex(0);
      typeCombo1.removeAllItems();
      typeCombo1.setSelectedItem("");
      typeCombo1.setEnabled(false);
      orderCombo1.setSelectedIndex(0);
      orderCombo1.setEnabled(false);

      fieldCombo2.setSelectedIndex(0);
      typeCombo2.removeAllItems();
      typeCombo2.setSelectedItem("");
      typeCombo2.setEnabled(false);
      orderCombo2.setSelectedIndex(0);
      orderCombo2.setEnabled(false);

      resetExactHitsCnt();
    }

    private void resetExactHitsCnt() {
      operatorRegistry.get(SearchTabOperator.class).ifPresent(operator -> {
        if (StringUtils.isNullOrEmpty((String) fieldCombo1.getSelectedItem()) &&
            StringUtils.isNullOrEmpty((String) fieldCombo2.getSelectedItem())) {
          operator.enableExactHitsCB(true);
          operator.setExactHits(false);
        } else {
          operator.enableExactHitsCB(false);
          operator.setExactHits(true);
        }
      });
    }
  }

  enum Order {
    ASC, DESC;

    static String[] names() {
      return Arrays.stream(values()).map(Order::name).toArray(String[]::new);
    }
  }
}
