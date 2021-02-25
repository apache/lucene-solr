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

package org.apache.lucene.luke.app.desktop.components.dialog.analysis;

import java.awt.BorderLayout;
import java.awt.Component;
import java.awt.Dialog;
import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.Window;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.swing.BorderFactory;
import javax.swing.JButton;
import javax.swing.JDialog;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTable;
import javax.swing.ListSelectionModel;
import javax.swing.table.TableCellRenderer;
import org.apache.lucene.luke.app.desktop.Preferences;
import org.apache.lucene.luke.app.desktop.PreferencesFactory;
import org.apache.lucene.luke.app.desktop.components.ComponentOperatorRegistry;
import org.apache.lucene.luke.app.desktop.components.TableColumnInfo;
import org.apache.lucene.luke.app.desktop.components.TableModelBase;
import org.apache.lucene.luke.app.desktop.components.fragments.analysis.CustomAnalyzerPanelOperator;
import org.apache.lucene.luke.app.desktop.util.DialogOpener;
import org.apache.lucene.luke.app.desktop.util.MessageUtils;
import org.apache.lucene.luke.app.desktop.util.TableUtils;
import org.apache.lucene.luke.app.desktop.util.lang.Callable;

/** Factory of edit filters dialog */
public final class EditFiltersDialogFactory implements DialogOpener.DialogFactory {

  private static EditFiltersDialogFactory instance;

  private final Preferences prefs;

  private final ComponentOperatorRegistry operatorRegistry;

  private final EditParamsDialogFactory editParamsDialogFactory;

  private final JLabel targetLbl = new JLabel();

  private final JTable filtersTable = new JTable();

  private final ListenerFunctions listeners = new ListenerFunctions();

  private final FiltersTableMouseListener tableListener = new FiltersTableMouseListener();

  private JDialog dialog;

  private List<String> selectedFilters;

  private Callable callback;

  private EditFiltersMode mode;

  public static synchronized EditFiltersDialogFactory getInstance() throws IOException {
    if (instance == null) {
      instance = new EditFiltersDialogFactory();
    }
    return instance;
  }

  private EditFiltersDialogFactory() throws IOException {
    this.prefs = PreferencesFactory.getInstance();
    this.operatorRegistry = ComponentOperatorRegistry.getInstance();
    this.editParamsDialogFactory = EditParamsDialogFactory.getInstance();
  }

  public void setSelectedFilters(List<String> selectedFilters) {
    this.selectedFilters = selectedFilters;
  }

  public void setCallback(Callable callback) {
    this.callback = callback;
  }

  public void setMode(EditFiltersMode mode) {
    this.mode = mode;
  }

  @Override
  public JDialog create(Window owner, String title, int width, int height) {
    dialog = new JDialog(owner, title, Dialog.ModalityType.APPLICATION_MODAL);
    dialog.add(content());
    dialog.setSize(new Dimension(width, height));
    dialog.setLocationRelativeTo(owner);
    dialog.getContentPane().setBackground(prefs.getColorTheme().getBackgroundColor());
    return dialog;
  }

  private JPanel content() {
    JPanel panel = new JPanel(new BorderLayout());
    panel.setOpaque(false);
    panel.setBorder(BorderFactory.createEmptyBorder(10, 10, 10, 10));

    JPanel header = new JPanel(new FlowLayout(FlowLayout.LEADING, 10, 10));
    header.setOpaque(false);
    header.add(new JLabel(MessageUtils.getLocalizedMessage("analysis.dialog.hint.edit_param")));
    header.add(targetLbl);
    panel.add(header, BorderLayout.PAGE_START);

    TableUtils.setupTable(
        filtersTable,
        ListSelectionModel.SINGLE_SELECTION,
        new FiltersTableModel(selectedFilters),
        tableListener,
        FiltersTableModel.Column.DELETE.getColumnWidth(),
        FiltersTableModel.Column.ORDER.getColumnWidth());
    filtersTable.setShowGrid(true);
    filtersTable
        .getColumnModel()
        .getColumn(FiltersTableModel.Column.TYPE.getIndex())
        .setCellRenderer(new TypeCellRenderer());
    panel.add(new JScrollPane(filtersTable), BorderLayout.CENTER);

    JPanel footer = new JPanel(new FlowLayout(FlowLayout.TRAILING, 10, 5));
    footer.setOpaque(false);
    JButton okBtn = new JButton(MessageUtils.getLocalizedMessage("button.ok"));
    okBtn.addActionListener(
        e -> {
          List<Integer> deletedIndexes = new ArrayList<>();
          for (int i = 0; i < filtersTable.getRowCount(); i++) {
            boolean deleted =
                (boolean) filtersTable.getValueAt(i, FiltersTableModel.Column.DELETE.getIndex());
            if (deleted) {
              deletedIndexes.add(i);
            }
          }
          operatorRegistry
              .get(CustomAnalyzerPanelOperator.class)
              .ifPresent(
                  operator -> {
                    switch (mode) {
                      case CHARFILTER:
                        operator.updateCharFilters(deletedIndexes);
                        break;
                      case TOKENFILTER:
                        operator.updateTokenFilters(deletedIndexes);
                        break;
                    }
                  });
          callback.call();
          dialog.dispose();
        });
    footer.add(okBtn);
    JButton cancelBtn = new JButton(MessageUtils.getLocalizedMessage("button.cancel"));
    cancelBtn.addActionListener(e -> dialog.dispose());
    footer.add(cancelBtn);
    panel.add(footer, BorderLayout.PAGE_END);

    return panel;
  }

  private class ListenerFunctions {

    void showEditParamsDialog(MouseEvent e) {
      if (e.getClickCount() != 2 || e.isConsumed()) {
        return;
      }
      int selectedIndex = filtersTable.rowAtPoint(e.getPoint());
      if (selectedIndex < 0 || selectedIndex >= selectedFilters.size()) {
        return;
      }

      switch (mode) {
        case CHARFILTER:
          showEditParamsCharFilterDialog(selectedIndex);
          break;
        case TOKENFILTER:
          showEditParamsTokenFilterDialog(selectedIndex);
          break;
        default:
      }
    }

    private void showEditParamsCharFilterDialog(int selectedIndex) {
      int targetIndex = filtersTable.getSelectedRow();
      String selectedItem =
          (String) filtersTable.getValueAt(selectedIndex, FiltersTableModel.Column.TYPE.getIndex());
      Map<String, String> params =
          operatorRegistry
              .get(CustomAnalyzerPanelOperator.class)
              .map(operator -> operator.getCharFilterParams(targetIndex))
              .orElse(Collections.emptyMap());
      new DialogOpener<>(editParamsDialogFactory)
          .open(
              dialog,
              MessageUtils.getLocalizedMessage("analysis.dialog.title.char_filter_params"),
              400,
              300,
              factory -> {
                factory.setMode(EditParamsMode.CHARFILTER);
                factory.setTargetIndex(targetIndex);
                factory.setTarget(selectedItem);
                factory.setParams(params);
              });
    }

    private void showEditParamsTokenFilterDialog(int selectedIndex) {
      int targetIndex = filtersTable.getSelectedRow();
      String selectedItem =
          (String) filtersTable.getValueAt(selectedIndex, FiltersTableModel.Column.TYPE.getIndex());
      Map<String, String> params =
          operatorRegistry
              .get(CustomAnalyzerPanelOperator.class)
              .map(operator -> operator.getTokenFilterParams(targetIndex))
              .orElse(Collections.emptyMap());
      new DialogOpener<>(editParamsDialogFactory)
          .open(
              dialog,
              MessageUtils.getLocalizedMessage("analysis.dialog.title.char_filter_params"),
              400,
              300,
              factory -> {
                factory.setMode(EditParamsMode.TOKENFILTER);
                factory.setTargetIndex(targetIndex);
                factory.setTarget(selectedItem);
                factory.setParams(params);
              });
    }
  }

  private class FiltersTableMouseListener extends MouseAdapter {
    @Override
    public void mouseClicked(MouseEvent e) {
      listeners.showEditParamsDialog(e);
    }
  }

  static final class FiltersTableModel extends TableModelBase<FiltersTableModel.Column> {

    enum Column implements TableColumnInfo {
      DELETE("Delete", 0, Boolean.class, 50),
      ORDER("Order", 1, Integer.class, 50),
      TYPE("Factory class", 2, String.class, Integer.MAX_VALUE);

      private final String colName;
      private final int index;
      private final Class<?> type;
      private final int width;

      Column(String colName, int index, Class<?> type, int width) {
        this.colName = colName;
        this.index = index;
        this.type = type;
        this.width = width;
      }

      @Override
      public String getColName() {
        return colName;
      }

      @Override
      public int getIndex() {
        return index;
      }

      @Override
      public Class<?> getType() {
        return type;
      }

      @Override
      public int getColumnWidth() {
        return width;
      }
    }

    FiltersTableModel() {
      super();
    }

    FiltersTableModel(List<String> selectedFilters) {
      super(selectedFilters.size());
      for (int i = 0; i < selectedFilters.size(); i++) {
        data[i][Column.DELETE.getIndex()] = false;
        data[i][Column.ORDER.getIndex()] = i + 1;
        data[i][Column.TYPE.getIndex()] = selectedFilters.get(i);
      }
    }

    @Override
    public boolean isCellEditable(int rowIndex, int columnIndex) {
      return columnIndex == Column.DELETE.getIndex();
    }

    @Override
    public void setValueAt(Object value, int rowIndex, int columnIndex) {
      data[rowIndex][columnIndex] = value;
    }

    @Override
    protected Column[] columnInfos() {
      return Column.values();
    }
  }

  static final class TypeCellRenderer implements TableCellRenderer {

    @Override
    public Component getTableCellRendererComponent(
        JTable table, Object value, boolean isSelected, boolean hasFocus, int row, int column) {
      String[] tmp = ((String) value).split("\\.");
      String type = tmp[tmp.length - 1];
      return new JLabel(type);
    }
  }
}
