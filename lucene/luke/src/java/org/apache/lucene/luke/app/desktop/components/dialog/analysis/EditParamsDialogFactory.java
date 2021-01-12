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
import java.awt.Dialog;
import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.Window;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import javax.swing.BorderFactory;
import javax.swing.JButton;
import javax.swing.JDialog;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTable;
import javax.swing.ListSelectionModel;
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

/** Factory of edit parameters dialog */
public final class EditParamsDialogFactory implements DialogOpener.DialogFactory {

  private static EditParamsDialogFactory instance;

  private final Preferences prefs;

  private final ComponentOperatorRegistry operatorRegistry;

  private final JTable paramsTable = new JTable();

  private JDialog dialog;

  private EditParamsMode mode;

  private String target;

  private int targetIndex;

  private Map<String, String> params = new HashMap<>();

  private Callable callback;

  public static synchronized EditParamsDialogFactory getInstance() throws IOException {
    if (instance == null) {
      instance = new EditParamsDialogFactory();
    }
    return instance;
  }

  private EditParamsDialogFactory() throws IOException {
    this.prefs = PreferencesFactory.getInstance();
    this.operatorRegistry = ComponentOperatorRegistry.getInstance();
  }

  public void setMode(EditParamsMode mode) {
    this.mode = mode;
  }

  public void setTarget(String target) {
    this.target = target;
  }

  public void setTargetIndex(int targetIndex) {
    this.targetIndex = targetIndex;
  }

  public void setParams(Map<String, String> params) {
    this.params.putAll(params);
  }

  public void setCallback(Callable callback) {
    this.callback = callback;
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
    header.add(new JLabel("Parameters for:"));
    String[] tmp = target.split("\\.");
    JLabel targetLbl = new JLabel(tmp[tmp.length - 1]);
    header.add(targetLbl);
    panel.add(header, BorderLayout.PAGE_START);

    TableUtils.setupTable(
        paramsTable,
        ListSelectionModel.SINGLE_SELECTION,
        new ParamsTableModel(params),
        null,
        ParamsTableModel.Column.DELETE.getColumnWidth(),
        ParamsTableModel.Column.NAME.getColumnWidth());
    paramsTable.setShowGrid(true);
    panel.add(new JScrollPane(paramsTable), BorderLayout.CENTER);

    JPanel footer = new JPanel(new FlowLayout(FlowLayout.TRAILING, 10, 5));
    footer.setOpaque(false);
    JButton okBtn = new JButton(MessageUtils.getLocalizedMessage("button.ok"));
    okBtn.addActionListener(
        e -> {
          Map<String, String> params = new HashMap<>();
          for (int i = 0; i < paramsTable.getRowCount(); i++) {
            boolean deleted =
                (boolean) paramsTable.getValueAt(i, ParamsTableModel.Column.DELETE.getIndex());
            String name =
                (String) paramsTable.getValueAt(i, ParamsTableModel.Column.NAME.getIndex());
            String value =
                (String) paramsTable.getValueAt(i, ParamsTableModel.Column.VALUE.getIndex());
            if (deleted
                || Objects.isNull(name)
                || name.equals("")
                || Objects.isNull(value)
                || value.equals("")) {
              continue;
            }
            params.put(name, value);
          }
          updateTargetParams(params);
          callback.call();
          this.params.clear();
          dialog.dispose();
        });
    footer.add(okBtn);
    JButton cancelBtn = new JButton(MessageUtils.getLocalizedMessage("button.cancel"));
    cancelBtn.addActionListener(
        e -> {
          this.params.clear();
          dialog.dispose();
        });
    footer.add(cancelBtn);
    panel.add(footer, BorderLayout.PAGE_END);

    return panel;
  }

  private void updateTargetParams(Map<String, String> params) {
    operatorRegistry
        .get(CustomAnalyzerPanelOperator.class)
        .ifPresent(
            operator -> {
              switch (mode) {
                case CHARFILTER:
                  operator.updateCharFilterParams(targetIndex, params);
                  break;
                case TOKENIZER:
                  operator.updateTokenizerParams(params);
                  break;
                case TOKENFILTER:
                  operator.updateTokenFilterParams(targetIndex, params);
                  break;
              }
            });
  }

  static final class ParamsTableModel extends TableModelBase<ParamsTableModel.Column> {

    enum Column implements TableColumnInfo {
      DELETE("Delete", 0, Boolean.class, 50),
      NAME("Name", 1, String.class, 150),
      VALUE("Value", 2, String.class, Integer.MAX_VALUE);

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

    private static final int PARAM_SIZE = 20;

    ParamsTableModel(Map<String, String> params) {
      super(PARAM_SIZE);
      List<String> keys = new ArrayList<>(params.keySet());
      for (int i = 0; i < keys.size(); i++) {
        data[i][Column.NAME.getIndex()] = keys.get(i);
        data[i][Column.VALUE.getIndex()] = params.get(keys.get(i));
      }
      for (int i = 0; i < data.length; i++) {
        data[i][Column.DELETE.getIndex()] = false;
      }
    }

    @Override
    public boolean isCellEditable(int rowIndex, int columnIndex) {
      return true;
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
}
