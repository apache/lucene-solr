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

import javax.swing.BorderFactory;
import javax.swing.JButton;
import javax.swing.JDialog;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTable;
import javax.swing.ListSelectionModel;
import java.awt.BorderLayout;
import java.awt.Dialog;
import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.Window;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.lucene.luke.app.desktop.Preferences;
import org.apache.lucene.luke.app.desktop.PreferencesFactory;
import org.apache.lucene.luke.app.desktop.components.TableColumnInfo;
import org.apache.lucene.luke.app.desktop.components.TableModelBase;
import org.apache.lucene.luke.app.desktop.util.DialogOpener;
import org.apache.lucene.luke.app.desktop.util.MessageUtils;
import org.apache.lucene.luke.app.desktop.util.TableUtils;
import org.apache.lucene.luke.models.analysis.Analysis;

/** Factory of token attribute dialog */
public final class TokenAttributeDialogFactory implements DialogOpener.DialogFactory {

  private static TokenAttributeDialogFactory instance;

  private final Preferences prefs;

  private final JTable attributesTable = new JTable();

  private JDialog dialog;

  private String term;

  private List<Analysis.TokenAttribute> attributes;

  public synchronized static TokenAttributeDialogFactory getInstance() throws IOException {
    if (instance == null) {
      instance = new TokenAttributeDialogFactory();
    }
    return instance;
  }

  private TokenAttributeDialogFactory() throws IOException {
    this.prefs = PreferencesFactory.getInstance();
  }

  public void setTerm(String term) {
    this.term = term;
  }

  public void setAttributes(List<Analysis.TokenAttribute> attributes) {
    this.attributes = attributes;
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
    panel.setBorder(BorderFactory.createEmptyBorder(15, 15, 15, 15));

    JPanel header = new JPanel(new FlowLayout(FlowLayout.LEADING));
    header.setOpaque(false);
    header.add(new JLabel("All token attributes for:"));
    header.add(new JLabel(term));
    panel.add(header, BorderLayout.PAGE_START);

    List<TokenAttValue> attrValues = attributes.stream()
        .flatMap(att -> att.getAttValues().entrySet().stream().map(e -> TokenAttValue.of(att.getAttClass(), e.getKey(), e.getValue())))
        .collect(Collectors.toList());
    TableUtils.setupTable(attributesTable, ListSelectionModel.SINGLE_SELECTION, new AttributeTableModel(attrValues), null);
    panel.add(new JScrollPane(attributesTable), BorderLayout.CENTER);

    JPanel footer = new JPanel(new FlowLayout(FlowLayout.TRAILING));
    footer.setOpaque(false);
    JButton okBtn = new JButton(MessageUtils.getLocalizedMessage("button.ok"));
    okBtn.addActionListener(e -> dialog.dispose());
    footer.add(okBtn);
    panel.add(footer, BorderLayout.PAGE_END);

    return panel;
  }

  static final class AttributeTableModel extends TableModelBase<AttributeTableModel.Column> {

    enum Column implements TableColumnInfo {

      ATTR("Attribute", 0, String.class),
      NAME("Name", 1, String.class),
      VALUE("Value", 2, String.class);

      private final String colName;
      private final int index;
      private final Class<?> type;

      Column(String colName, int index, Class<?> type) {
        this.colName = colName;
        this.index = index;
        this.type = type;
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
    }

    AttributeTableModel(List<TokenAttValue> attrValues) {
      super(attrValues.size());
      for (int i = 0; i < attrValues.size(); i++) {
        TokenAttValue attrValue = attrValues.get(i);
        data[i][Column.ATTR.getIndex()] = attrValue.getAttClass();
        data[i][Column.NAME.getIndex()] = attrValue.getName();
        data[i][Column.VALUE.getIndex()] = attrValue.getValue();
      }
    }

    @Override
    protected Column[] columnInfos() {
      return Column.values();
    }
  }

  static final class TokenAttValue {
    private String attClass;
    private String name;
    private String value;

    public static TokenAttValue of(String attClass, String name, String value) {
      TokenAttValue attValue = new TokenAttValue();
      attValue.attClass = attClass;
      attValue.name = name;
      attValue.value = value;
      return attValue;
    }

    private TokenAttValue() {
    }

    String getAttClass() {
      return attClass;
    }

    String getName() {
      return name;
    }

    String getValue() {
      return value;
    }
  }

}
