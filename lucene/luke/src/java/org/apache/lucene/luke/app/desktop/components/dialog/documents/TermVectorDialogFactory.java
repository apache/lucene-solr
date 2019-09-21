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

package org.apache.lucene.luke.app.desktop.components.dialog.documents;

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
import java.awt.Insets;
import java.awt.Window;
import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import org.apache.lucene.luke.app.desktop.Preferences;
import org.apache.lucene.luke.app.desktop.PreferencesFactory;
import org.apache.lucene.luke.app.desktop.components.TableColumnInfo;
import org.apache.lucene.luke.app.desktop.components.TableModelBase;
import org.apache.lucene.luke.app.desktop.util.DialogOpener;
import org.apache.lucene.luke.app.desktop.util.MessageUtils;
import org.apache.lucene.luke.app.desktop.util.TableUtils;
import org.apache.lucene.luke.models.documents.TermVectorEntry;

/** Factory of term vector dialog */
public final class TermVectorDialogFactory implements DialogOpener.DialogFactory {

  private static TermVectorDialogFactory instance;

  private final Preferences prefs;

  private JDialog dialog;

  private String field;

  private List<TermVectorEntry> tvEntries;

  public synchronized static TermVectorDialogFactory getInstance() throws IOException {
    if (instance == null) {
      instance = new TermVectorDialogFactory();
    }
    return instance;
  }

  private TermVectorDialogFactory() throws IOException {
    this.prefs = PreferencesFactory.getInstance();
  }

  public void setField(String field) {
    this.field = field;
  }

  public void setTvEntries(List<TermVectorEntry> tvEntries) {
    this.tvEntries = tvEntries;
  }

  @Override
  public JDialog create(Window owner, String title, int width, int height) {
    if (Objects.isNull(field) || Objects.isNull(tvEntries)) {
      throw new IllegalStateException("field name and/or term vector is not set.");
    }

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

    JPanel header = new JPanel(new FlowLayout(FlowLayout.LEADING, 5, 5));
    header.setOpaque(false);
    header.add(new JLabel(MessageUtils.getLocalizedMessage("documents.termvector.label.term_vector")));
    header.add(new JLabel(field));
    panel.add(header, BorderLayout.PAGE_START);

    JTable tvTable = new JTable();
    TableUtils.setupTable(tvTable, ListSelectionModel.SINGLE_SELECTION, new TermVectorTableModel(tvEntries), null, 100, 50, 100);
    JScrollPane scrollPane = new JScrollPane(tvTable);
    panel.add(scrollPane, BorderLayout.CENTER);

    JPanel footer = new JPanel(new FlowLayout(FlowLayout.TRAILING, 0, 10));
    footer.setOpaque(false);
    JButton closeBtn = new JButton(MessageUtils.getLocalizedMessage("button.close"));
    closeBtn.setMargin(new Insets(3, 3, 3, 3));
    closeBtn.addActionListener(e -> dialog.dispose());
    footer.add(closeBtn);
    panel.add(footer, BorderLayout.PAGE_END);

    return panel;
  }

  static final class TermVectorTableModel extends TableModelBase<TermVectorTableModel.Column> {

    enum Column implements TableColumnInfo {

      TERM("Term", 0, String.class),
      FREQ("Freq", 1, Long.class),
      POSITIONS("Positions", 2, String.class),
      OFFSETS("Offsets", 3, String.class);

      private String colName;
      private int index;
      private Class<?> type;

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

    TermVectorTableModel() {
      super();
    }

    TermVectorTableModel(List<TermVectorEntry> tvEntries) {
      super(tvEntries.size());

      for (int i = 0; i < tvEntries.size(); i++) {
        TermVectorEntry entry = tvEntries.get(i);

        String termText = entry.getTermText();
        long freq = tvEntries.get(i).getFreq();
        String positions = String.join(",",
            entry.getPositions().stream()
                .map(pos -> Integer.toString(pos.getPosition()))
                .collect(Collectors.toList()));
        String offsets = String.join(",",
            entry.getPositions().stream()
                .filter(pos -> pos.getStartOffset().isPresent() && pos.getEndOffset().isPresent())
                .map(pos -> Integer.toString(pos.getStartOffset().orElse(-1)) + "-" + Integer.toString(pos.getEndOffset().orElse(-1)))
                .collect(Collectors.toList())
        );

        data[i] = new Object[]{termText, freq, positions, offsets};
      }

    }

    @Override
    protected Column[] columnInfos() {
      return Column.values();
    }
  }
}
