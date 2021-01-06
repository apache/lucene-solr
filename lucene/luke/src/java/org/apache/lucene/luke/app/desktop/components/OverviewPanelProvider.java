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

package org.apache.lucene.luke.app.desktop.components;

import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.GridLayout;
import java.awt.Insets;
import java.awt.event.ActionEvent;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import javax.swing.BorderFactory;
import javax.swing.BoxLayout;
import javax.swing.JButton;
import javax.swing.JLabel;
import javax.swing.JMenuItem;
import javax.swing.JPanel;
import javax.swing.JPopupMenu;
import javax.swing.JScrollPane;
import javax.swing.JSpinner;
import javax.swing.JSplitPane;
import javax.swing.JTable;
import javax.swing.JTextField;
import javax.swing.ListSelectionModel;
import javax.swing.SpinnerNumberModel;
import javax.swing.table.DefaultTableCellRenderer;
import javax.swing.table.TableRowSorter;
import org.apache.lucene.luke.app.IndexHandler;
import org.apache.lucene.luke.app.IndexObserver;
import org.apache.lucene.luke.app.LukeState;
import org.apache.lucene.luke.app.desktop.MessageBroker;
import org.apache.lucene.luke.app.desktop.util.MessageUtils;
import org.apache.lucene.luke.app.desktop.util.StyleConstants;
import org.apache.lucene.luke.app.desktop.util.TableUtils;
import org.apache.lucene.luke.models.overview.Overview;
import org.apache.lucene.luke.models.overview.OverviewFactory;
import org.apache.lucene.luke.models.overview.TermCountsOrder;
import org.apache.lucene.luke.models.overview.TermStats;

/** Provider of the Overview panel */
public final class OverviewPanelProvider {

  private static final int GRIDX_DESC = 0;
  private static final int GRIDX_VAL = 1;
  private static final double WEIGHTX_DESC = 0.1;
  private static final double WEIGHTX_VAL = 0.9;

  private final OverviewFactory overviewFactory = new OverviewFactory();

  private final ComponentOperatorRegistry operatorRegistry;

  private final TabSwitcherProxy tabSwitcher;

  private final MessageBroker messageBroker;

  private final JPanel panel = new JPanel();

  private final JLabel indexPathLbl = new JLabel();

  private final JLabel numFieldsLbl = new JLabel();

  private final JLabel numDocsLbl = new JLabel();

  private final JLabel numTermsLbl = new JLabel();

  private final JLabel delOptLbl = new JLabel();

  private final JLabel indexVerLbl = new JLabel();

  private final JLabel indexFmtLbl = new JLabel();

  private final JLabel dirImplLbl = new JLabel();

  private final JLabel commitPointLbl = new JLabel();

  private final JLabel commitUserDataLbl = new JLabel();

  private final JTable termCountsTable = new JTable();

  private final JTextField selectedField = new JTextField();

  private final JButton showTopTermsBtn = new JButton();

  private final JSpinner numTopTermsSpnr = new JSpinner();

  private final JTable topTermsTable = new JTable();

  private final JPopupMenu topTermsContextMenu = new JPopupMenu();

  private final ListenerFunctions listeners = new ListenerFunctions();

  private Overview overviewModel;

  public OverviewPanelProvider() {
    this.messageBroker = MessageBroker.getInstance();
    this.operatorRegistry = ComponentOperatorRegistry.getInstance();
    this.tabSwitcher = TabSwitcherProxy.getInstance();

    IndexHandler.getInstance().addObserver(new Observer());
  }

  public JPanel get() {
    panel.setOpaque(false);
    panel.setLayout(new GridLayout(1, 1));
    panel.setBorder(BorderFactory.createLineBorder(Color.gray));

    JSplitPane splitPane =
        new JSplitPane(JSplitPane.VERTICAL_SPLIT, initUpperPanel(), initLowerPanel());
    splitPane.setDividerLocation(0.4);
    splitPane.setOpaque(false);
    panel.add(splitPane);

    setUpTopTermsContextMenu();

    return panel;
  }

  private JPanel initUpperPanel() {
    JPanel panel = new JPanel(new GridBagLayout());
    panel.setOpaque(false);
    panel.setBorder(BorderFactory.createEmptyBorder(3, 3, 3, 3));

    GridBagConstraints c = new GridBagConstraints();
    c.fill = GridBagConstraints.HORIZONTAL;
    c.insets = new Insets(2, 10, 2, 2);
    c.gridy = 0;

    c.gridx = GRIDX_DESC;
    c.weightx = WEIGHTX_DESC;
    panel.add(
        new JLabel(MessageUtils.getLocalizedMessage("overview.label.index_path"), JLabel.RIGHT), c);

    c.gridx = GRIDX_VAL;
    c.weightx = WEIGHTX_VAL;
    indexPathLbl.setText("?");
    panel.add(indexPathLbl, c);

    c.gridx = GRIDX_DESC;
    c.gridy += 1;
    c.weightx = WEIGHTX_DESC;
    panel.add(
        new JLabel(MessageUtils.getLocalizedMessage("overview.label.num_fields"), JLabel.RIGHT), c);

    c.gridx = GRIDX_VAL;
    c.weightx = WEIGHTX_VAL;
    numFieldsLbl.setText("?");
    panel.add(numFieldsLbl, c);

    c.gridx = GRIDX_DESC;
    c.gridy += 1;
    c.weightx = WEIGHTX_DESC;
    panel.add(
        new JLabel(MessageUtils.getLocalizedMessage("overview.label.num_docs"), JLabel.RIGHT), c);

    c.gridx = GRIDX_VAL;
    c.weightx = WEIGHTX_VAL;
    numDocsLbl.setText("?");
    panel.add(numDocsLbl, c);

    c.gridx = GRIDX_DESC;
    c.gridy += 1;
    c.weightx = WEIGHTX_DESC;
    panel.add(
        new JLabel(MessageUtils.getLocalizedMessage("overview.label.num_terms"), JLabel.RIGHT), c);

    c.gridx = GRIDX_VAL;
    c.weightx = WEIGHTX_VAL;
    numTermsLbl.setText("?");
    panel.add(numTermsLbl, c);

    c.gridx = GRIDX_DESC;
    c.gridy += 1;
    c.weightx = WEIGHTX_DESC;
    panel.add(
        new JLabel(MessageUtils.getLocalizedMessage("overview.label.del_opt"), JLabel.RIGHT), c);

    c.gridx = GRIDX_VAL;
    c.weightx = WEIGHTX_VAL;
    delOptLbl.setText("?");
    panel.add(delOptLbl, c);

    c.gridx = GRIDX_DESC;
    c.gridy += 1;
    c.weightx = WEIGHTX_DESC;
    panel.add(
        new JLabel(MessageUtils.getLocalizedMessage("overview.label.index_version"), JLabel.RIGHT),
        c);

    c.gridx = GRIDX_VAL;
    c.weightx = WEIGHTX_VAL;
    indexVerLbl.setText("?");
    panel.add(indexVerLbl, c);

    c.gridx = GRIDX_DESC;
    c.gridy += 1;
    c.weightx = WEIGHTX_DESC;
    panel.add(
        new JLabel(MessageUtils.getLocalizedMessage("overview.label.index_format"), JLabel.RIGHT),
        c);

    c.gridx = GRIDX_VAL;
    c.weightx = WEIGHTX_VAL;
    indexFmtLbl.setText("?");
    panel.add(indexFmtLbl, c);

    c.gridx = GRIDX_DESC;
    c.gridy += 1;
    c.weightx = WEIGHTX_DESC;
    panel.add(
        new JLabel(MessageUtils.getLocalizedMessage("overview.label.dir_impl"), JLabel.RIGHT), c);

    c.gridx = GRIDX_VAL;
    c.weightx = WEIGHTX_VAL;
    dirImplLbl.setText("?");
    panel.add(dirImplLbl, c);

    c.gridx = GRIDX_DESC;
    c.gridy += 1;
    c.weightx = WEIGHTX_DESC;
    panel.add(
        new JLabel(MessageUtils.getLocalizedMessage("overview.label.commit_point"), JLabel.RIGHT),
        c);

    c.gridx = GRIDX_VAL;
    c.weightx = WEIGHTX_VAL;
    commitPointLbl.setText("?");
    panel.add(commitPointLbl, c);

    c.gridx = GRIDX_DESC;
    c.gridy += 1;
    c.weightx = WEIGHTX_DESC;
    panel.add(
        new JLabel(
            MessageUtils.getLocalizedMessage("overview.label.commit_userdata"), JLabel.RIGHT),
        c);

    c.gridx = GRIDX_VAL;
    c.weightx = WEIGHTX_VAL;
    commitUserDataLbl.setText("?");
    panel.add(commitUserDataLbl, c);

    return panel;
  }

  private JPanel initLowerPanel() {
    JPanel panel = new JPanel(new BorderLayout());
    panel.setOpaque(false);

    JLabel label = new JLabel(MessageUtils.getLocalizedMessage("overview.label.select_fields"));
    label.setBorder(BorderFactory.createEmptyBorder(5, 10, 5, 10));
    panel.add(label, BorderLayout.PAGE_START);

    JSplitPane splitPane =
        new JSplitPane(JSplitPane.HORIZONTAL_SPLIT, initTermCountsPanel(), initTopTermsPanel());
    splitPane.setOpaque(false);
    splitPane.setDividerLocation(320);
    splitPane.setBorder(BorderFactory.createEmptyBorder(10, 10, 10, 10));
    panel.add(splitPane, BorderLayout.CENTER);

    return panel;
  }

  private JPanel initTermCountsPanel() {
    JPanel panel = new JPanel(new BorderLayout());
    panel.setOpaque(false);

    JLabel label = new JLabel(MessageUtils.getLocalizedMessage("overview.label.available_fields"));
    label.setBorder(BorderFactory.createEmptyBorder(0, 0, 5, 0));
    panel.add(label, BorderLayout.PAGE_START);

    TableUtils.setupTable(
        termCountsTable,
        ListSelectionModel.SINGLE_SELECTION,
        new TermCountsTableModel(),
        new MouseAdapter() {
          @Override
          public void mouseClicked(MouseEvent e) {
            listeners.selectField(e);
          }
        },
        TermCountsTableModel.Column.NAME.getColumnWidth(),
        TermCountsTableModel.Column.TERM_COUNT.getColumnWidth());
    JScrollPane scrollPane = new JScrollPane(termCountsTable);
    panel.add(scrollPane, BorderLayout.CENTER);

    panel.setOpaque(false);
    return panel;
  }

  private JPanel initTopTermsPanel() {
    JPanel panel = new JPanel(new GridLayout(1, 1));
    panel.setOpaque(false);

    JPanel selectedPanel = new JPanel(new BorderLayout());
    selectedPanel.setOpaque(false);
    JPanel innerPanel = new JPanel();
    innerPanel.setOpaque(false);
    innerPanel.setLayout(new BoxLayout(innerPanel, BoxLayout.PAGE_AXIS));
    innerPanel.setBorder(BorderFactory.createEmptyBorder(20, 0, 0, 0));
    selectedPanel.add(innerPanel, BorderLayout.PAGE_START);

    JPanel innerPanel1 = new JPanel(new FlowLayout(FlowLayout.LEADING));
    innerPanel1.setOpaque(false);
    innerPanel1.add(new JLabel(MessageUtils.getLocalizedMessage("overview.label.selected_field")));
    innerPanel.add(innerPanel1);

    selectedField.setColumns(20);
    selectedField.setPreferredSize(new Dimension(100, 30));
    selectedField.setFont(StyleConstants.FONT_MONOSPACE_LARGE);
    selectedField.setEditable(false);
    selectedField.setBackground(Color.white);
    JPanel innerPanel2 = new JPanel(new FlowLayout(FlowLayout.LEADING));
    innerPanel2.setOpaque(false);
    innerPanel2.add(selectedField);
    innerPanel.add(innerPanel2);

    showTopTermsBtn.setText(MessageUtils.getLocalizedMessage("overview.button.show_terms"));
    showTopTermsBtn.setPreferredSize(new Dimension(170, 40));
    showTopTermsBtn.setFont(StyleConstants.FONT_BUTTON_LARGE);
    showTopTermsBtn.addActionListener(listeners::showTopTerms);
    showTopTermsBtn.setEnabled(false);
    JPanel innerPanel3 = new JPanel(new FlowLayout(FlowLayout.LEADING));
    innerPanel3.setOpaque(false);
    innerPanel3.add(showTopTermsBtn);
    innerPanel.add(innerPanel3);

    JPanel innerPanel4 = new JPanel(new FlowLayout(FlowLayout.LEADING));
    innerPanel4.setOpaque(false);
    innerPanel4.add(new JLabel(MessageUtils.getLocalizedMessage("overview.label.num_top_terms")));
    innerPanel.add(innerPanel4);

    SpinnerNumberModel numberModel = new SpinnerNumberModel(50, 0, 1000, 1);
    numTopTermsSpnr.setPreferredSize(new Dimension(80, 30));
    numTopTermsSpnr.setModel(numberModel);
    JPanel innerPanel5 = new JPanel(new FlowLayout(FlowLayout.LEADING));
    innerPanel5.setOpaque(false);
    innerPanel5.add(numTopTermsSpnr);
    innerPanel.add(innerPanel5);

    JPanel termsPanel = new JPanel(new BorderLayout());
    termsPanel.setOpaque(false);
    JLabel label = new JLabel(MessageUtils.getLocalizedMessage("overview.label.top_terms"));
    label.setBorder(BorderFactory.createEmptyBorder(0, 0, 5, 0));
    termsPanel.add(label, BorderLayout.PAGE_START);

    TableUtils.setupTable(
        topTermsTable,
        ListSelectionModel.SINGLE_SELECTION,
        new TopTermsTableModel(),
        new MouseAdapter() {
          @Override
          public void mouseClicked(MouseEvent e) {
            listeners.showTopTermsContextMenu(e);
          }
        },
        TopTermsTableModel.Column.RANK.getColumnWidth(),
        TopTermsTableModel.Column.FREQ.getColumnWidth());
    JScrollPane scrollPane = new JScrollPane(topTermsTable);
    termsPanel.add(scrollPane, BorderLayout.CENTER);

    JSplitPane splitPane = new JSplitPane(JSplitPane.HORIZONTAL_SPLIT, selectedPanel, termsPanel);
    splitPane.setOpaque(false);
    splitPane.setDividerLocation(180);
    splitPane.setBorder(BorderFactory.createEmptyBorder());
    panel.add(splitPane);

    return panel;
  }

  private void setUpTopTermsContextMenu() {
    JMenuItem item1 =
        new JMenuItem(MessageUtils.getLocalizedMessage("overview.toptermtable.menu.item1"));
    item1.addActionListener(listeners::browseByTerm);
    topTermsContextMenu.add(item1);

    JMenuItem item2 =
        new JMenuItem(MessageUtils.getLocalizedMessage("overview.toptermtable.menu.item2"));
    item2.addActionListener(listeners::searchByTerm);
    topTermsContextMenu.add(item2);
  }

  // control methods

  private void selectField() {
    String field = getSelectedField();
    selectedField.setText(field);
    showTopTermsBtn.setEnabled(true);
  }

  private void showTopTerms() {
    String field = getSelectedField();
    int numTerms = (int) numTopTermsSpnr.getModel().getValue();
    List<TermStats> termStats = overviewModel.getTopTerms(field, numTerms);

    // update top terms table
    topTermsTable.setModel(new TopTermsTableModel(termStats, numTerms));
    topTermsTable
        .getColumnModel()
        .getColumn(TopTermsTableModel.Column.RANK.getIndex())
        .setMaxWidth(TopTermsTableModel.Column.RANK.getColumnWidth());
    topTermsTable
        .getColumnModel()
        .getColumn(TopTermsTableModel.Column.FREQ.getIndex())
        .setMaxWidth(TopTermsTableModel.Column.FREQ.getColumnWidth());
    messageBroker.clearStatusMessage();
  }

  private void browseByTerm() {
    String field = getSelectedField();
    String term = getSelectedTerm();
    operatorRegistry
        .get(DocumentsTabOperator.class)
        .ifPresent(
            operator -> {
              operator.browseTerm(field, term);
              tabSwitcher.switchTab(TabbedPaneProvider.Tab.DOCUMENTS);
            });
  }

  private void searchByTerm() {
    String field = getSelectedField();
    String term = getSelectedTerm();
    operatorRegistry
        .get(SearchTabOperator.class)
        .ifPresent(
            operator -> {
              operator.searchByTerm(field, term);
              tabSwitcher.switchTab(TabbedPaneProvider.Tab.SEARCH);
            });
  }

  private String getSelectedField() {
    int selected = termCountsTable.getSelectedRow();
    // need to convert selected row index to underlying model index
    // https://docs.oracle.com/javase/8/docs/api/javax/swing/table/TableRowSorter.html
    int row = termCountsTable.convertRowIndexToModel(selected);
    if (row < 0 || row >= termCountsTable.getRowCount()) {
      throw new IllegalStateException("Field is not selected.");
    }
    return (String)
        termCountsTable.getModel().getValueAt(row, TermCountsTableModel.Column.NAME.getIndex());
  }

  private String getSelectedTerm() {
    int rowTerm = topTermsTable.getSelectedRow();
    if (rowTerm < 0 || rowTerm >= topTermsTable.getRowCount()) {
      throw new IllegalStateException("Term is not selected.");
    }
    return (String)
        topTermsTable.getModel().getValueAt(rowTerm, TopTermsTableModel.Column.TEXT.getIndex());
  }

  private class ListenerFunctions {

    void selectField(MouseEvent e) {
      OverviewPanelProvider.this.selectField();
    }

    void showTopTerms(ActionEvent e) {
      OverviewPanelProvider.this.showTopTerms();
    }

    void showTopTermsContextMenu(MouseEvent e) {
      if (e.getClickCount() == 2 && !e.isConsumed()) {
        int row = topTermsTable.rowAtPoint(e.getPoint());
        if (row != topTermsTable.getSelectedRow()) {
          topTermsTable.changeSelection(row, topTermsTable.getSelectedColumn(), false, false);
        }
        topTermsContextMenu.show(e.getComponent(), e.getX(), e.getY());
      }
    }

    void browseByTerm(ActionEvent e) {
      OverviewPanelProvider.this.browseByTerm();
    }

    void searchByTerm(ActionEvent e) {
      OverviewPanelProvider.this.searchByTerm();
    }
  }

  private class Observer implements IndexObserver {

    @Override
    public void openIndex(LukeState state) {
      overviewModel = overviewFactory.newInstance(state.getIndexReader(), state.getIndexPath());

      indexPathLbl.setText(overviewModel.getIndexPath());
      indexPathLbl.setToolTipText(overviewModel.getIndexPath());
      numFieldsLbl.setText(Integer.toString(overviewModel.getNumFields()));
      numDocsLbl.setText(Integer.toString(overviewModel.getNumDocuments()));
      numTermsLbl.setText(Long.toString(overviewModel.getNumTerms()));
      String del =
          overviewModel.hasDeletions()
              ? String.format(Locale.ENGLISH, "Yes (%d)", overviewModel.getNumDeletedDocs())
              : "No";
      String opt = overviewModel.isOptimized().map(b -> b ? "Yes" : "No").orElse("?");
      delOptLbl.setText(del + " / " + opt);
      indexVerLbl.setText(overviewModel.getIndexVersion().map(v -> Long.toString(v)).orElse("?"));
      indexFmtLbl.setText(overviewModel.getIndexFormat().orElse(""));
      dirImplLbl.setText(overviewModel.getDirImpl().orElse(""));
      commitPointLbl.setText(overviewModel.getCommitDescription().orElse("---"));
      commitUserDataLbl.setText(overviewModel.getCommitUserData().orElse("---"));

      // term counts table
      Map<String, Long> termCounts = overviewModel.getSortedTermCounts(TermCountsOrder.COUNT_DESC);
      long numTerms = overviewModel.getNumTerms();
      termCountsTable.setModel(new TermCountsTableModel(numTerms, termCounts));
      termCountsTable.setRowSorter(new TableRowSorter<>(termCountsTable.getModel()));
      termCountsTable
          .getColumnModel()
          .getColumn(TermCountsTableModel.Column.NAME.getIndex())
          .setMaxWidth(TermCountsTableModel.Column.NAME.getColumnWidth());
      termCountsTable
          .getColumnModel()
          .getColumn(TermCountsTableModel.Column.TERM_COUNT.getIndex())
          .setMaxWidth(TermCountsTableModel.Column.TERM_COUNT.getColumnWidth());
      DefaultTableCellRenderer rightRenderer = new DefaultTableCellRenderer();
      rightRenderer.setHorizontalAlignment(JLabel.RIGHT);
      termCountsTable
          .getColumnModel()
          .getColumn(TermCountsTableModel.Column.RATIO.getIndex())
          .setCellRenderer(rightRenderer);

      // top terms table
      topTermsTable.setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
      topTermsTable
          .getColumnModel()
          .getColumn(TopTermsTableModel.Column.RANK.getIndex())
          .setMaxWidth(TopTermsTableModel.Column.RANK.getColumnWidth());
      topTermsTable
          .getColumnModel()
          .getColumn(TopTermsTableModel.Column.FREQ.getIndex())
          .setMaxWidth(TopTermsTableModel.Column.FREQ.getColumnWidth());
      topTermsTable.getColumnModel().setColumnMargin(StyleConstants.TABLE_COLUMN_MARGIN_DEFAULT);
    }

    @Override
    public void closeIndex() {
      indexPathLbl.setText("");
      numFieldsLbl.setText("");
      numDocsLbl.setText("");
      numTermsLbl.setText("");
      delOptLbl.setText("");
      indexVerLbl.setText("");
      indexFmtLbl.setText("");
      dirImplLbl.setText("");
      commitPointLbl.setText("");
      commitUserDataLbl.setText("");

      selectedField.setText("");
      showTopTermsBtn.setEnabled(false);

      termCountsTable.setRowSorter(null);
      termCountsTable.setModel(new TermCountsTableModel());
      topTermsTable.setModel(new TopTermsTableModel());
    }
  }

  static final class TermCountsTableModel extends TableModelBase<TermCountsTableModel.Column> {

    enum Column implements TableColumnInfo {
      NAME("Name", 0, String.class, 150),
      TERM_COUNT("Term count", 1, Long.class, 100),
      RATIO("%", 2, String.class, Integer.MAX_VALUE);

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

    TermCountsTableModel() {
      super();
    }

    TermCountsTableModel(double numTerms, Map<String, Long> termCounts) {
      super(termCounts.size());
      int i = 0;
      for (Map.Entry<String, Long> e : termCounts.entrySet()) {
        String term = e.getKey();
        Long count = e.getValue();
        data[i++] =
            new Object[] {
              term, count, String.format(Locale.ENGLISH, "%.2f %%", count / numTerms * 100)
            };
      }
    }

    @Override
    protected Column[] columnInfos() {
      return Column.values();
    }
  }

  static final class TopTermsTableModel extends TableModelBase<TopTermsTableModel.Column> {

    enum Column implements TableColumnInfo {
      RANK("Rank", 0, Integer.class, 50),
      FREQ("Freq", 1, Integer.class, 80),
      TEXT("Text", 2, String.class, Integer.MAX_VALUE);

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

    TopTermsTableModel() {
      super();
    }

    TopTermsTableModel(List<TermStats> termStats, int numTerms) {
      super(Math.min(numTerms, termStats.size()));
      for (int i = 0; i < data.length; i++) {
        int rank = i + 1;
        int freq = termStats.get(i).getDocFreq();
        String termText = termStats.get(i).getDecodedTermText();
        data[i] = new Object[] {rank, freq, termText};
      }
    }

    @Override
    protected Column[] columnInfos() {
      return Column.values();
    }
  }
}
