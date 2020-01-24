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

import javax.swing.BorderFactory;
import javax.swing.BoxLayout;
import javax.swing.JButton;
import javax.swing.JComboBox;
import javax.swing.JComponent;
import javax.swing.JLabel;
import javax.swing.JList;
import javax.swing.JMenuItem;
import javax.swing.JPanel;
import javax.swing.JPopupMenu;
import javax.swing.JScrollPane;
import javax.swing.JSpinner;
import javax.swing.JSplitPane;
import javax.swing.JTable;
import javax.swing.JTextField;
import javax.swing.ListSelectionModel;
import javax.swing.SpinnerModel;
import javax.swing.SpinnerNumberModel;
import javax.swing.event.ChangeEvent;
import javax.swing.table.TableCellRenderer;
import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.GridLayout;
import java.awt.Insets;
import java.awt.Toolkit;
import java.awt.datatransfer.Clipboard;
import java.awt.datatransfer.StringSelection;
import java.awt.event.ActionEvent;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.Term;
import org.apache.lucene.luke.app.IndexHandler;
import org.apache.lucene.luke.app.IndexObserver;
import org.apache.lucene.luke.app.LukeState;
import org.apache.lucene.luke.app.desktop.MessageBroker;
import org.apache.lucene.luke.app.desktop.components.dialog.HelpDialogFactory;
import org.apache.lucene.luke.app.desktop.components.dialog.documents.AddDocumentDialogFactory;
import org.apache.lucene.luke.app.desktop.components.dialog.documents.DocValuesDialogFactory;
import org.apache.lucene.luke.app.desktop.components.dialog.documents.StoredValueDialogFactory;
import org.apache.lucene.luke.app.desktop.components.dialog.documents.TermVectorDialogFactory;
import org.apache.lucene.luke.app.desktop.util.DialogOpener;
import org.apache.lucene.luke.app.desktop.util.FontUtils;
import org.apache.lucene.luke.app.desktop.util.HelpHeaderRenderer;
import org.apache.lucene.luke.app.desktop.util.MessageUtils;
import org.apache.lucene.luke.app.desktop.util.StyleConstants;
import org.apache.lucene.luke.app.desktop.util.TableUtils;
import org.apache.lucene.luke.models.documents.DocValues;
import org.apache.lucene.luke.models.documents.DocumentField;
import org.apache.lucene.luke.models.documents.Documents;
import org.apache.lucene.luke.models.documents.DocumentsFactory;
import org.apache.lucene.luke.models.documents.TermPosting;
import org.apache.lucene.luke.models.documents.TermVectorEntry;
import org.apache.lucene.luke.util.BytesRefUtils;

/** Provider of the Documents panel */
public final class DocumentsPanelProvider implements DocumentsTabOperator {

  private final DocumentsFactory documentsFactory = new DocumentsFactory();

  private final MessageBroker messageBroker;

  private final ComponentOperatorRegistry operatorRegistry;

  private final TabSwitcherProxy tabSwitcher;

  private final AddDocumentDialogFactory addDocDialogFactory;

  private final TermVectorDialogFactory tvDialogFactory;

  private final DocValuesDialogFactory dvDialogFactory;

  private final StoredValueDialogFactory valueDialogFactory;

  private final TableCellRenderer tableHeaderRenderer;

  private final JComboBox<String> fieldsCombo = new JComboBox<>();

  private final JButton firstTermBtn = new JButton();

  private final JTextField termTF = new JTextField();

  private final JButton nextTermBtn = new JButton();

  private final JTextField selectedTermTF = new JTextField();

  private final JButton firstTermDocBtn = new JButton();

  private final JTextField termDocIdxTF = new JTextField();

  private final JButton nextTermDocBtn = new JButton();

  private final JLabel termDocsNumLbl = new JLabel();

  private final JTable posTable = new JTable();

  private final JSpinner docNumSpnr = new JSpinner();

  private final JLabel maxDocsLbl = new JLabel();

  private final JButton mltBtn = new JButton();

  private final JButton addDocBtn = new JButton();

  private final JButton copyDocValuesBtn = new JButton();

  private final JTable documentTable = new JTable();

  private final JPopupMenu documentContextMenu = new JPopupMenu();

  private final ListenerFunctions listeners = new ListenerFunctions();

  private Documents documentsModel;

  public DocumentsPanelProvider() throws IOException {
    this.messageBroker = MessageBroker.getInstance();
    this.operatorRegistry = ComponentOperatorRegistry.getInstance();
    this.tabSwitcher = TabSwitcherProxy.getInstance();
    this.addDocDialogFactory = AddDocumentDialogFactory.getInstance();
    this.tvDialogFactory = TermVectorDialogFactory.getInstance();
    this.dvDialogFactory = DocValuesDialogFactory.getInstance();
    this.valueDialogFactory = StoredValueDialogFactory.getInstance();
    HelpDialogFactory helpDialogFactory = HelpDialogFactory.getInstance();
    this.tableHeaderRenderer = new HelpHeaderRenderer(
        "About Flags", "Format: IdfpoNPSB#txxVDtxxxxTx/x",
        createFlagsHelpDialog(), helpDialogFactory);

    IndexHandler.getInstance().addObserver(new Observer());
    operatorRegistry.register(DocumentsTabOperator.class, this);
  }

  private JComponent createFlagsHelpDialog() {
    String[] values = new String[]{
        "I - index options(docs, frequencies, positions, offsets)",
        "N - norms",
        "P - payloads",
        "S - stored",
        "B - binary stored values",
        "#txx - numeric stored values(type, precision)",
        "V - term vectors",
        "Dtxxxxx - doc values(type)",
        "Tx/x - point values(num bytes/dimension)"
    };
    JList<String> list = new JList<>(values);
    return new JScrollPane(list);
  }

  public JPanel get() {
    JPanel panel = new JPanel(new GridLayout(1, 1));
    panel.setOpaque(false);
    panel.setBorder(BorderFactory.createLineBorder(Color.gray));

    JSplitPane splitPane = new JSplitPane(JSplitPane.VERTICAL_SPLIT, initUpperPanel(), initLowerPanel());
    splitPane.setOpaque(false);
    splitPane.setDividerLocation(0.4);
    panel.add(splitPane);

    setUpDocumentContextMenu();

    return panel;
  }

  private JPanel initUpperPanel() {
    JPanel panel = new JPanel(new GridBagLayout());
    panel.setOpaque(false);
    GridBagConstraints c = new GridBagConstraints();

    c.gridx = 0;
    c.gridy = 0;
    c.weightx = 0.5;
    c.anchor = GridBagConstraints.FIRST_LINE_START;
    c.fill = GridBagConstraints.HORIZONTAL;
    panel.add(initBrowseTermsPanel(), c);

    c.gridx = 1;
    c.gridy = 0;
    c.weightx = 0.5;
    c.anchor = GridBagConstraints.FIRST_LINE_START;
    c.fill = GridBagConstraints.HORIZONTAL;
    panel.add(initBrowseDocsByTermPanel(), c);

    return panel;
  }

  private JPanel initBrowseTermsPanel() {
    JPanel panel = new JPanel(new BorderLayout());
    panel.setOpaque(false);
    panel.setBorder(BorderFactory.createEmptyBorder(3, 3, 3, 3));

    JPanel top = new JPanel(new FlowLayout(FlowLayout.LEADING));
    top.setOpaque(false);
    JLabel label = new JLabel(MessageUtils.getLocalizedMessage("documents.label.browse_terms"));
    top.add(label);

    panel.add(top, BorderLayout.PAGE_START);

    JPanel center = new JPanel(new GridBagLayout());
    center.setOpaque(false);
    GridBagConstraints c = new GridBagConstraints();
    c.fill = GridBagConstraints.BOTH;

    fieldsCombo.addActionListener(listeners::showFirstTerm);
    c.gridx = 0;
    c.gridy = 0;
    c.insets = new Insets(5, 5, 5, 5);
    c.weightx = 0.0;
    c.gridwidth = 2;
    center.add(fieldsCombo, c);

    firstTermBtn.setText(FontUtils.elegantIconHtml("&#x38;", MessageUtils.getLocalizedMessage("documents.button.first_term")));
    firstTermBtn.setMaximumSize(new Dimension(80, 30));
    firstTermBtn.addActionListener(listeners::showFirstTerm);
    c.gridx = 0;
    c.gridy = 1;
    c.insets = new Insets(5, 5, 5, 5);
    c.weightx = 0.2;
    c.gridwidth = 1;
    center.add(firstTermBtn, c);

    termTF.setColumns(20);
    termTF.setMinimumSize(new Dimension(50, 25));
    termTF.setFont(StyleConstants.FONT_MONOSPACE_LARGE);
    termTF.addActionListener(listeners::seekNextTerm);
    c.gridx = 1;
    c.gridy = 1;
    c.insets = new Insets(5, 5, 5, 5);
    c.weightx = 0.5;
    c.gridwidth = 1;
    center.add(termTF, c);

    nextTermBtn.setText(MessageUtils.getLocalizedMessage("documents.button.next"));
    nextTermBtn.addActionListener(listeners::showNextTerm);
    c.gridx = 2;
    c.gridy = 1;
    c.insets = new Insets(5, 5, 5, 5);
    c.weightx = 0.1;
    c.gridwidth = 1;
    center.add(nextTermBtn, c);

    panel.add(center, BorderLayout.CENTER);

    JPanel footer = new JPanel(new FlowLayout(FlowLayout.LEADING, 20, 5));
    footer.setOpaque(false);
    JLabel hintLbl = new JLabel(MessageUtils.getLocalizedMessage("documents.label.browse_terms_hint"));
    footer.add(hintLbl);
    panel.add(footer, BorderLayout.PAGE_END);

    return panel;
  }

  private JPanel initBrowseDocsByTermPanel() {
    JPanel panel = new JPanel(new BorderLayout());
    panel.setOpaque(false);
    panel.setBorder(BorderFactory.createEmptyBorder(3, 3, 3, 3));

    JPanel center = new JPanel(new GridBagLayout());
    center.setOpaque(false);
    GridBagConstraints c = new GridBagConstraints();
    c.fill = GridBagConstraints.BOTH;

    JLabel label = new JLabel(MessageUtils.getLocalizedMessage("documents.label.browse_doc_by_term"));
    c.gridx = 0;
    c.gridy = 0;
    c.weightx = 0.0;
    c.gridwidth = 2;
    c.insets = new Insets(5, 5, 5, 5);
    center.add(label, c);

    selectedTermTF.setColumns(20);
    selectedTermTF.setFont(StyleConstants.FONT_MONOSPACE_LARGE);
    selectedTermTF.setEditable(false);
    selectedTermTF.setBackground(Color.white);
    c.gridx = 0;
    c.gridy = 1;
    c.weightx = 0.0;
    c.gridwidth = 2;
    c.insets = new Insets(5, 5, 5, 5);
    center.add(selectedTermTF, c);

    firstTermDocBtn.setText(FontUtils.elegantIconHtml("&#x38;", MessageUtils.getLocalizedMessage("documents.button.first_termdoc")));
    firstTermDocBtn.addActionListener(listeners::showFirstTermDoc);
    c.gridx = 0;
    c.gridy = 2;
    c.weightx = 0.2;
    c.gridwidth = 1;
    c.insets = new Insets(5, 3, 5, 5);
    center.add(firstTermDocBtn, c);

    termDocIdxTF.setEditable(false);
    termDocIdxTF.setBackground(Color.white);
    c.gridx = 1;
    c.gridy = 2;
    c.weightx = 0.5;
    c.gridwidth = 1;
    c.insets = new Insets(5, 5, 5, 5);
    center.add(termDocIdxTF, c);

    nextTermDocBtn.setText(MessageUtils.getLocalizedMessage("documents.button.next"));
    nextTermDocBtn.addActionListener(listeners::showNextTermDoc);
    c.gridx = 2;
    c.gridy = 2;
    c.weightx = 0.2;
    c.gridwidth = 1;
    c.insets = new Insets(5, 5, 5, 5);
    center.add(nextTermDocBtn, c);

    termDocsNumLbl.setText("in ? docs");
    c.gridx = 3;
    c.gridy = 2;
    c.weightx = 0.3;
    c.gridwidth = 1;
    c.insets = new Insets(5, 5, 5, 5);
    center.add(termDocsNumLbl, c);

    TableUtils.setupTable(posTable, ListSelectionModel.SINGLE_SELECTION, new PosTableModel(), null,
        PosTableModel.Column.POSITION.getColumnWidth(), PosTableModel.Column.OFFSETS.getColumnWidth(), PosTableModel.Column.PAYLOAD.getColumnWidth());
    JScrollPane scrollPane = new JScrollPane(posTable);
    scrollPane.setMinimumSize(new Dimension(100, 100));
    c.gridx = 0;
    c.gridy = 3;
    c.gridwidth = 4;
    c.insets = new Insets(5, 5, 5, 5);
    center.add(scrollPane, c);

    panel.add(center, BorderLayout.CENTER);

    return panel;
  }

  private JPanel initLowerPanel() {
    JPanel panel = new JPanel(new BorderLayout());
    panel.setOpaque(false);
    panel.setBorder(BorderFactory.createEmptyBorder(3, 3, 3, 3));

    JPanel browseDocsPanel = new JPanel();
    browseDocsPanel.setOpaque(false);
    browseDocsPanel.setLayout(new BoxLayout(browseDocsPanel, BoxLayout.PAGE_AXIS));
    browseDocsPanel.add(initBrowseDocsBar());

    JPanel browseDocsNote1 = new JPanel(new FlowLayout(FlowLayout.LEADING));
    browseDocsNote1.setOpaque(false);
    browseDocsNote1.add(new JLabel(MessageUtils.getLocalizedMessage("documents.label.doc_table_note1")));
    browseDocsPanel.add(browseDocsNote1);

    JPanel browseDocsNote2 = new JPanel(new FlowLayout(FlowLayout.LEADING));
    browseDocsNote2.setOpaque(false);
    browseDocsNote2.add(new JLabel(MessageUtils.getLocalizedMessage("documents.label.doc_table_note2")));
    browseDocsPanel.add(browseDocsNote2);

    panel.add(browseDocsPanel, BorderLayout.PAGE_START);

    TableUtils.setupTable(documentTable, ListSelectionModel.MULTIPLE_INTERVAL_SELECTION, new DocumentsTableModel(), new MouseAdapter() {
          @Override
          public void mouseClicked(MouseEvent e) {
            listeners.showDocumentContextMenu(e);
          }
        },
        DocumentsTableModel.Column.FIELD.getColumnWidth(),
        DocumentsTableModel.Column.FLAGS.getColumnWidth(),
        DocumentsTableModel.Column.NORM.getColumnWidth(),
        DocumentsTableModel.Column.VALUE.getColumnWidth());
    JPanel flagsHeader = new JPanel(new FlowLayout(FlowLayout.CENTER));
    flagsHeader.setOpaque(false);
    flagsHeader.add(new JLabel("Flags"));
    flagsHeader.add(new JLabel("Help"));
    documentTable.getColumnModel().getColumn(DocumentsTableModel.Column.FLAGS.getIndex()).setHeaderValue(flagsHeader);

    JScrollPane scrollPane = new JScrollPane(documentTable);
    scrollPane.getHorizontalScrollBar().setAutoscrolls(false);
    panel.add(scrollPane, BorderLayout.CENTER);

    return panel;
  }

  private JPanel initBrowseDocsBar() {
    JPanel panel = new JPanel(new GridLayout(1, 2));
    panel.setOpaque(false);
    panel.setBorder(BorderFactory.createEmptyBorder(5, 0, 0, 5));

    JPanel left = new JPanel(new FlowLayout(FlowLayout.LEADING, 10, 2));
    left.setOpaque(false);
    JLabel label = new JLabel(FontUtils.elegantIconHtml("&#x68;", MessageUtils.getLocalizedMessage("documents.label.browse_doc_by_idx")));
    label.setHorizontalTextPosition(JLabel.LEFT);
    left.add(label);
    docNumSpnr.setPreferredSize(new Dimension(100, 25));
    docNumSpnr.addChangeListener(listeners::showCurrentDoc);
    left.add(docNumSpnr);
    maxDocsLbl.setText("in ? docs");
    left.add(maxDocsLbl);
    panel.add(left);

    JPanel right = new JPanel(new FlowLayout(FlowLayout.TRAILING));
    right.setOpaque(false);
    copyDocValuesBtn.setText(FontUtils.elegantIconHtml("&#xe0e6;", MessageUtils.getLocalizedMessage("documents.buttont.copy_values")));
    copyDocValuesBtn.setMargin(new Insets(5, 0, 5, 0));
    copyDocValuesBtn.addActionListener(listeners::copySelectedOrAllStoredValues);
    right.add(copyDocValuesBtn);
    mltBtn.setText(FontUtils.elegantIconHtml("&#xe030;", MessageUtils.getLocalizedMessage("documents.button.mlt")));
    mltBtn.setMargin(new Insets(5, 0, 5, 0));
    mltBtn.addActionListener(listeners::mltSearch);
    right.add(mltBtn);
    addDocBtn.setText(FontUtils.elegantIconHtml("&#x59;", MessageUtils.getLocalizedMessage("documents.button.add")));
    addDocBtn.setMargin(new Insets(5, 0, 5, 0));
    addDocBtn.addActionListener(listeners::showAddDocumentDialog);
    right.add(addDocBtn);
    panel.add(right);

    return panel;
  }

  private void setUpDocumentContextMenu() {
    // show term vector
    JMenuItem item1 = new JMenuItem(MessageUtils.getLocalizedMessage("documents.doctable.menu.item1"));
    item1.addActionListener(listeners::showTermVectorDialog);
    documentContextMenu.add(item1);

    // show doc values
    JMenuItem item2 = new JMenuItem(MessageUtils.getLocalizedMessage("documents.doctable.menu.item2"));
    item2.addActionListener(listeners::showDocValuesDialog);
    documentContextMenu.add(item2);

    // show stored value
    JMenuItem item3 = new JMenuItem(MessageUtils.getLocalizedMessage("documents.doctable.menu.item3"));
    item3.addActionListener(listeners::showStoredValueDialog);
    documentContextMenu.add(item3);

    // copy stored value to clipboard
    JMenuItem item4 = new JMenuItem(MessageUtils.getLocalizedMessage("documents.doctable.menu.item4"));
    item4.addActionListener(listeners::copyStoredValue);
    documentContextMenu.add(item4);
  }

  // control methods

  private void showFirstTerm() {
    String fieldName = (String) fieldsCombo.getSelectedItem();
    if (fieldName == null || fieldName.length() == 0) {
      messageBroker.showStatusMessage(MessageUtils.getLocalizedMessage("documents.field.message.not_selected"));
      return;
    }

    termDocIdxTF.setText("");
    clearPosTable();

    Optional<Term> firstTerm = documentsModel.firstTerm(fieldName);
    String firstTermText = firstTerm.map(Term::text).orElse("");
    termTF.setText(firstTermText);
    selectedTermTF.setText(firstTermText);
    if (firstTerm.isPresent()) {
      String num = documentsModel.getDocFreq().map(String::valueOf).orElse("?");
      termDocsNumLbl.setText("in " + num + " docs");

      nextTermBtn.setEnabled(true);
      termTF.setEditable(true);
      firstTermDocBtn.setEnabled(true);
    } else {
      nextTermBtn.setEnabled(false);
      termTF.setEditable(false);
      firstTermDocBtn.setEnabled(false);
    }
    nextTermDocBtn.setEnabled(false);
    messageBroker.clearStatusMessage();
  }

  private void showNextTerm() {
    termDocIdxTF.setText("");
    clearPosTable();

    Optional<Term> nextTerm = documentsModel.nextTerm();
    String nextTermText = nextTerm.map(Term::text).orElse("");
    termTF.setText(nextTermText);
    selectedTermTF.setText(nextTermText);
    if (nextTerm.isPresent()) {
      String num = documentsModel.getDocFreq().map(String::valueOf).orElse("?");
      termDocsNumLbl.setText("in " + num + " docs");

      termTF.setEditable(true);
      firstTermDocBtn.setEnabled(true);
    } else {
      nextTermBtn.setEnabled(false);
      termTF.setEditable(false);
      firstTermDocBtn.setEnabled(false);
    }
    nextTermDocBtn.setEnabled(false);
    messageBroker.clearStatusMessage();
  }

  @Override
  public void seekNextTerm() {
    termDocIdxTF.setText("");
    posTable.setModel(new PosTableModel());

    String termText = termTF.getText();

    Optional<Term> nextTerm = documentsModel.seekTerm(termText);
    String nextTermText = nextTerm.map(Term::text).orElse("");
    termTF.setText(nextTermText);
    selectedTermTF.setText(nextTermText);
    if (nextTerm.isPresent()) {
      String num = documentsModel.getDocFreq().map(String::valueOf).orElse("?");
      termDocsNumLbl.setText("in " + num + " docs");

      termTF.setEditable(true);
      firstTermDocBtn.setEnabled(true);
    } else {
      nextTermBtn.setEnabled(false);
      termTF.setEditable(false);
      firstTermDocBtn.setEnabled(false);
    }
    nextTermDocBtn.setEnabled(false);
    messageBroker.clearStatusMessage();
  }


  private void clearPosTable() {
    TableUtils.setupTable(posTable, ListSelectionModel.SINGLE_SELECTION, new PosTableModel(), null,
        PosTableModel.Column.POSITION.getColumnWidth(),
        PosTableModel.Column.OFFSETS.getColumnWidth(),
        PosTableModel.Column.PAYLOAD.getColumnWidth());
  }

  @Override
  public void showFirstTermDoc() {
    int docid = documentsModel.firstTermDoc().orElse(-1);
    if (docid < 0) {
      nextTermDocBtn.setEnabled(false);
      messageBroker.showStatusMessage(MessageUtils.getLocalizedMessage("documents.termdocs.message.not_available"));
      return;
    }
    termDocIdxTF.setText(String.valueOf(1));
    displayDoc(docid);

    List<TermPosting> postings = documentsModel.getTermPositions();
    posTable.setModel(new PosTableModel(postings));
    posTable.getColumnModel().getColumn(PosTableModel.Column.POSITION.getIndex()).setPreferredWidth(PosTableModel.Column.POSITION.getColumnWidth());
    posTable.getColumnModel().getColumn(PosTableModel.Column.OFFSETS.getIndex()).setPreferredWidth(PosTableModel.Column.OFFSETS.getColumnWidth());
    posTable.getColumnModel().getColumn(PosTableModel.Column.PAYLOAD.getIndex()).setPreferredWidth(PosTableModel.Column.PAYLOAD.getColumnWidth());

    nextTermDocBtn.setEnabled(true);
    messageBroker.clearStatusMessage();
  }

  private void showNextTermDoc() {
    int docid = documentsModel.nextTermDoc().orElse(-1);
    if (docid < 0) {
      nextTermDocBtn.setEnabled(false);
      messageBroker.showStatusMessage(MessageUtils.getLocalizedMessage("documents.termdocs.message.not_available"));
      return;
    }
    int curIdx = Integer.parseInt(termDocIdxTF.getText());
    termDocIdxTF.setText(String.valueOf(curIdx + 1));
    displayDoc(docid);

    List<TermPosting> postings = documentsModel.getTermPositions();
    posTable.setModel(new PosTableModel(postings));

    nextTermDocBtn.setDefaultCapable(true);
    messageBroker.clearStatusMessage();
  }

  private void showCurrentDoc() {
    int docid = (Integer) docNumSpnr.getValue();
    displayDoc(docid);
  }

  private void mltSearch() {
    int docNum = (int) docNumSpnr.getValue();
    operatorRegistry.get(SearchTabOperator.class).ifPresent(operator -> {
      operator.mltSearch(docNum);
      tabSwitcher.switchTab(TabbedPaneProvider.Tab.SEARCH);
    });
  }

  private void showAddDocumentDialog() {
    new DialogOpener<>(addDocDialogFactory).open("Add document", 600, 500,
        (factory) -> {
        });
  }

  private void showTermVectorDialog() {
    int docid = (Integer) docNumSpnr.getValue();
    String field = (String) documentTable.getModel().getValueAt(documentTable.getSelectedRow(), DocumentsTableModel.Column.FIELD.getIndex());
    List<TermVectorEntry> tvEntries = documentsModel.getTermVectors(docid, field);
    if (tvEntries.isEmpty()) {
      messageBroker.showStatusMessage(MessageUtils.getLocalizedMessage("documents.termvector.message.not_available", field, docid));
      return;
    }

    new DialogOpener<>(tvDialogFactory).open(
        "Term Vector", 600, 400,
        (factory) -> {
          factory.setField(field);
          factory.setTvEntries(tvEntries);
        });
    messageBroker.clearStatusMessage();
  }

  private void showDocValuesDialog() {
    int docid = (Integer) docNumSpnr.getValue();
    String field = (String) documentTable.getModel().getValueAt(documentTable.getSelectedRow(), DocumentsTableModel.Column.FIELD.getIndex());
    Optional<DocValues> docValues = documentsModel.getDocValues(docid, field);
    if (docValues.isPresent()) {
      new DialogOpener<>(dvDialogFactory).open(
          "Doc Values", 400, 300,
          (factory) -> {
            factory.setValue(field, docValues.get());
          });
      messageBroker.clearStatusMessage();
    } else {
      messageBroker.showStatusMessage(MessageUtils.getLocalizedMessage("documents.docvalues.message.not_available", field, docid));
    }
  }

  private void showStoredValueDialog() {
    int docid = (Integer) docNumSpnr.getValue();
    String field = (String) documentTable.getModel().getValueAt(documentTable.getSelectedRow(), DocumentsTableModel.Column.FIELD.getIndex());
    String value = (String) documentTable.getModel().getValueAt(documentTable.getSelectedRow(), DocumentsTableModel.Column.VALUE.getIndex());
    if (Objects.isNull(value)) {
      messageBroker.showStatusMessage(MessageUtils.getLocalizedMessage("documents.stored.message.not_availabe", field, docid));
      return;
    }
    new DialogOpener<>(valueDialogFactory).open(
        "Stored Value", 400, 300,
        (factory) -> {
          factory.setField(field);
          factory.setValue(value);
        });
    messageBroker.clearStatusMessage();
  }

  private void copyStoredValue() {
    int docid = (Integer) docNumSpnr.getValue();
    String field = (String) documentTable.getModel().getValueAt(documentTable.getSelectedRow(), DocumentsTableModel.Column.FIELD.getIndex());
    String value = (String) documentTable.getModel().getValueAt(documentTable.getSelectedRow(), DocumentsTableModel.Column.VALUE.getIndex());
    if (Objects.isNull(value)) {
      messageBroker.showStatusMessage(MessageUtils.getLocalizedMessage("documents.stored.message.not_availabe", field, docid));
      return;
    }
    Clipboard clipboard = Toolkit.getDefaultToolkit().getSystemClipboard();
    StringSelection selection = new StringSelection(value);
    clipboard.setContents(selection, null);
    messageBroker.clearStatusMessage();
  }

  private void copySelectedOrAllStoredValues() {
    StringSelection selection;
    if (documentTable.getSelectedRowCount() == 0) {
      selection = copyAllValues();
    } else {
      selection = copySelectedValues();
    }
    Clipboard clipboard = Toolkit.getDefaultToolkit().getSystemClipboard();
    clipboard.setContents(selection, null);
    messageBroker.clearStatusMessage();
  }

  private StringSelection copyAllValues() {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < documentTable.getRowCount(); i++) {
      String value = (String) documentTable.getModel().getValueAt(i, DocumentsTableModel.Column.VALUE.getIndex());
      if (Objects.nonNull(value)) {
        sb.append((i == 0) ? value : System.lineSeparator() + value);
      }
    }
    return new StringSelection(sb.toString());
  }

  private StringSelection copySelectedValues() {
    StringBuilder sb = new StringBuilder();
    boolean isFirst = true;
    for (int rowIndex : documentTable.getSelectedRows()) {
      String value = (String) documentTable.getModel().getValueAt(rowIndex, DocumentsTableModel.Column.VALUE.getIndex());
      if (Objects.nonNull(value)) {
        sb.append(isFirst ? value : System.lineSeparator() + value);
        isFirst = false;
      }
    }
    return new StringSelection(sb.toString());
  }

  @Override
  public void browseTerm(String field, String term) {
    fieldsCombo.setSelectedItem(field);
    termTF.setText(term);
    seekNextTerm();
    showFirstTermDoc();
  }

  @Override
  public void displayLatestDoc() {
    int docid = documentsModel.getMaxDoc() - 1;
    showDoc(docid);
  }

  @Override
  public void displayDoc(int docid) {
    showDoc(docid);
  }

  ;

  private void showDoc(int docid) {
    docNumSpnr.setValue(docid);

    List<DocumentField> doc = documentsModel.getDocumentFields(docid);
    documentTable.setModel(new DocumentsTableModel(doc));
    documentTable.setFont(StyleConstants.FONT_MONOSPACE_LARGE);
    documentTable.getColumnModel().getColumn(DocumentsTableModel.Column.FIELD.getIndex()).setPreferredWidth(DocumentsTableModel.Column.FIELD.getColumnWidth());
    documentTable.getColumnModel().getColumn(DocumentsTableModel.Column.FLAGS.getIndex()).setMinWidth(DocumentsTableModel.Column.FLAGS.getColumnWidth());
    documentTable.getColumnModel().getColumn(DocumentsTableModel.Column.FLAGS.getIndex()).setMaxWidth(DocumentsTableModel.Column.FIELD.getColumnWidth());
    documentTable.getColumnModel().getColumn(DocumentsTableModel.Column.NORM.getIndex()).setMinWidth(DocumentsTableModel.Column.NORM.getColumnWidth());
    documentTable.getColumnModel().getColumn(DocumentsTableModel.Column.NORM.getIndex()).setMaxWidth(DocumentsTableModel.Column.NORM.getColumnWidth());
    documentTable.getColumnModel().getColumn(DocumentsTableModel.Column.VALUE.getIndex()).setPreferredWidth(DocumentsTableModel.Column.VALUE.getColumnWidth());
    documentTable.getColumnModel().getColumn(DocumentsTableModel.Column.FLAGS.getIndex()).setHeaderRenderer(tableHeaderRenderer);

    messageBroker.clearStatusMessage();
  }

  private class ListenerFunctions {

    void showFirstTerm(ActionEvent e) {
      DocumentsPanelProvider.this.showFirstTerm();
    }

    void seekNextTerm(ActionEvent e) {
      DocumentsPanelProvider.this.seekNextTerm();
    }

    void showNextTerm(ActionEvent e) {
      DocumentsPanelProvider.this.showNextTerm();
    }

    void showFirstTermDoc(ActionEvent e) {
      DocumentsPanelProvider.this.showFirstTermDoc();
    }

    void showNextTermDoc(ActionEvent e) {
      DocumentsPanelProvider.this.showNextTermDoc();
    }

    void showCurrentDoc(ChangeEvent e) {
      DocumentsPanelProvider.this.showCurrentDoc();
    }

    void mltSearch(ActionEvent e) {
      DocumentsPanelProvider.this.mltSearch();
    }

    void showAddDocumentDialog(ActionEvent e) {
      DocumentsPanelProvider.this.showAddDocumentDialog();
    }

    void showDocumentContextMenu(MouseEvent e) {
      if (e.getClickCount() == 2 && !e.isConsumed()) {
        int row = documentTable.rowAtPoint(e.getPoint());
        if (row != documentTable.getSelectedRow()) {
          documentTable.changeSelection(row, documentTable.getSelectedColumn(), false, false);
        }
        documentContextMenu.show(e.getComponent(), e.getX(), e.getY());
      }
    }

    void showTermVectorDialog(ActionEvent e) {
      DocumentsPanelProvider.this.showTermVectorDialog();
    }

    void showDocValuesDialog(ActionEvent e) {
      DocumentsPanelProvider.this.showDocValuesDialog();
    }

    void showStoredValueDialog(ActionEvent e) {
      DocumentsPanelProvider.this.showStoredValueDialog();
    }

    void copyStoredValue(ActionEvent e) {
      DocumentsPanelProvider.this.copyStoredValue();
    }

    void copySelectedOrAllStoredValues(ActionEvent e) {
      DocumentsPanelProvider.this.copySelectedOrAllStoredValues();
    }

  }

  private class Observer implements IndexObserver {

    @Override
    public void openIndex(LukeState state) {
      documentsModel = documentsFactory.newInstance(state.getIndexReader());

      addDocBtn.setEnabled(!state.readOnly() && state.hasDirectoryReader());

      int maxDoc = documentsModel.getMaxDoc();
      maxDocsLbl.setText("in " + maxDoc + " docs");
      if (maxDoc > 0) {
        int max = Math.max(maxDoc - 1, 0);
        SpinnerModel spinnerModel = new SpinnerNumberModel(0, 0, max, 1);
        docNumSpnr.setModel(spinnerModel);
        docNumSpnr.setEnabled(true);
        displayDoc(0);
      } else {
        docNumSpnr.setEnabled(false);
      }

      documentsModel.getFieldNames().stream().sorted().forEach(fieldsCombo::addItem);
    }

    @Override
    public void closeIndex() {
      maxDocsLbl.setText("in ? docs");
      docNumSpnr.setEnabled(false);
      fieldsCombo.removeAllItems();
      termTF.setText("");
      selectedTermTF.setText("");
      termDocsNumLbl.setText("");
      termDocIdxTF.setText("");

      posTable.setModel(new PosTableModel());
      documentTable.setModel(new DocumentsTableModel());
    }
  }

  static final class PosTableModel extends TableModelBase<PosTableModel.Column> {

    enum Column implements TableColumnInfo {

      POSITION("Position", 0, Integer.class, 80),
      OFFSETS("Offsets", 1, String.class, 120),
      PAYLOAD("Payload", 2, String.class, 300);

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

    PosTableModel() {
      super();
    }

    PosTableModel(List<TermPosting> postings) {
      super(postings.size());

      for (int i = 0; i < postings.size(); i++) {
        TermPosting p = postings.get(i);

        int position = postings.get(i).getPosition();
        String offset = null;
        if (p.getStartOffset() >= 0 && p.getEndOffset() >= 0) {
          offset = p.getStartOffset() + "-" + p.getEndOffset();
        }
        String payload = null;
        if (p.getPayload() != null) {
          payload = BytesRefUtils.decode(p.getPayload());
        }

        data[i] = new Object[]{position, offset, payload};
      }
    }

    @Override
    protected Column[] columnInfos() {
      return Column.values();
    }
  }

  static final class DocumentsTableModel extends TableModelBase<DocumentsTableModel.Column> {

    enum Column implements TableColumnInfo {
      FIELD("Field", 0, String.class, 150),
      FLAGS("Flags", 1, String.class, 200),
      NORM("Norm", 2, Long.class, 80),
      VALUE("Value", 3, String.class, 500);

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

    DocumentsTableModel() {
      super();
    }

    DocumentsTableModel(List<DocumentField> doc) {
      super(doc.size());

      for (int i = 0; i < doc.size(); i++) {
        DocumentField docField = doc.get(i);
        String field = docField.getName();
        String flags = flags(docField);
        long norm = docField.getNorm();
        String value = null;
        if (docField.getStringValue() != null) {
          value = docField.getStringValue();
        } else if (docField.getNumericValue() != null) {
          value = String.valueOf(docField.getNumericValue());
        } else if (docField.getBinaryValue() != null) {
          value = String.valueOf(docField.getBinaryValue());
        }
        data[i] = new Object[]{field, flags, norm, value};
      }
    }

    private static String flags(org.apache.lucene.luke.models.documents.DocumentField f) {
      StringBuilder sb = new StringBuilder();
      // index options
      if (f.getIdxOptions() == null || f.getIdxOptions() == IndexOptions.NONE) {
        sb.append("-----");
      } else {
        sb.append("I");
        switch (f.getIdxOptions()) {
          case DOCS:
            sb.append("d---");
            break;
          case DOCS_AND_FREQS:
            sb.append("df--");
            break;
          case DOCS_AND_FREQS_AND_POSITIONS:
            sb.append("dfp-");
            break;
          case DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS:
            sb.append("dfpo");
            break;
          default:
            sb.append("----");
        }
      }
      // has norm?
      if (f.hasNorms()) {
        sb.append("N");
      } else {
        sb.append("-");
      }
      // has payloads?
      if (f.hasPayloads()) {
        sb.append("P");
      } else {
        sb.append("-");
      }
      // stored?
      if (f.isStored()) {
        sb.append("S");
      } else {
        sb.append("-");
      }
      // binary?
      if (f.getBinaryValue() != null) {
        sb.append("B");
      } else {
        sb.append("-");
      }
      // numeric?
      if (f.getNumericValue() == null) {
        sb.append("----");
      } else {
        sb.append("#");
        // try faking it
        Number numeric = f.getNumericValue();
        if (numeric instanceof Integer) {
          sb.append("i32");
        } else if (numeric instanceof Long) {
          sb.append("i64");
        } else if (numeric instanceof Float) {
          sb.append("f32");
        } else if (numeric instanceof Double) {
          sb.append("f64");
        } else if (numeric instanceof Short) {
          sb.append("i16");
        } else if (numeric instanceof Byte) {
          sb.append("i08");
        } else if (numeric instanceof BigDecimal) {
          sb.append("b^d");
        } else if (numeric instanceof BigInteger) {
          sb.append("b^i");
        } else {
          sb.append("???");
        }
      }
      // has term vector?
      if (f.hasTermVectors()) {
        sb.append("V");
      } else {
        sb.append("-");
      }
      // doc values
      if (f.getDvType() == null || f.getDvType() == DocValuesType.NONE) {
        sb.append("-------");
      } else {
        sb.append("D");
        switch (f.getDvType()) {
          case NUMERIC:
            sb.append("number");
            break;
          case BINARY:
            sb.append("binary");
            break;
          case SORTED:
            sb.append("sorted");
            break;
          case SORTED_NUMERIC:
            sb.append("srtnum");
            break;
          case SORTED_SET:
            sb.append("srtset");
            break;
          default:
            sb.append("??????");
        }
      }
      // point values
      if (f.getPointDimensionCount() == 0) {
        sb.append("----");
      } else {
        sb.append("T");
        sb.append(f.getPointNumBytes());
        sb.append("/");
        sb.append(f.getPointDimensionCount());
      }
      return sb.toString();
    }

    @Override
    protected Column[] columnInfos() {
      return Column.values();
    }
  }

}

