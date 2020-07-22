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
import javax.swing.BoxLayout;
import javax.swing.DefaultCellEditor;
import javax.swing.JButton;
import javax.swing.JComboBox;
import javax.swing.JComponent;
import javax.swing.JDialog;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTable;
import javax.swing.JTextArea;
import javax.swing.ListSelectionModel;
import javax.swing.UIManager;
import javax.swing.table.JTableHeader;
import javax.swing.table.TableCellRenderer;
import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Component;
import java.awt.Dialog;
import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.GridLayout;
import java.awt.Insets;
import java.awt.Window;
import java.awt.event.ActionEvent;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Constructor;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FloatPoint;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.IndexableFieldType;
import org.apache.lucene.luke.app.IndexHandler;
import org.apache.lucene.luke.app.IndexObserver;
import org.apache.lucene.luke.app.LukeState;
import org.apache.lucene.luke.app.desktop.Preferences;
import org.apache.lucene.luke.app.desktop.PreferencesFactory;
import org.apache.lucene.luke.app.desktop.components.AnalysisTabOperator;
import org.apache.lucene.luke.app.desktop.components.ComponentOperatorRegistry;
import org.apache.lucene.luke.app.desktop.components.DocumentsTabOperator;
import org.apache.lucene.luke.app.desktop.components.TabSwitcherProxy;
import org.apache.lucene.luke.app.desktop.components.TabbedPaneProvider;
import org.apache.lucene.luke.app.desktop.components.TableColumnInfo;
import org.apache.lucene.luke.app.desktop.components.TableModelBase;
import org.apache.lucene.luke.app.desktop.components.dialog.HelpDialogFactory;
import org.apache.lucene.luke.app.desktop.dto.documents.NewField;
import org.apache.lucene.luke.app.desktop.util.DialogOpener;
import org.apache.lucene.luke.app.desktop.util.FontUtils;
import org.apache.lucene.luke.app.desktop.util.HelpHeaderRenderer;
import org.apache.lucene.luke.app.desktop.util.MessageUtils;
import org.apache.lucene.luke.app.desktop.util.NumericUtils;
import org.apache.lucene.luke.app.desktop.util.StringUtils;
import org.apache.lucene.luke.app.desktop.util.TableUtils;
import org.apache.lucene.luke.models.LukeException;
import org.apache.lucene.luke.models.tools.IndexTools;
import org.apache.lucene.luke.models.tools.IndexToolsFactory;
import org.apache.lucene.luke.util.LoggerFactory;
import org.apache.lucene.util.BytesRef;

/** Factory of add document dialog */
public final class AddDocumentDialogFactory implements DialogOpener.DialogFactory, AddDocumentDialogOperator {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static AddDocumentDialogFactory instance;

  private final static int ROW_COUNT = 50;

  private final Preferences prefs;

  private final IndexHandler indexHandler;

  private final IndexToolsFactory toolsFactory = new IndexToolsFactory();

  private final TabSwitcherProxy tabSwitcher;

  private final ComponentOperatorRegistry operatorRegistry;

  private final IndexOptionsDialogFactory indexOptionsDialogFactory;

  private final HelpDialogFactory helpDialogFactory;

  private final ListenerFunctions listeners = new ListenerFunctions();

  private final JLabel analyzerNameLbl = new JLabel(StandardAnalyzer.class.getName());

  private final List<NewField> newFieldList;

  private final JButton addBtn = new JButton();

  private final JButton closeBtn = new JButton();

  private final JTextArea infoTA = new JTextArea();

  private IndexTools toolsModel;

  private JDialog dialog;

  public synchronized static AddDocumentDialogFactory getInstance() throws IOException {
    if (instance == null) {
      instance = new AddDocumentDialogFactory();
    }
    return  instance;
  }

  private AddDocumentDialogFactory() throws IOException {
    this.prefs = PreferencesFactory.getInstance();
    this.indexHandler = IndexHandler.getInstance();
    this.tabSwitcher = TabSwitcherProxy.getInstance();
    this.operatorRegistry = ComponentOperatorRegistry.getInstance();
    this.indexOptionsDialogFactory = IndexOptionsDialogFactory.getInstance();
    this.helpDialogFactory = HelpDialogFactory.getInstance();
    this.newFieldList = IntStream.range(0, ROW_COUNT).mapToObj(i -> NewField.newInstance()).collect(Collectors.toList());

    operatorRegistry.register(AddDocumentDialogOperator.class, this);
    indexHandler.addObserver(new Observer());

    initialize();
  }

  private void initialize() {
    addBtn.setText(MessageUtils.getLocalizedMessage("add_document.button.add"));
    addBtn.setMargin(new Insets(3, 3, 3, 3));
    addBtn.setEnabled(true);
    addBtn.addActionListener(listeners::addDocument);

    closeBtn.setText(MessageUtils.getLocalizedMessage("button.cancel"));
    closeBtn.setMargin(new Insets(3, 3, 3, 3));
    closeBtn.addActionListener(e -> dialog.dispose());

    infoTA.setRows(3);
    infoTA.setLineWrap(true);
    infoTA.setEditable(false);
    infoTA.setText(MessageUtils.getLocalizedMessage("add_document.info"));
    infoTA.setForeground(Color.gray);
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
    panel.add(header(), BorderLayout.PAGE_START);
    panel.add(center(), BorderLayout.CENTER);
    panel.add(footer(), BorderLayout.PAGE_END);
    return panel;
  }

  private JPanel header() {
    JPanel panel = new JPanel();
    panel.setOpaque(false);
    panel.setLayout(new BoxLayout(panel, BoxLayout.PAGE_AXIS));

    JPanel analyzerHeader = new JPanel(new FlowLayout(FlowLayout.LEADING, 10, 10));
    analyzerHeader.setOpaque(false);
    analyzerHeader.add(new JLabel(MessageUtils.getLocalizedMessage("add_document.label.analyzer")));
    analyzerHeader.add(analyzerNameLbl);
    JLabel changeLbl = new JLabel(MessageUtils.getLocalizedMessage("add_document.hyperlink.change"));
    changeLbl.addMouseListener(new MouseAdapter() {
      @Override
      public void mouseClicked(MouseEvent e) {
        dialog.dispose();
        tabSwitcher.switchTab(TabbedPaneProvider.Tab.ANALYZER);
      }
    });
    analyzerHeader.add(FontUtils.toLinkText(changeLbl));
    panel.add(analyzerHeader);

    return panel;
  }

  private JPanel center() {
    JPanel panel = new JPanel(new BorderLayout());
    panel.setOpaque(false);
    panel.setBorder(BorderFactory.createEmptyBorder(5, 5, 5, 5));

    JPanel tableHeader = new JPanel(new FlowLayout(FlowLayout.LEADING, 10, 5));
    tableHeader.setOpaque(false);
    tableHeader.add(new JLabel(MessageUtils.getLocalizedMessage("add_document.label.fields")));
    panel.add(tableHeader, BorderLayout.PAGE_START);

    JScrollPane scrollPane = new JScrollPane(fieldsTable());
    scrollPane.setOpaque(false);
    scrollPane.getViewport().setOpaque(false);
    panel.add(scrollPane, BorderLayout.CENTER);

    JPanel tableFooter = new JPanel(new FlowLayout(FlowLayout.TRAILING, 10, 5));
    tableFooter.setOpaque(false);
    addBtn.setEnabled(true);
    tableFooter.add(addBtn);
    tableFooter.add(closeBtn);
    panel.add(tableFooter, BorderLayout.PAGE_END);

    return panel;
  }

  private JTable fieldsTable() {
    JTable fieldsTable = new JTable();
    TableUtils.setupTable(fieldsTable, ListSelectionModel.SINGLE_SELECTION, new FieldsTableModel(newFieldList), null, 30, 150, 120, 80);
    fieldsTable.setShowGrid(true);
    JComboBox<Class<? extends IndexableField>> typesCombo = new JComboBox<>(presetFieldClasses);
    typesCombo.setRenderer((list, value, index, isSelected, cellHasFocus) -> new JLabel(value.getSimpleName()));
    fieldsTable.getColumnModel().getColumn(FieldsTableModel.Column.TYPE.getIndex()).setCellEditor(new DefaultCellEditor(typesCombo));
    for (int i = 0; i < fieldsTable.getModel().getRowCount(); i++) {
      fieldsTable.getModel().setValueAt(TextField.class, i, FieldsTableModel.Column.TYPE.getIndex());
    }
    fieldsTable.getColumnModel().getColumn(FieldsTableModel.Column.TYPE.getIndex()).setHeaderRenderer(
        new HelpHeaderRenderer(
            "About Type", "Select Field Class:",
            createTypeHelpDialog(), helpDialogFactory, dialog));
    fieldsTable.getColumnModel().getColumn(FieldsTableModel.Column.TYPE.getIndex()).setCellRenderer(new TypeCellRenderer());
    fieldsTable.getColumnModel().getColumn(FieldsTableModel.Column.OPTIONS.getIndex()).setCellRenderer(new OptionsCellRenderer(dialog, indexOptionsDialogFactory, newFieldList));
    return fieldsTable;
  }

  private JComponent createTypeHelpDialog() {
    JPanel panel = new JPanel(new BorderLayout());
    panel.setOpaque(false);

    JTextArea descTA = new JTextArea();

    JPanel header = new JPanel();
    header.setOpaque(false);
    header.setLayout(new BoxLayout(header, BoxLayout.PAGE_AXIS));
    String[] typeList = new String[]{
        "TextField",
        "StringField",
        "IntPoint",
        "LongPoint",
        "FloatPoint",
        "DoublePoint",
        "SortedDocValuesField",
        "SortedSetDocValuesField",
        "NumericDocValuesField",
        "SortedNumericDocValuesField",
        "StoredField",
        "Field"
    };
    JPanel wrapper1 = new JPanel(new FlowLayout(FlowLayout.LEADING));
    wrapper1.setOpaque(false);
    JComboBox<String> typeCombo = new JComboBox<>(typeList);
    typeCombo.setSelectedItem(typeList[0]);
    typeCombo.addActionListener(e -> {
      String selected = (String) typeCombo.getSelectedItem();
      descTA.setText(MessageUtils.getLocalizedMessage("help.fieldtype." + selected));
    });
    wrapper1.add(typeCombo);
    header.add(wrapper1);
    JPanel wrapper2 = new JPanel(new FlowLayout(FlowLayout.LEADING));
    wrapper2.setOpaque(false);
    wrapper2.add(new JLabel("Brief description and Examples"));
    header.add(wrapper2);
    panel.add(header, BorderLayout.PAGE_START);

    descTA.setBorder(BorderFactory.createEmptyBorder(5, 5, 5, 5));
    descTA.setEditable(false);
    descTA.setLineWrap(true);
    descTA.setRows(10);
    descTA.setText(MessageUtils.getLocalizedMessage("help.fieldtype." + typeList[0]));
    JScrollPane scrollPane = new JScrollPane(descTA);
    panel.add(scrollPane, BorderLayout.CENTER);

    return panel;
  }

  private JPanel footer() {
    JPanel panel = new JPanel(new GridLayout(1, 1));
    panel.setOpaque(false);
    panel.setBorder(BorderFactory.createEmptyBorder(5, 5, 5, 5));

    JScrollPane scrollPane = new JScrollPane(infoTA);
    scrollPane.setOpaque(false);
    scrollPane.getViewport().setOpaque(false);
    panel.add(scrollPane);
    return panel;
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private final Class<? extends IndexableField>[] presetFieldClasses = new Class[]{
      TextField.class, StringField.class,
      IntPoint.class, LongPoint.class, FloatPoint.class, DoublePoint.class,
      SortedDocValuesField.class, SortedSetDocValuesField.class,
      NumericDocValuesField.class, SortedNumericDocValuesField.class,
      StoredField.class, Field.class
  };

  @Override
  public void setAnalyzer(Analyzer analyzer) {
    analyzerNameLbl.setText(analyzer.getClass().getName());
  }

  private class ListenerFunctions {

    void addDocument(ActionEvent e) {
      List<NewField> validFields = newFieldList.stream()
          .filter(nf -> !nf.isDeleted())
          .filter(nf -> !StringUtils.isNullOrEmpty(nf.getName()))
          .filter(nf -> !StringUtils.isNullOrEmpty(nf.getValue()))
          .collect(Collectors.toList());
      if (validFields.isEmpty()) {
        infoTA.setText("Please add one or more fields. Name and Value are both required.");
        return;
      }

      Document doc = new Document();
      try {
        for (NewField nf : validFields) {
          doc.add(toIndexableField(nf));
        }
      } catch (NumberFormatException ex) {
        log.error("Error converting field value", e);
        throw new LukeException("Invalid value: " + ex.getMessage(), ex);
      } catch (Exception ex) {
        log.error("Error converting field value", e);
        throw new LukeException(ex.getMessage(), ex);
      }

      addDocument(doc);
      log.info("Added document: {}", doc);
    }

    @SuppressWarnings("unchecked")
    private IndexableField toIndexableField(NewField nf) throws Exception {
      final Constructor<? extends IndexableField> constr;
      if (nf.getType().equals(TextField.class) || nf.getType().equals(StringField.class)) {
        Field.Store store = nf.isStored() ? Field.Store.YES : Field.Store.NO;
        constr = nf.getType().getConstructor(String.class, String.class, Field.Store.class);
        return constr.newInstance(nf.getName(), nf.getValue(), store);
      } else if (nf.getType().equals(IntPoint.class)) {
        constr = nf.getType().getConstructor(String.class, int[].class);
        int[] values = NumericUtils.convertToIntArray(nf.getValue(), false);
        return constr.newInstance(nf.getName(), values);
      } else if (nf.getType().equals(LongPoint.class)) {
        constr = nf.getType().getConstructor(String.class, long[].class);
        long[] values = NumericUtils.convertToLongArray(nf.getValue(), false);
        return constr.newInstance(nf.getName(), values);
      } else if (nf.getType().equals(FloatPoint.class)) {
        constr = nf.getType().getConstructor(String.class, float[].class);
        float[] values = NumericUtils.convertToFloatArray(nf.getValue(), false);
        return constr.newInstance(nf.getName(), values);
      } else if (nf.getType().equals(DoublePoint.class)) {
        constr = nf.getType().getConstructor(String.class, double[].class);
        double[] values = NumericUtils.convertToDoubleArray(nf.getValue(), false);
        return constr.newInstance(nf.getName(), values);
      } else if (nf.getType().equals(SortedDocValuesField.class) ||
          nf.getType().equals(SortedSetDocValuesField.class)) {
        constr = nf.getType().getConstructor(String.class, BytesRef.class);
        return constr.newInstance(nf.getName(), new BytesRef(nf.getValue()));
      } else if (nf.getType().equals(NumericDocValuesField.class) ||
          nf.getType().equals(SortedNumericDocValuesField.class)) {
        constr = nf.getType().getConstructor(String.class, long.class);
        long value = NumericUtils.tryConvertToLongValue(nf.getValue());
        return constr.newInstance(nf.getName(), value);
      } else if (nf.getType().equals(StoredField.class)) {
        constr = nf.getType().getConstructor(String.class, String.class);
        return constr.newInstance(nf.getName(), nf.getValue());
      } else if (nf.getType().equals(Field.class)) {
        constr = nf.getType().getConstructor(String.class, String.class, IndexableFieldType.class);
        return constr.newInstance(nf.getName(), nf.getValue(), nf.getFieldType());
      } else {
        // TODO: unknown field
        return new StringField(nf.getName(), nf.getValue(), Field.Store.YES);
      }
    }

    private void addDocument(Document doc) {
      try {
        Analyzer analyzer = operatorRegistry.get(AnalysisTabOperator.class)
            .map(AnalysisTabOperator::getCurrentAnalyzer)
            .orElse(new StandardAnalyzer());
        toolsModel.addDocument(doc, analyzer);
        indexHandler.reOpen();
        operatorRegistry.get(DocumentsTabOperator.class).ifPresent(DocumentsTabOperator::displayLatestDoc);
        tabSwitcher.switchTab(TabbedPaneProvider.Tab.DOCUMENTS);
        infoTA.setText(MessageUtils.getLocalizedMessage("add_document.message.success"));
        addBtn.setEnabled(false);
        closeBtn.setText(MessageUtils.getLocalizedMessage("button.close"));
      } catch (LukeException e) {
        infoTA.setText(MessageUtils.getLocalizedMessage("add_document.message.fail"));
        throw e;
      } catch (Exception e) {
        infoTA.setText(MessageUtils.getLocalizedMessage("add_document.message.fail"));
        throw new LukeException(e.getMessage(), e);
      }
    }

  }

  private class Observer implements IndexObserver {

    @Override
    public void openIndex(LukeState state) {
      toolsModel = toolsFactory.newInstance(state.getIndexReader(), state.useCompound(), state.keepAllCommits());
    }

    @Override
    public void closeIndex() {
      toolsModel = null;
    }
  }

  static final class FieldsTableModel extends TableModelBase<FieldsTableModel.Column> {

    enum Column implements TableColumnInfo {
      DEL("Del", 0, Boolean.class),
      NAME("Name", 1, String.class),
      TYPE("Type", 2, Class.class),
      OPTIONS("Options", 3, String.class),
      VALUE("Value", 4, String.class);

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

    private final List<NewField> newFieldList;

    FieldsTableModel(List<NewField> newFieldList) {
      super(newFieldList.size());
      this.newFieldList = newFieldList;
    }

    @Override
    public Object getValueAt(int rowIndex, int columnIndex) {
      if (columnIndex == Column.OPTIONS.getIndex()) {
        return "";
      }
      return data[rowIndex][columnIndex];
    }

    @Override
    public boolean isCellEditable(int rowIndex, int columnIndex) {
      return columnIndex != Column.OPTIONS.getIndex();
    }

    @SuppressWarnings("unchecked")
    @Override
    public void setValueAt(Object value, int rowIndex, int columnIndex) {
      data[rowIndex][columnIndex] = value;
      fireTableCellUpdated(rowIndex, columnIndex);
      NewField selectedField = newFieldList.get(rowIndex);
      if (columnIndex == Column.DEL.getIndex()) {
        selectedField.setDeleted((Boolean) value);
      } else if (columnIndex == Column.NAME.getIndex()) {
        selectedField.setName((String) value);
      } else if (columnIndex == Column.TYPE.getIndex()) {
        selectedField.setType((Class<? extends IndexableField>) value);
        selectedField.resetFieldType((Class<? extends IndexableField>) value);
        selectedField.setStored(selectedField.getFieldType().stored());
      } else if (columnIndex == Column.VALUE.getIndex()) {
        selectedField.setValue((String) value);
      }
    }

    @Override
    protected Column[] columnInfos() {
      return Column.values();
    }
  }

  static final class TypeCellRenderer implements TableCellRenderer {

    @SuppressWarnings("unchecked")
    @Override
    public Component getTableCellRendererComponent(JTable table, Object value, boolean isSelected, boolean hasFocus, int row, int column) {
      String simpleName = ((Class<? extends IndexableField>) value).getSimpleName();
      return new JLabel(simpleName);
    }
  }

  static final class OptionsCellRenderer implements TableCellRenderer {

    private JDialog dialog;

    private final IndexOptionsDialogFactory indexOptionsDialogFactory;

    private final List<NewField> newFieldList;

    private final JPanel panel = new JPanel();

    private JTable table;

    public OptionsCellRenderer(JDialog dialog, IndexOptionsDialogFactory indexOptionsDialogFactory, List<NewField> newFieldList) {
      this.dialog = dialog;
      this.indexOptionsDialogFactory = indexOptionsDialogFactory;
      this.newFieldList = newFieldList;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Component getTableCellRendererComponent(JTable table, Object value, boolean isSelected, boolean hasFocus, int row, int column) {
      if (table != null && this.table != table) {
        this.table = table;
        final JTableHeader header = table.getTableHeader();
        if (header != null) {
          panel.setLayout(new FlowLayout(FlowLayout.CENTER, 0, 0));
          panel.setBorder(UIManager.getBorder("TableHeader.cellBorder"));
          panel.add(new JLabel(value.toString()));

          JLabel optionsLbl = new JLabel("options");
          table.addMouseListener(new MouseAdapter() {
            @Override
            public void mouseClicked(MouseEvent e) {
              int row = table.rowAtPoint(e.getPoint());
              int col = table.columnAtPoint(e.getPoint());
              if (row >= 0 && col == FieldsTableModel.Column.OPTIONS.getIndex()) {
                String title = "Index options for:";
                new DialogOpener<>(indexOptionsDialogFactory).open(dialog, title, 500, 500,
                    (factory) -> {
                      factory.setNewField(newFieldList.get(row));
                    });
              }
            }
          });
          panel.add(FontUtils.toLinkText(optionsLbl));
        }
      }
      return panel;
    }

  }
}
