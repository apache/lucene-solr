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
import javax.swing.BoxLayout;
import javax.swing.ButtonGroup;
import javax.swing.DefaultCellEditor;
import javax.swing.JCheckBox;
import javax.swing.JComboBox;
import javax.swing.JFormattedTextField;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JRadioButton;
import javax.swing.JScrollPane;
import javax.swing.JSeparator;
import javax.swing.JTable;
import javax.swing.JTextField;
import javax.swing.ListSelectionModel;
import java.awt.Color;
import java.awt.FlowLayout;
import java.awt.GridLayout;
import java.awt.event.ActionEvent;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;

import org.apache.lucene.document.DateTools;
import org.apache.lucene.luke.app.desktop.components.ComponentOperatorRegistry;
import org.apache.lucene.luke.app.desktop.components.TableColumnInfo;
import org.apache.lucene.luke.app.desktop.components.TableModelBase;
import org.apache.lucene.luke.app.desktop.util.MessageUtils;
import org.apache.lucene.luke.app.desktop.util.TableUtils;
import org.apache.lucene.luke.models.search.QueryParserConfig;

/** Provider of the QueryParser pane (tab) */
public final class QueryParserPaneProvider implements QueryParserTabOperator {

  private final JRadioButton standardRB = new JRadioButton();

  private final JRadioButton classicRB = new JRadioButton();

  private final JComboBox<String> dfCB = new JComboBox<>();

  private final JComboBox<String> defOpCombo = new JComboBox<>(new String[]{QueryParserConfig.Operator.OR.name(), QueryParserConfig.Operator.AND.name()});

  private final JCheckBox posIncCB = new JCheckBox();

  private final JCheckBox wildCardCB = new JCheckBox();

  private final JCheckBox splitWSCB = new JCheckBox();

  private final JCheckBox genPhraseQueryCB = new JCheckBox();

  private final JCheckBox genMultiTermSynonymsPhraseQueryCB = new JCheckBox();

  private final JFormattedTextField slopFTF = new JFormattedTextField();

  private final JFormattedTextField minSimFTF = new JFormattedTextField();

  private final JFormattedTextField prefLenFTF = new JFormattedTextField();

  private final JComboBox<String> dateResCB = new JComboBox<>();

  private final JTextField locationTF = new JTextField();

  private final JTextField timezoneTF = new JTextField();

  private final JTable pointRangeQueryTable = new JTable();

  private final ListenerFunctions listeners = new ListenerFunctions();

  private final QueryParserConfig config = new QueryParserConfig.Builder().build();

  public QueryParserPaneProvider() {
    ComponentOperatorRegistry.getInstance().register(QueryParserTabOperator.class, this);
  }

  public JScrollPane get() {
    JPanel panel = new JPanel();
    panel.setOpaque(false);
    panel.setLayout(new BoxLayout(panel, BoxLayout.PAGE_AXIS));
    panel.setBorder(BorderFactory.createEmptyBorder(3, 3, 3, 3));

    panel.add(initSelectParserPane());
    panel.add(new JSeparator(JSeparator.HORIZONTAL));
    panel.add(initParserSettingsPanel());
    panel.add(new JSeparator(JSeparator.HORIZONTAL));
    panel.add(initPhraseQuerySettingsPanel());
    panel.add(new JSeparator(JSeparator.HORIZONTAL));
    panel.add(initFuzzyQuerySettingsPanel());
    panel.add(new JSeparator(JSeparator.HORIZONTAL));
    panel.add(initDateRangeQuerySettingsPanel());
    panel.add(new JSeparator(JSeparator.HORIZONTAL));
    panel.add(initPointRangeQuerySettingsPanel());

    JScrollPane scrollPane = new JScrollPane(panel);
    scrollPane.setOpaque(false);
    scrollPane.getViewport().setOpaque(false);
    return scrollPane;
  }

  private JPanel initSelectParserPane() {
    JPanel panel = new JPanel(new FlowLayout(FlowLayout.LEADING));
    panel.setOpaque(false);

    standardRB.setText("StandardQueryParser");
    standardRB.setSelected(true);
    standardRB.addActionListener(listeners::selectStandardQParser);
    standardRB.setOpaque(false);

    classicRB.setText("Classic QueryParser");
    classicRB.addActionListener(listeners::selectClassicQparser);
    classicRB.setOpaque(false);

    ButtonGroup group = new ButtonGroup();
    group.add(standardRB);
    group.add(classicRB);

    panel.add(standardRB);
    panel.add(classicRB);

    return panel;
  }

  private JPanel initParserSettingsPanel() {
    JPanel panel = new JPanel(new GridLayout(3, 2));
    panel.setOpaque(false);

    JPanel defField = new JPanel(new FlowLayout(FlowLayout.LEADING));
    defField.setOpaque(false);
    JLabel dfLabel = new JLabel(MessageUtils.getLocalizedMessage("search_parser.label.df"));
    defField.add(dfLabel);
    defField.add(dfCB);
    panel.add(defField);

    JPanel defOp = new JPanel(new FlowLayout(FlowLayout.LEADING));
    defOp.setOpaque(false);
    JLabel defOpLabel = new JLabel(MessageUtils.getLocalizedMessage("search_parser.label.dop"));
    defOp.add(defOpLabel);
    defOpCombo.setSelectedItem(config.getDefaultOperator().name());
    defOp.add(defOpCombo);
    panel.add(defOp);

    posIncCB.setText(MessageUtils.getLocalizedMessage("search_parser.checkbox.pos_incr"));
    posIncCB.setSelected(config.isEnablePositionIncrements());
    posIncCB.setOpaque(false);
    panel.add(posIncCB);

    wildCardCB.setText(MessageUtils.getLocalizedMessage("search_parser.checkbox.lead_wildcard"));
    wildCardCB.setSelected(config.isAllowLeadingWildcard());
    wildCardCB.setOpaque(false);
    panel.add(wildCardCB);

    splitWSCB.setText(MessageUtils.getLocalizedMessage("search_parser.checkbox.split_ws"));
    splitWSCB.setEnabled(config.isSplitOnWhitespace());
    splitWSCB.addActionListener(listeners::toggleSplitOnWhiteSpace);
    splitWSCB.setOpaque(false);
    panel.add(splitWSCB);

    return panel;
  }

  private JPanel initPhraseQuerySettingsPanel() {
    JPanel panel = new JPanel();
    panel.setOpaque(false);
    panel.setLayout(new BoxLayout(panel, BoxLayout.PAGE_AXIS));

    JPanel header = new JPanel(new FlowLayout(FlowLayout.LEADING));
    header.setOpaque(false);
    header.add(new JLabel(MessageUtils.getLocalizedMessage("search_parser.label.phrase_query")));
    panel.add(header);

    JPanel genPQ = new JPanel(new FlowLayout(FlowLayout.LEADING));
    genPQ.setOpaque(false);
    genPQ.setBorder(BorderFactory.createEmptyBorder(0, 20, 0, 0));
    genPhraseQueryCB.setText(MessageUtils.getLocalizedMessage("search_parser.checkbox.gen_pq"));
    genPhraseQueryCB.setEnabled(config.isAutoGeneratePhraseQueries());
    genPhraseQueryCB.setOpaque(false);
    genPQ.add(genPhraseQueryCB);
    panel.add(genPQ);

    JPanel genMTPQ = new JPanel(new FlowLayout(FlowLayout.LEADING));
    genMTPQ.setOpaque(false);
    genMTPQ.setBorder(BorderFactory.createEmptyBorder(0, 20, 0, 0));
    genMultiTermSynonymsPhraseQueryCB.setText(MessageUtils.getLocalizedMessage("search_parser.checkbox.gen_mts"));
    genMultiTermSynonymsPhraseQueryCB.setEnabled(config.isAutoGenerateMultiTermSynonymsPhraseQuery());
    genMultiTermSynonymsPhraseQueryCB.setOpaque(false);
    genMTPQ.add(genMultiTermSynonymsPhraseQueryCB);
    panel.add(genMTPQ);

    JPanel slop = new JPanel(new FlowLayout(FlowLayout.LEADING));
    slop.setOpaque(false);
    slop.setBorder(BorderFactory.createEmptyBorder(0, 20, 0, 0));
    JLabel slopLabel = new JLabel(MessageUtils.getLocalizedMessage("search_parser.label.phrase_slop"));
    slop.add(slopLabel);
    slopFTF.setColumns(5);
    slopFTF.setValue(config.getPhraseSlop());
    slop.add(slopFTF);
    slop.add(new JLabel(MessageUtils.getLocalizedMessage("label.int_required")));
    panel.add(slop);

    return panel;
  }

  private JPanel initFuzzyQuerySettingsPanel() {
    JPanel panel = new JPanel();
    panel.setOpaque(false);
    panel.setLayout(new BoxLayout(panel, BoxLayout.PAGE_AXIS));

    JPanel header = new JPanel(new FlowLayout(FlowLayout.LEADING));
    header.setOpaque(false);
    header.add(new JLabel(MessageUtils.getLocalizedMessage("search_parser.label.fuzzy_query")));
    panel.add(header);

    JPanel minSim = new JPanel(new FlowLayout(FlowLayout.LEADING));
    minSim.setOpaque(false);
    minSim.setBorder(BorderFactory.createEmptyBorder(0, 20, 0, 0));
    JLabel minSimLabel = new JLabel(MessageUtils.getLocalizedMessage("search_parser.label.fuzzy_minsim"));
    minSim.add(minSimLabel);
    minSimFTF.setColumns(5);
    minSimFTF.setValue(config.getFuzzyMinSim());
    minSim.add(minSimFTF);
    minSim.add(new JLabel(MessageUtils.getLocalizedMessage("label.float_required")));
    panel.add(minSim);

    JPanel prefLen = new JPanel(new FlowLayout(FlowLayout.LEADING));
    prefLen.setOpaque(false);
    prefLen.setBorder(BorderFactory.createEmptyBorder(0, 20, 0, 0));
    JLabel prefLenLabel = new JLabel(MessageUtils.getLocalizedMessage("search_parser.label.fuzzy_preflen"));
    prefLen.add(prefLenLabel);
    prefLenFTF.setColumns(5);
    prefLenFTF.setValue(config.getFuzzyPrefixLength());
    prefLen.add(prefLenFTF);
    prefLen.add(new JLabel(MessageUtils.getLocalizedMessage("label.int_required")));
    panel.add(prefLen);

    return panel;
  }

  private JPanel initDateRangeQuerySettingsPanel() {
    JPanel panel = new JPanel();
    panel.setOpaque(false);
    panel.setLayout(new BoxLayout(panel, BoxLayout.PAGE_AXIS));

    JPanel header = new JPanel(new FlowLayout(FlowLayout.LEADING));
    header.setOpaque(false);
    header.add(new JLabel(MessageUtils.getLocalizedMessage("search_parser.label.daterange_query")));
    panel.add(header);

    JPanel resolution = new JPanel(new FlowLayout(FlowLayout.LEADING));
    resolution.setOpaque(false);
    resolution.setBorder(BorderFactory.createEmptyBorder(0, 20, 0, 0));
    JLabel resLabel = new JLabel(MessageUtils.getLocalizedMessage("search_parser.label.date_res"));
    resolution.add(resLabel);
    Arrays.stream(DateTools.Resolution.values()).map(DateTools.Resolution::name).forEach(dateResCB::addItem);
    dateResCB.setSelectedItem(config.getDateResolution().name());
    dateResCB.setOpaque(false);
    resolution.add(dateResCB);
    panel.add(resolution);

    JPanel locale = new JPanel(new FlowLayout(FlowLayout.LEADING));
    locale.setOpaque(false);
    locale.setBorder(BorderFactory.createEmptyBorder(0, 20, 0, 0));
    JLabel locLabel = new JLabel(MessageUtils.getLocalizedMessage("search_parser.label.locale"));
    locale.add(locLabel);
    locationTF.setColumns(10);
    locationTF.setText(config.getLocale().toLanguageTag());
    locale.add(locationTF);
    JLabel tzLabel = new JLabel(MessageUtils.getLocalizedMessage("search_parser.label.timezone"));
    locale.add(tzLabel);
    timezoneTF.setColumns(10);
    timezoneTF.setText(config.getTimeZone().getID());
    locale.add(timezoneTF);
    panel.add(locale);

    return panel;
  }

  private JPanel initPointRangeQuerySettingsPanel() {
    JPanel panel = new JPanel();
    panel.setOpaque(false);
    panel.setLayout(new BoxLayout(panel, BoxLayout.PAGE_AXIS));
    panel.setBorder(BorderFactory.createEmptyBorder(5, 5, 5, 5));

    JPanel header = new JPanel(new FlowLayout(FlowLayout.LEADING));
    header.setOpaque(false);
    header.add(new JLabel(MessageUtils.getLocalizedMessage("search_parser.label.pointrange_query")));
    panel.add(header);

    JPanel headerNote = new JPanel(new FlowLayout(FlowLayout.LEADING));
    headerNote.setOpaque(false);
    headerNote.add(new JLabel(MessageUtils.getLocalizedMessage("search_parser.label.pointrange_hint")));
    panel.add(headerNote);

    TableUtils.setupTable(pointRangeQueryTable, ListSelectionModel.SINGLE_SELECTION, new PointTypesTableModel(), null, PointTypesTableModel.Column.FIELD.getColumnWidth());
    pointRangeQueryTable.setShowGrid(true);
    JScrollPane scrollPane = new JScrollPane(pointRangeQueryTable);
    panel.add(scrollPane);

    return panel;
  }

  @Override
  public void setSearchableFields(Collection<String> searchableFields) {
    dfCB.removeAllItems();
    for (String field : searchableFields) {
      dfCB.addItem(field);
    }
  }

  @Override
  public void setRangeSearchableFields(Collection<String> rangeSearchableFields) {
    pointRangeQueryTable.setModel(new PointTypesTableModel(rangeSearchableFields));
    pointRangeQueryTable.setShowGrid(true);
    String[] numTypes = Arrays.stream(PointTypesTableModel.NumType.values())
        .map(PointTypesTableModel.NumType::name)
        .toArray(String[]::new);
    JComboBox<String> numTypesCombo = new JComboBox<>(numTypes);
    numTypesCombo.setRenderer((list, value, index, isSelected, cellHasFocus) -> new JLabel(value));
    pointRangeQueryTable.getColumnModel().getColumn(PointTypesTableModel.Column.TYPE.getIndex()).setCellEditor(new DefaultCellEditor(numTypesCombo));
    pointRangeQueryTable.getColumnModel().getColumn(PointTypesTableModel.Column.TYPE.getIndex()).setCellRenderer(
        (table, value, isSelected, hasFocus, row, column) -> new JLabel((String) value)
    );
    pointRangeQueryTable.getColumnModel().getColumn(PointTypesTableModel.Column.FIELD.getIndex()).setPreferredWidth(PointTypesTableModel.Column.FIELD.getColumnWidth());
    pointRangeQueryTable.setPreferredScrollableViewportSize(pointRangeQueryTable.getPreferredSize());

    // set default type to Integer
    for (int i = 0; i < rangeSearchableFields.size(); i++) {
      pointRangeQueryTable.setValueAt(PointTypesTableModel.NumType.INT.name(), i, PointTypesTableModel.Column.TYPE.getIndex());
    }

  }

  @Override
  public QueryParserConfig getConfig() {
    int phraseSlop = (int) slopFTF.getValue();
    float fuzzyMinSimFloat = (float) minSimFTF.getValue();
    int fuzzyPrefLenInt = (int) prefLenFTF.getValue();

    Map<String, Class<? extends Number>> typeMap = new HashMap<>();
    for (int row = 0; row < pointRangeQueryTable.getModel().getRowCount(); row++) {
      String field = (String) pointRangeQueryTable.getValueAt(row, PointTypesTableModel.Column.FIELD.getIndex());
      String type = (String) pointRangeQueryTable.getValueAt(row, PointTypesTableModel.Column.TYPE.getIndex());
      switch (PointTypesTableModel.NumType.valueOf(type)) {
        case INT:
          typeMap.put(field, Integer.class);
          break;
        case LONG:
          typeMap.put(field, Long.class);
          break;
        case FLOAT:
          typeMap.put(field, Float.class);
          break;
        case DOUBLE:
          typeMap.put(field, Double.class);
          break;
        default:
          break;
      }
    }

    return new QueryParserConfig.Builder()
        .useClassicParser(classicRB.isSelected())
        .defaultOperator(QueryParserConfig.Operator.valueOf((String) defOpCombo.getSelectedItem()))
        .enablePositionIncrements(posIncCB.isSelected())
        .allowLeadingWildcard(wildCardCB.isSelected())
        .splitOnWhitespace(splitWSCB.isSelected())
        .autoGeneratePhraseQueries(genPhraseQueryCB.isSelected())
        .autoGenerateMultiTermSynonymsPhraseQuery(genMultiTermSynonymsPhraseQueryCB.isSelected())
        .phraseSlop(phraseSlop)
        .fuzzyMinSim(fuzzyMinSimFloat)
        .fuzzyPrefixLength(fuzzyPrefLenInt)
        .dateResolution(DateTools.Resolution.valueOf((String) dateResCB.getSelectedItem()))
        .locale(new Locale(locationTF.getText()))
        .timeZone(TimeZone.getTimeZone(timezoneTF.getText()))
        .typeMap(typeMap)
        .build();
  }

  @Override
  public String getDefaultField() {
    return (String) dfCB.getSelectedItem();
  }

  private class ListenerFunctions {

    void selectStandardQParser(ActionEvent e) {
      splitWSCB.setEnabled(false);
      genPhraseQueryCB.setEnabled(false);
      genMultiTermSynonymsPhraseQueryCB.setEnabled(false);
      TableUtils.setEnabled(pointRangeQueryTable, true);
    }

    void selectClassicQparser(ActionEvent e) {
      splitWSCB.setEnabled(true);
      if (splitWSCB.isSelected()) {
        genPhraseQueryCB.setEnabled(true);
      } else {
        genPhraseQueryCB.setEnabled(false);
        genPhraseQueryCB.setSelected(false);
      }
      genMultiTermSynonymsPhraseQueryCB.setEnabled(true);
      pointRangeQueryTable.setEnabled(false);
      pointRangeQueryTable.setForeground(Color.gray);
      TableUtils.setEnabled(pointRangeQueryTable, false);
    }

    void toggleSplitOnWhiteSpace(ActionEvent e) {
      if (splitWSCB.isSelected()) {
        genPhraseQueryCB.setEnabled(true);
      } else {
        genPhraseQueryCB.setEnabled(false);
        genPhraseQueryCB.setSelected(false);
      }
    }

  }

  static final class PointTypesTableModel extends TableModelBase<PointTypesTableModel.Column> {

    enum Column implements TableColumnInfo {

      FIELD("Field", 0, String.class, 300),
      TYPE("Numeric Type", 1, NumType.class, 150);

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

    enum NumType {

      INT, LONG, FLOAT, DOUBLE

    }

    PointTypesTableModel() {
      super();
    }

    PointTypesTableModel(Collection<String> rangeSearchableFields) {
      super(rangeSearchableFields.size());
      int i = 0;
      for (String field : rangeSearchableFields) {
        data[i++][Column.FIELD.getIndex()] = field;
      }
    }

    @Override
    public boolean isCellEditable(int rowIndex, int columnIndex) {
      return columnIndex == Column.TYPE.getIndex();
    }

    @Override
    public void setValueAt(Object value, int rowIndex, int columnIndex) {
      data[rowIndex][columnIndex] = value;
      fireTableCellUpdated(rowIndex, columnIndex);
    }

    @Override
    protected Column[] columnInfos() {
      return Column.values();
    }
  }

}
