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
import javax.swing.DefaultComboBoxModel;
import javax.swing.DefaultListModel;
import javax.swing.JButton;
import javax.swing.JComboBox;
import javax.swing.JDialog;
import javax.swing.JLabel;
import javax.swing.JList;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.ListSelectionModel;
import java.awt.BorderLayout;
import java.awt.Dialog;
import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.Insets;
import java.awt.Toolkit;
import java.awt.Window;
import java.awt.datatransfer.Clipboard;
import java.awt.datatransfer.StringSelection;
import java.awt.event.ActionEvent;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import org.apache.lucene.luke.app.desktop.Preferences;
import org.apache.lucene.luke.app.desktop.PreferencesFactory;
import org.apache.lucene.luke.app.desktop.util.DialogOpener;
import org.apache.lucene.luke.app.desktop.util.FontUtils;
import org.apache.lucene.luke.app.desktop.util.MessageUtils;
import org.apache.lucene.luke.models.documents.DocValues;
import org.apache.lucene.luke.util.BytesRefUtils;
import org.apache.lucene.util.NumericUtils;

/** Factory of doc values dialog */
public final class DocValuesDialogFactory implements DialogOpener.DialogFactory {

  private static DocValuesDialogFactory instance;

  private final Preferences prefs;

  private final JComboBox<String> decodersCombo = new JComboBox<>();

  private final JList<String> valueList = new JList<>();

  private final ListenerFunctions listeners = new ListenerFunctions();

  private JDialog dialog;

  private String field;

  private DocValues docValues;

  public synchronized static DocValuesDialogFactory getInstance() throws IOException {
    if (instance == null) {
      instance = new DocValuesDialogFactory();
    }
    return instance;
  }

  private DocValuesDialogFactory() throws IOException {
    this.prefs = PreferencesFactory.getInstance();
  }

  public void setValue(String field, DocValues docValues) {
    this.field = field;
    this.docValues = docValues;

    DefaultListModel<String> values = new DefaultListModel<>();
    if (docValues.getValues().size() > 0) {
      decodersCombo.setEnabled(false);
      docValues.getValues().stream()
          .map(BytesRefUtils::decode)
          .forEach(values::addElement);
    } else if (docValues.getNumericValues().size() > 0) {
      decodersCombo.setEnabled(true);
      docValues.getNumericValues().stream()
          .map(String::valueOf)
          .forEach(values::addElement);
    }

    valueList.setModel(values);
  }

  @Override
  public JDialog create(Window owner, String title, int width, int height) {
    if (Objects.isNull(field) || Objects.isNull(docValues)) {
      throw new IllegalStateException("field name and/or doc values is not set.");
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
    panel.add(headerPanel(), BorderLayout.PAGE_START);
    JScrollPane scrollPane = new JScrollPane(valueList());
    scrollPane.setOpaque(false);
    scrollPane.getViewport().setOpaque(false);
    panel.add(scrollPane, BorderLayout.CENTER);
    panel.add(footerPanel(), BorderLayout.PAGE_END);
    return panel;
  }

  private JPanel headerPanel() {
    JPanel header = new JPanel();
    header.setOpaque(false);
    header.setLayout(new BoxLayout(header, BoxLayout.PAGE_AXIS));

    JPanel fieldHeader = new JPanel(new FlowLayout(FlowLayout.LEADING, 3, 3));
    fieldHeader.setOpaque(false);
    fieldHeader.add(new JLabel(MessageUtils.getLocalizedMessage("documents.docvalues.label.doc_values")));
    fieldHeader.add(new JLabel(field));
    header.add(fieldHeader);

    JPanel typeHeader = new JPanel(new FlowLayout(FlowLayout.LEADING, 3, 3));
    typeHeader.setOpaque(false);
    typeHeader.add(new JLabel(MessageUtils.getLocalizedMessage("documents.docvalues.label.type")));
    typeHeader.add(new JLabel(docValues.getDvType().toString()));
    header.add(typeHeader);

    JPanel decodeHeader = new JPanel(new FlowLayout(FlowLayout.TRAILING, 3, 3));
    decodeHeader.setOpaque(false);
    decodeHeader.add(new JLabel("decoded as"));
    String[] decoders = Arrays.stream(Decoder.values()).map(Decoder::toString).toArray(String[]::new);
    decodersCombo.setModel(new DefaultComboBoxModel<>(decoders));
    decodersCombo.setSelectedItem(Decoder.LONG.toString());
    decodersCombo.addActionListener(listeners::selectDecoder);
    decodeHeader.add(decodersCombo);
    if (docValues.getValues().size() > 0) {
      decodeHeader.setEnabled(false);
    }
    header.add(decodeHeader);

    return header;
  }

  private JList<String> valueList() {
    valueList.setVisibleRowCount(5);
    valueList.setSelectionMode(ListSelectionModel.MULTIPLE_INTERVAL_SELECTION);
    valueList.setLayoutOrientation(JList.VERTICAL);

    DefaultListModel<String> values = new DefaultListModel<>();
    if (docValues.getValues().size() > 0) {
      docValues.getValues().stream()
          .map(BytesRefUtils::decode)
          .forEach(values::addElement);
    } else {
      docValues.getNumericValues().stream()
          .map(String::valueOf)
          .forEach(values::addElement);
    }
    valueList.setModel(values);

    return valueList;
  }

  private JPanel footerPanel() {
    JPanel footer = new JPanel(new FlowLayout(FlowLayout.TRAILING, 5, 5));
    footer.setOpaque(false);

    JButton copyBtn = new JButton(FontUtils.elegantIconHtml("&#xe0e6;", MessageUtils.getLocalizedMessage("button.copy")));
    copyBtn.setMargin(new Insets(3, 0, 3, 0));
    copyBtn.addActionListener(listeners::copyValues);
    footer.add(copyBtn);

    JButton closeBtn = new JButton(MessageUtils.getLocalizedMessage("button.close"));
    closeBtn.setMargin(new Insets(3, 0, 3, 0));
    closeBtn.addActionListener(e -> dialog.dispose());
    footer.add(closeBtn);

    return footer;
  }

  // control methods

  private void selectDecoder() {
    String decoderLabel = (String) decodersCombo.getSelectedItem();
    Decoder decoder = Decoder.fromLabel(decoderLabel);

    if (docValues.getNumericValues().isEmpty()) {
      return;
    }

    DefaultListModel<String> values = new DefaultListModel<>();
    switch (decoder) {
      case LONG:
        docValues.getNumericValues().stream()
            .map(String::valueOf)
            .forEach(values::addElement);
        break;
      case FLOAT:
        docValues.getNumericValues().stream()
            .mapToInt(Long::intValue)
            .mapToObj(NumericUtils::sortableIntToFloat)
            .map(String::valueOf)
            .forEach(values::addElement);
        break;
      case DOUBLE:
        docValues.getNumericValues().stream()
            .map(NumericUtils::sortableLongToDouble)
            .map(String::valueOf)
            .forEach(values::addElement);
        break;
    }

    valueList.setModel(values);
  }

  private void copyValues() {
    List<String> values = valueList.getSelectedValuesList();
    if (values.isEmpty()) {
      values = getAllVlues();
    }

    Clipboard clipboard = Toolkit.getDefaultToolkit().getSystemClipboard();
    StringSelection selection = new StringSelection(String.join("\n", values));
    clipboard.setContents(selection, null);
  }

  private List<String> getAllVlues() {
    List<String> values = new ArrayList<>();
    for (int i = 0; i < valueList.getModel().getSize(); i++) {
      values.add(valueList.getModel().getElementAt(i));
    }
    return values;
  }

  private class ListenerFunctions {

    void selectDecoder(ActionEvent e) {
      DocValuesDialogFactory.this.selectDecoder();
    }

    void copyValues(ActionEvent e) {
      DocValuesDialogFactory.this.copyValues();
    }
  }


  /** doc value decoders */
  public enum Decoder {

    LONG("long"), FLOAT("float"), DOUBLE("double");

    private final String label;

    Decoder(String label) {
      this.label = label;
    }

    @Override
    public String toString() {
      return label;
    }

    public static Decoder fromLabel(String label) {
      for (Decoder d : values()) {
        if (d.label.equalsIgnoreCase(label)) {
          return d;
        }
      }
      throw new IllegalArgumentException("No such decoder: " + label);
    }
  }

}
