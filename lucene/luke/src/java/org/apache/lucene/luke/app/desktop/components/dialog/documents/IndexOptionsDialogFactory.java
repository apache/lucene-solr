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

import java.awt.Dialog;
import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.Insets;
import java.awt.Window;
import java.io.IOException;
import java.util.Arrays;
import javax.swing.BorderFactory;
import javax.swing.BoxLayout;
import javax.swing.JButton;
import javax.swing.JCheckBox;
import javax.swing.JComboBox;
import javax.swing.JDialog;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JSeparator;
import javax.swing.JTextField;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableFieldType;
import org.apache.lucene.luke.app.desktop.Preferences;
import org.apache.lucene.luke.app.desktop.PreferencesFactory;
import org.apache.lucene.luke.app.desktop.dto.documents.NewField;
import org.apache.lucene.luke.app.desktop.util.DialogOpener;
import org.apache.lucene.luke.app.desktop.util.MessageUtils;

/** Factory of index options dialog */
public final class IndexOptionsDialogFactory implements DialogOpener.DialogFactory {

  private static IndexOptionsDialogFactory instance;

  private final Preferences prefs;

  private final JCheckBox storedCB = new JCheckBox();

  private final JCheckBox tokenizedCB = new JCheckBox();

  private final JCheckBox omitNormsCB = new JCheckBox();

  private final JComboBox<String> idxOptCombo = new JComboBox<>(availableIndexOptions());

  private final JCheckBox storeTVCB = new JCheckBox();

  private final JCheckBox storeTVPosCB = new JCheckBox();

  private final JCheckBox storeTVOffCB = new JCheckBox();

  private final JCheckBox storeTVPayCB = new JCheckBox();

  private final JComboBox<String> dvTypeCombo = new JComboBox<>(availableDocValuesType());

  private final JTextField dimCountTF = new JTextField();

  private final JTextField dimNumBytesTF = new JTextField();

  private JDialog dialog;

  private NewField nf;

  public static synchronized IndexOptionsDialogFactory getInstance() throws IOException {
    if (instance == null) {
      instance = new IndexOptionsDialogFactory();
    }
    return instance;
  }

  private IndexOptionsDialogFactory() throws IOException {
    this.prefs = PreferencesFactory.getInstance();
    initialize();
  }

  private void initialize() {
    storedCB.setText(MessageUtils.getLocalizedMessage("idx_options.checkbox.stored"));
    storedCB.setOpaque(false);
    tokenizedCB.setText(MessageUtils.getLocalizedMessage("idx_options.checkbox.tokenized"));
    tokenizedCB.setOpaque(false);
    omitNormsCB.setText(MessageUtils.getLocalizedMessage("idx_options.checkbox.omit_norm"));
    omitNormsCB.setOpaque(false);
    idxOptCombo.setPreferredSize(new Dimension(300, idxOptCombo.getPreferredSize().height));
    storeTVCB.setText(MessageUtils.getLocalizedMessage("idx_options.checkbox.store_tv"));
    storeTVCB.setOpaque(false);
    storeTVPosCB.setText(MessageUtils.getLocalizedMessage("idx_options.checkbox.store_tv_pos"));
    storeTVPosCB.setOpaque(false);
    storeTVOffCB.setText(MessageUtils.getLocalizedMessage("idx_options.checkbox.store_tv_off"));
    storeTVOffCB.setOpaque(false);
    storeTVPayCB.setText(MessageUtils.getLocalizedMessage("idx_options.checkbox.store_tv_pay"));
    storeTVPayCB.setOpaque(false);
    dimCountTF.setColumns(4);
    dimNumBytesTF.setColumns(4);
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
    JPanel panel = new JPanel();
    panel.setOpaque(false);
    panel.setLayout(new BoxLayout(panel, BoxLayout.PAGE_AXIS));
    panel.setBorder(BorderFactory.createEmptyBorder(15, 15, 15, 15));

    panel.add(indexOptions());
    panel.add(new JSeparator(JSeparator.HORIZONTAL));
    panel.add(tvOptions());
    panel.add(new JSeparator(JSeparator.HORIZONTAL));
    panel.add(dvOptions());
    panel.add(new JSeparator(JSeparator.HORIZONTAL));
    panel.add(pvOptions());
    panel.add(new JSeparator(JSeparator.HORIZONTAL));
    panel.add(footer());
    return panel;
  }

  private JPanel indexOptions() {
    JPanel panel = new JPanel();
    panel.setOpaque(false);
    panel.setLayout(new BoxLayout(panel, BoxLayout.PAGE_AXIS));

    JPanel inner1 = new JPanel(new FlowLayout(FlowLayout.LEADING, 10, 5));
    inner1.setOpaque(false);
    inner1.add(storedCB);

    inner1.add(tokenizedCB);
    inner1.add(omitNormsCB);
    panel.add(inner1);

    JPanel inner2 = new JPanel(new FlowLayout(FlowLayout.LEADING, 10, 1));
    inner2.setOpaque(false);
    JLabel idxOptLbl =
        new JLabel(MessageUtils.getLocalizedMessage("idx_options.label.index_options"));
    inner2.add(idxOptLbl);
    inner2.add(idxOptCombo);
    panel.add(inner2);

    return panel;
  }

  private JPanel tvOptions() {
    JPanel panel = new JPanel();
    panel.setOpaque(false);
    panel.setLayout(new BoxLayout(panel, BoxLayout.PAGE_AXIS));

    JPanel inner1 = new JPanel(new FlowLayout(FlowLayout.LEADING, 10, 2));
    inner1.setOpaque(false);
    inner1.add(storeTVCB);
    panel.add(inner1);

    JPanel inner2 = new JPanel(new FlowLayout(FlowLayout.LEADING, 10, 2));
    inner2.setOpaque(false);
    inner2.setBorder(BorderFactory.createEmptyBorder(0, 10, 0, 0));
    inner2.add(storeTVPosCB);
    inner2.add(storeTVOffCB);
    inner2.add(storeTVPayCB);
    panel.add(inner2);

    return panel;
  }

  private JPanel dvOptions() {
    JPanel panel = new JPanel(new FlowLayout(FlowLayout.LEADING, 10, 2));
    panel.setOpaque(false);
    JLabel dvTypeLbl = new JLabel(MessageUtils.getLocalizedMessage("idx_options.label.dv_type"));
    panel.add(dvTypeLbl);
    panel.add(dvTypeCombo);
    return panel;
  }

  private JPanel pvOptions() {
    JPanel panel = new JPanel();
    panel.setOpaque(false);
    panel.setLayout(new BoxLayout(panel, BoxLayout.PAGE_AXIS));

    JPanel inner1 = new JPanel(new FlowLayout(FlowLayout.LEADING, 10, 2));
    inner1.setOpaque(false);
    inner1.add(new JLabel(MessageUtils.getLocalizedMessage("idx_options.label.point_dims")));
    panel.add(inner1);

    JPanel inner2 = new JPanel(new FlowLayout(FlowLayout.LEADING, 10, 2));
    inner2.setOpaque(false);
    inner2.setBorder(BorderFactory.createEmptyBorder(0, 10, 0, 0));
    inner2.add(new JLabel(MessageUtils.getLocalizedMessage("idx_options.label.point_dc")));
    inner2.add(dimCountTF);
    inner2.add(new JLabel(MessageUtils.getLocalizedMessage("idx_options.label.point_nb")));
    inner2.add(dimNumBytesTF);
    panel.add(inner2);

    return panel;
  }

  private JPanel footer() {
    JPanel panel = new JPanel(new FlowLayout(FlowLayout.TRAILING));
    panel.setOpaque(false);
    JButton okBtn = new JButton(MessageUtils.getLocalizedMessage("button.ok"));
    okBtn.setMargin(new Insets(3, 3, 3, 3));
    okBtn.addActionListener(e -> saveOptions());
    panel.add(okBtn);
    JButton cancelBtn = new JButton(MessageUtils.getLocalizedMessage("button.cancel"));
    cancelBtn.setMargin(new Insets(3, 3, 3, 3));
    cancelBtn.addActionListener(e -> dialog.dispose());
    panel.add(cancelBtn);

    return panel;
  }

  // control methods

  public void setNewField(NewField nf) {
    this.nf = nf;

    storedCB.setSelected(nf.isStored());

    IndexableFieldType fieldType = nf.getFieldType();
    tokenizedCB.setSelected(fieldType.tokenized());
    omitNormsCB.setSelected(fieldType.omitNorms());
    idxOptCombo.setSelectedItem(fieldType.indexOptions().name());
    storeTVCB.setSelected(fieldType.storeTermVectors());
    storeTVPosCB.setSelected(fieldType.storeTermVectorPositions());
    storeTVOffCB.setSelected(fieldType.storeTermVectorOffsets());
    storeTVPayCB.setSelected(fieldType.storeTermVectorPayloads());
    dvTypeCombo.setSelectedItem(fieldType.docValuesType().name());
    dimCountTF.setText(String.valueOf(fieldType.pointDimensionCount()));
    dimNumBytesTF.setText(String.valueOf(fieldType.pointNumBytes()));

    if (nf.getType().equals(org.apache.lucene.document.TextField.class)
        || nf.getType().equals(StringField.class)
        || nf.getType().equals(Field.class)) {
      storedCB.setEnabled(true);
    } else {
      storedCB.setEnabled(false);
    }

    if (nf.getType().equals(Field.class)) {
      tokenizedCB.setEnabled(true);
      omitNormsCB.setEnabled(true);
      idxOptCombo.setEnabled(true);
      storeTVCB.setEnabled(true);
      storeTVPosCB.setEnabled(true);
      storeTVOffCB.setEnabled(true);
      storeTVPosCB.setEnabled(true);
    } else {
      tokenizedCB.setEnabled(false);
      omitNormsCB.setEnabled(false);
      idxOptCombo.setEnabled(false);
      storeTVCB.setEnabled(false);
      storeTVPosCB.setEnabled(false);
      storeTVOffCB.setEnabled(false);
      storeTVPayCB.setEnabled(false);
    }

    // TODO
    dvTypeCombo.setEnabled(false);
    dimCountTF.setEnabled(false);
    dimNumBytesTF.setEnabled(false);
  }

  private void saveOptions() {
    nf.setStored(storedCB.isSelected());
    if (nf.getType().equals(Field.class)) {
      FieldType ftype = (FieldType) nf.getFieldType();
      ftype.setStored(storedCB.isSelected());
      ftype.setTokenized(tokenizedCB.isSelected());
      ftype.setOmitNorms(omitNormsCB.isSelected());
      ftype.setIndexOptions(IndexOptions.valueOf((String) idxOptCombo.getSelectedItem()));
      ftype.setStoreTermVectors(storeTVCB.isSelected());
      ftype.setStoreTermVectorPositions(storeTVPosCB.isSelected());
      ftype.setStoreTermVectorOffsets(storeTVOffCB.isSelected());
      ftype.setStoreTermVectorPayloads(storeTVPayCB.isSelected());
    }
    dialog.dispose();
  }

  private static String[] availableIndexOptions() {
    return Arrays.stream(IndexOptions.values()).map(IndexOptions::name).toArray(String[]::new);
  }

  private static String[] availableDocValuesType() {
    return Arrays.stream(DocValuesType.values()).map(DocValuesType::name).toArray(String[]::new);
  }
}
