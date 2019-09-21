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
import javax.swing.JCheckBox;
import javax.swing.JFormattedTextField;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import java.awt.Color;
import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.GridLayout;
import java.awt.event.ActionEvent;

import org.apache.lucene.luke.app.desktop.components.ComponentOperatorRegistry;
import org.apache.lucene.luke.app.desktop.util.MessageUtils;
import org.apache.lucene.luke.app.desktop.util.StyleConstants;
import org.apache.lucene.luke.models.search.SimilarityConfig;

/** Provider of the Similarity pane */
public final class SimilarityPaneProvider implements SimilarityTabOperator {

  private final JCheckBox tfidfCB = new JCheckBox();

  private final JCheckBox discardOverlapsCB = new JCheckBox();

  private final JFormattedTextField k1FTF = new JFormattedTextField();

  private final JFormattedTextField bFTF = new JFormattedTextField();

  private final SimilarityConfig config = new SimilarityConfig.Builder().build();

  private final ListenerFunctions listeners = new ListenerFunctions();

  public SimilarityPaneProvider() {
    ComponentOperatorRegistry.getInstance().register(SimilarityTabOperator.class, this);
  }

  public JScrollPane get() {
    JPanel panel = new JPanel();
    panel.setOpaque(false);
    panel.setLayout(new BoxLayout(panel, BoxLayout.PAGE_AXIS));
    panel.setBorder(BorderFactory.createEmptyBorder(10, 10, 10, 10));

    panel.add(initSimilaritySettingsPanel());

    JScrollPane scrollPane = new JScrollPane(panel);
    scrollPane.setOpaque(false);
    scrollPane.getViewport().setOpaque(false);
    return scrollPane;
  }

  private JPanel initSimilaritySettingsPanel() {
    JPanel panel = new JPanel(new GridLayout(4, 1));
    panel.setOpaque(false);
    panel.setMaximumSize(new Dimension(700, 220));

    tfidfCB.setText(MessageUtils.getLocalizedMessage("search_similarity.checkbox.use_classic"));
    tfidfCB.addActionListener(listeners::toggleTfIdf);
    tfidfCB.setOpaque(false);
    panel.add(tfidfCB);

    discardOverlapsCB.setText(MessageUtils.getLocalizedMessage("search_similarity.checkbox.discount_overlaps"));
    discardOverlapsCB.setSelected(config.isUseClassicSimilarity());
    discardOverlapsCB.setOpaque(false);
    panel.add(discardOverlapsCB);

    JLabel bm25Label = new JLabel(MessageUtils.getLocalizedMessage("search_similarity.label.bm25_params"));
    panel.add(bm25Label);

    JPanel bm25Params = new JPanel(new FlowLayout(FlowLayout.LEADING));
    bm25Params.setOpaque(false);
    bm25Params.setBorder(BorderFactory.createEmptyBorder(0, 20, 0, 0));

    JPanel k1Val = new JPanel(new FlowLayout(FlowLayout.LEADING));
    k1Val.setOpaque(false);
    k1Val.add(new JLabel("k1: "));
    k1FTF.setColumns(5);
    k1FTF.setValue(config.getK1());
    k1Val.add(k1FTF);
    k1Val.add(new JLabel(MessageUtils.getLocalizedMessage("label.float_required")));
    bm25Params.add(k1Val);

    JPanel bVal = new JPanel(new FlowLayout(FlowLayout.LEADING));
    bVal.setOpaque(false);
    bVal.add(new JLabel("b: "));
    bFTF.setColumns(5);
    bFTF.setValue(config.getB());
    bVal.add(bFTF);
    bVal.add(new JLabel(MessageUtils.getLocalizedMessage("label.float_required")));
    bm25Params.add(bVal);

    panel.add(bm25Params);

    return panel;
  }

  @Override
  public SimilarityConfig getConfig() {
    float k1 = (float) k1FTF.getValue();
    float b = (float) bFTF.getValue();
    return new SimilarityConfig.Builder()
        .useClassicSimilarity(tfidfCB.isSelected())
        .discountOverlaps(discardOverlapsCB.isSelected())
        .k1(k1)
        .b(b)
        .build();
  }

  private class ListenerFunctions {

    void toggleTfIdf(ActionEvent e) {
      if (tfidfCB.isSelected()) {
        k1FTF.setEnabled(false);
        k1FTF.setBackground(StyleConstants.DISABLED_COLOR);
        bFTF.setEnabled(false);
        bFTF.setBackground(StyleConstants.DISABLED_COLOR);
      } else {
        k1FTF.setEnabled(true);
        k1FTF.setBackground(Color.white);
        bFTF.setEnabled(true);
        bFTF.setBackground(Color.white);
      }
    }
  }

}
