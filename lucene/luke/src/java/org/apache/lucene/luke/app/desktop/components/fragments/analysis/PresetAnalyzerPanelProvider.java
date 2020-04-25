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

package org.apache.lucene.luke.app.desktop.components.fragments.analysis;

import javax.swing.BorderFactory;
import javax.swing.ComboBoxModel;
import javax.swing.DefaultComboBoxModel;
import javax.swing.JComboBox;
import javax.swing.JLabel;
import javax.swing.JPanel;
import java.awt.BorderLayout;
import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.event.ActionEvent;
import java.util.Collection;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.luke.app.desktop.components.AnalysisTabOperator;
import org.apache.lucene.luke.app.desktop.components.ComponentOperatorRegistry;
import org.apache.lucene.luke.app.desktop.util.MessageUtils;

/** Provider of the preset analyzer panel */
public final class PresetAnalyzerPanelProvider implements PresetAnalyzerPanelOperator {

  private final ComponentOperatorRegistry operatorRegistry;

  private final JComboBox<String> analyzersCB = new JComboBox<>();

  private final ListenerFunctions listeners = new ListenerFunctions();

  public PresetAnalyzerPanelProvider() {
    this.operatorRegistry = ComponentOperatorRegistry.getInstance();
    operatorRegistry.register(PresetAnalyzerPanelOperator.class, this);
  }

  public JPanel get() {
    JPanel panel = new JPanel(new BorderLayout());
    panel.setOpaque(false);
    panel.setBorder(BorderFactory.createEmptyBorder(3, 3, 3, 3));

    JLabel header = new JLabel(MessageUtils.getLocalizedMessage("analysis_preset.label.preset"));
    panel.add(header, BorderLayout.PAGE_START);

    JPanel center = new JPanel(new FlowLayout(FlowLayout.LEADING));
    center.setOpaque(false);
    center.setBorder(BorderFactory.createEmptyBorder(10, 10, 10, 10));
    center.setPreferredSize(new Dimension(400, 40));
    analyzersCB.addActionListener(listeners::setAnalyzer);
    analyzersCB.setEnabled(false);
    center.add(analyzersCB);
    panel.add(center, BorderLayout.CENTER);

    return panel;
  }

  // control methods

  @Override
  public void setPresetAnalyzers(Collection<Class<? extends Analyzer>> presetAnalyzers) {
    String[] analyzerNames = presetAnalyzers.stream().map(Class::getName).toArray(String[]::new);
    ComboBoxModel<String> model = new DefaultComboBoxModel<>(analyzerNames);
    analyzersCB.setModel(model);
    analyzersCB.setEnabled(true);
  }

  @Override
  public void setSelectedAnalyzer(Class<? extends Analyzer> analyzer) {
    analyzersCB.setSelectedItem(analyzer.getName());
  }

  private class ListenerFunctions {

    void setAnalyzer(ActionEvent e) {
      operatorRegistry.get(AnalysisTabOperator.class).ifPresent(operator ->
          operator.setAnalyzerByType((String) analyzersCB.getSelectedItem())
      );
    }

  }

}
