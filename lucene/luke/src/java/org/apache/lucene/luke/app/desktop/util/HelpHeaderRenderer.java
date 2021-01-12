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

package org.apache.lucene.luke.app.desktop.util;

import java.awt.Component;
import java.awt.FlowLayout;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.util.Objects;
import javax.swing.JComponent;
import javax.swing.JDialog;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JTable;
import javax.swing.UIManager;
import javax.swing.table.JTableHeader;
import javax.swing.table.TableCellRenderer;
import org.apache.lucene.luke.app.desktop.components.dialog.HelpDialogFactory;

/** Cell render class for table header with help dialog. */
public final class HelpHeaderRenderer implements TableCellRenderer {

  private JTable table;

  private final JPanel panel = new JPanel();

  private final JComponent helpContent;

  private final HelpDialogFactory helpDialogFactory;

  private final String title;

  private final String desc;

  private final JDialog parent;

  public HelpHeaderRenderer(
      String title, String desc, JComponent helpContent, HelpDialogFactory helpDialogFactory) {
    this(title, desc, helpContent, helpDialogFactory, null);
  }

  public HelpHeaderRenderer(
      String title,
      String desc,
      JComponent helpContent,
      HelpDialogFactory helpDialogFactory,
      JDialog parent) {
    this.title = title;
    this.desc = desc;
    this.helpContent = helpContent;
    this.helpDialogFactory = helpDialogFactory;
    this.parent = parent;
  }

  @Override
  @SuppressWarnings("unchecked")
  public Component getTableCellRendererComponent(
      JTable table, Object value, boolean isSelected, boolean hasFocus, int row, int column) {
    if (table != null && this.table != table) {
      this.table = table;
      final JTableHeader header = table.getTableHeader();
      if (header != null) {
        panel.setLayout(new FlowLayout(FlowLayout.LEADING));
        panel.setBorder(UIManager.getBorder("TableHeader.cellBorder"));
        panel.add(new JLabel(value.toString()));

        // add label with mouse click listener
        // when the label is clicked, help dialog will be displayed.
        JLabel helpLabel =
            new JLabel(
                FontUtils.elegantIconHtml(
                    "&#x74;", MessageUtils.getLocalizedMessage("label.help")));
        helpLabel.setHorizontalAlignment(JLabel.LEFT);
        helpLabel.setIconTextGap(5);
        panel.add(FontUtils.toLinkText(helpLabel));

        // add mouse listener to JTableHeader object.
        // see:
        // https://stackoverflow.com/questions/7137786/how-can-i-put-a-control-in-the-jtableheader-of-a-jtable
        header.addMouseListener(new HelpClickListener(column));
      }
    }
    return panel;
  }

  class HelpClickListener extends MouseAdapter {

    int column;

    HelpClickListener(int column) {
      this.column = column;
    }

    @Override
    public void mouseClicked(MouseEvent e) {
      showPopupIfNeeded(e);
    }

    private void showPopupIfNeeded(MouseEvent e) {
      JTableHeader header = (JTableHeader) e.getSource();
      int column = header.getTable().columnAtPoint(e.getPoint());
      if (column == this.column && e.getClickCount() == 1 && column != -1) {
        // only when the targeted column header is clicked, pop up the dialog
        if (Objects.nonNull(parent)) {
          new DialogOpener<>(helpDialogFactory)
              .open(
                  parent,
                  title,
                  600,
                  350,
                  (factory) -> {
                    factory.setDesc(desc);
                    factory.setContent(helpContent);
                  });
        } else {
          new DialogOpener<>(helpDialogFactory)
              .open(
                  title,
                  600,
                  350,
                  (factory) -> {
                    factory.setDesc(desc);
                    factory.setContent(helpContent);
                  });
        }
      }
    }
  }
}
