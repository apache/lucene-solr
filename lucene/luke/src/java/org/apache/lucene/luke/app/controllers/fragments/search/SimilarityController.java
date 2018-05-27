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

package org.apache.lucene.luke.app.controllers.fragments.search;

import javafx.fxml.FXML;
import javafx.scene.control.CheckBox;
import javafx.scene.control.TextField;
import org.apache.lucene.luke.models.LukeException;
import org.apache.lucene.luke.models.search.SimilarityConfig;

public class SimilarityController {

  private SimilarityConfig config = new SimilarityConfig.Builder().build();

  @FXML
  private CheckBox useClassic;

  @FXML
  private CheckBox discountOverlaps;

  @FXML
  private TextField k1Val;

  @FXML
  private TextField bVal;

  @FXML
  private void initialize() {
    useClassic.setSelected(config.isUseClassicSimilarity());
    useClassic.setOnAction(e -> {
      if (useClassic.isSelected()) {
        k1Val.setDisable(true);
        bVal.setDisable(true);
      } else {
        k1Val.setDisable(false);
        bVal.setDisable(false);
      }
    });

    discountOverlaps.setSelected(config.isUseClassicSimilarity());
    k1Val.setText(String.valueOf(config.getK1()));
    bVal.setText(String.valueOf(config.getB()));
  }

  public SimilarityConfig getConfig() throws LukeException {
    float k1;
    try {
      k1 = Float.parseFloat(k1Val.getText());
    } catch (NumberFormatException e) {
      throw new LukeException("Invalid input for k1: " + k1Val.getText());
    }

    float b;
    try {
      b = Float.parseFloat(bVal.getText());
    } catch (NumberFormatException e) {
      throw new LukeException("Invalid input for b: " + bVal.getText());
    }

    return new SimilarityConfig.Builder()
        .useClassicSimilarity(useClassic.isSelected())
        .discountOverlaps(discountOverlaps.isSelected())
        .k1(k1)
        .b(b)
        .build();
  }
}
