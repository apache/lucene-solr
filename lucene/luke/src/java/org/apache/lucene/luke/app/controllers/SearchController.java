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

package org.apache.lucene.luke.app.controllers;

import com.google.common.base.Strings;
import com.google.inject.Inject;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.fxml.FXML;
import javafx.scene.control.Accordion;
import javafx.scene.control.Button;
import javafx.scene.control.CheckBox;
import javafx.scene.control.ContextMenu;
import javafx.scene.control.Label;
import javafx.scene.control.MenuItem;
import javafx.scene.control.ScrollPane;
import javafx.scene.control.TableColumn;
import javafx.scene.control.TableView;
import javafx.scene.control.TextArea;
import javafx.scene.control.TextField;
import javafx.scene.control.TitledPane;
import javafx.scene.control.cell.PropertyValueFactory;
import javafx.stage.Stage;
import javafx.util.converter.IntegerStringConverter;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.Term;
import org.apache.lucene.luke.app.IndexHandler;
import org.apache.lucene.luke.app.IndexObserver;
import org.apache.lucene.luke.app.LukeState;
import org.apache.lucene.luke.app.controllers.dialog.ConfirmController;
import org.apache.lucene.luke.app.controllers.dialog.search.ExplanationController;
import org.apache.lucene.luke.app.controllers.dto.search.SearchResult;
import org.apache.lucene.luke.app.controllers.fragments.search.AnalyzerController;
import org.apache.lucene.luke.app.controllers.fragments.search.FieldValuesController;
import org.apache.lucene.luke.app.controllers.fragments.search.MLTController;
import org.apache.lucene.luke.app.controllers.fragments.search.QueryParserController;
import org.apache.lucene.luke.app.controllers.fragments.search.SimilarityController;
import org.apache.lucene.luke.app.controllers.fragments.search.SortController;
import org.apache.lucene.luke.app.util.DialogOpener;
import org.apache.lucene.luke.app.util.IntegerTextFormatter;
import org.apache.lucene.luke.models.LukeException;
import org.apache.lucene.luke.models.search.MLTConfig;
import org.apache.lucene.luke.models.search.QueryParserConfig;
import org.apache.lucene.luke.models.search.Search;
import org.apache.lucene.luke.models.search.SearchFactory;
import org.apache.lucene.luke.models.search.SearchResults;
import org.apache.lucene.luke.models.search.SimilarityConfig;
import org.apache.lucene.luke.models.tools.IndexTools;
import org.apache.lucene.luke.app.util.MessageUtils;
import org.apache.lucene.luke.models.tools.IndexToolsFactory;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TermQuery;

import javax.annotation.Nonnull;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.lucene.luke.app.util.ExceptionHandler.runnableWrapper;

public class SearchController extends ChildTabController implements IndexObserver {

  private static final int DEFAULT_PAGE_SIZE = 10;

  private final SearchFactory searchFactory;

  private final IndexToolsFactory toolsFactory;

  private final IndexHandler indexHandler;

  private Search searchModel;

  private IndexTools toolsModel;

  private Analyzer curAnalyzer;

  @FXML
  private Accordion settings;

  @FXML
  private TitledPane parserPane;

  @FXML
  private TitledPane mltPane;

  @FXML
  private ScrollPane parser;

  @FXML
  private QueryParserController parserController;

  @FXML
  private ScrollPane analyzer;

  @FXML
  private AnalyzerController analyzerController;

  @FXML
  private ScrollPane similarity;

  @FXML
  private SimilarityController similarityController;

  @FXML
  private ScrollPane sort;

  @FXML
  private SortController sortController;

  @FXML
  private ScrollPane values;

  @FXML
  private FieldValuesController valuesController;

  @FXML
  private MLTController mltController;

  @FXML
  private CheckBox termQuery;

  @FXML
  private TextArea queryExpr;

  @FXML
  private TextArea parsedQuery;

  @FXML
  private Button parseBtn;

  @FXML
  private CheckBox rewrite;

  @FXML
  private Button searchBtn;

  @FXML
  private Button mltBtn;

  @FXML
  private TextField mltDoc;

  @FXML
  private Label totalHits;

  @FXML
  private Label start;

  @FXML
  private Label end;

  @FXML
  private Button prev;

  @FXML
  private Button next;

  @FXML
  private Button delAll;

  @FXML
  private TableView<SearchResult> resultsTable;

  @FXML
  private TableColumn<SearchResult, Integer> docIdColumn;

  @FXML
  private TableColumn<SearchResult, Float> scoreColumn;

  @FXML
  private TableColumn<SearchResult, String> valuesColumn;

  private ObservableList<SearchResult> resultList;

  @Inject
  public SearchController(SearchFactory searchFactory, IndexToolsFactory toolsFactory, IndexHandler indexHandler) {
    this.searchFactory = searchFactory;
    this.toolsFactory = toolsFactory;
    this.indexHandler = indexHandler;
  }

  @FXML
  private void initialize() throws LukeException {
    settings.setExpandedPane(parserPane);
    parseBtn.setOnAction(e -> runnableWrapper(this::execParse));
    searchBtn.setOnAction(e -> runnableWrapper(this::execSearch));
    termQuery.setOnAction(e -> toggleTermQuery());

    mltDoc.setTextFormatter(new IntegerTextFormatter(new IntegerStringConverter(), 0));
    mltBtn.setOnAction(e -> runnableWrapper(this::execMLTSearch));

    totalHits.setText("0");
    start.setText("0");
    end.setText("0");

    next.setDisable(true);
    next.setOnAction(e -> runnableWrapper(this::nextPage));

    prev.setDisable(true);
    prev.setOnAction(e -> runnableWrapper(this::prevPage));

    delAll.setDisable(true);
    delAll.setOnAction(e -> runnableWrapper(this::showDeleteConfirmDialog));

    // initialize results table
    docIdColumn.setCellValueFactory(new PropertyValueFactory<>("docId"));
    scoreColumn.setCellValueFactory(new PropertyValueFactory<>("score"));
    valuesColumn.setCellValueFactory(new PropertyValueFactory<>("values"));
    resultList = FXCollections.observableArrayList();
    resultsTable.setItems(resultList);
    resultsTable.setContextMenu(createResultTableMenu());
  }

  private void toggleTermQuery() {
    if (termQuery.isSelected()) {
      settings.setDisable(true);
      parseBtn.setDisable(true);
      rewrite.setDisable(true);
      mltBtn.setDisable(true);
      mltDoc.setDisable(true);
      parsedQuery.setText("");
    } else {
      settings.setDisable(false);
      parseBtn.setDisable(false);
      rewrite.setDisable(false);
      mltBtn.setDisable(false);
      mltDoc.setDisable(false);
    }
  }

  @Override
  public void openIndex(LukeState state) throws LukeException {
    searchModel = searchFactory.newInstance(state.getIndexReader());
    toolsModel = toolsFactory.newInstance(state.getIndexReader(), state.useCompound(), state.keepAllCommits());
    sortController.setSearchModel(searchModel);

    queryExpr.setText("*:*");
    parserController.populateFields(searchModel.getSearchableFieldNames(), searchModel.getRangeSearchableFieldNames());
    sortController.populateFields(searchModel.getSortableFieldNames());
    valuesController.populateFields(searchModel.getFieldNames());
    mltController.populateFields(searchModel.getFieldNames());
  }

  @Override
  public void closeIndex() {
    searchModel = null;
    toolsModel = null;

    queryExpr.setText("");
    parsedQuery.setText("");
    totalHits.setText("0");
    start.setText("0");
    end.setText("0");
    next.setDisable(true);
    prev.setDisable(true);
    delAll.setDisable(true);
    resultList.clear();
  }

  private void execParse() throws LukeException {
    Query query = parse(rewrite.isSelected());
    parsedQuery.setText(query.toString());
    clearStatusMessage();
  }

  private void execSearch() throws LukeException {
    Query query;
    if (termQuery.isSelected()) {
      // term query
      if (Strings.isNullOrEmpty(queryExpr.getText())) {
        throw new LukeException("Query is not set.");
      }
      String[] tmp = queryExpr.getText().split(":");
      if (tmp.length < 2) {
        throw new LukeException(String.format("Invalid query [ %s ]", queryExpr.getText()));
      }
      query = new TermQuery(new Term(tmp[0].trim(), tmp[1].trim()));
    } else {
      query = parse(false);
    }
    SimilarityConfig simConfig = similarityController.getConfig();
    Sort sort = sortController.getSort();
    Set<String> fieldsToLoad = valuesController.getFieldsToLoad();
    resultList.clear();
    SearchResults results = searchModel.search(query, simConfig, sort, fieldsToLoad, DEFAULT_PAGE_SIZE);
    populateResults(results);
  }

  private void execMLTSearch() throws LukeException {
    if (Strings.isNullOrEmpty(mltDoc.getText())) {
      throw new LukeException("Doc num is not set.");
    }
    int docNum = Integer.parseInt(mltDoc.getText());
    MLTConfig mltConfig = mltController.getMLTConfig();

    Query query = searchModel.mltQuery(docNum, mltConfig, curAnalyzer);
    Set<String> fieldsToLoad = valuesController.getFieldsToLoad();
    resultList.clear();
    SearchResults results = searchModel.search(query, new SimilarityConfig.Builder().build(), fieldsToLoad, DEFAULT_PAGE_SIZE);
    populateResults(results);
  }

  private void nextPage() throws LukeException {
    resultList.clear();
    searchModel.nextPage().ifPresent(this::populateResults);
  }

  private void prevPage() throws LukeException {
    resultList.clear();
    searchModel.prevPage().ifPresent(this::populateResults);
  }

  private Query parse(boolean rewrite) throws LukeException {
    String expr = Strings.isNullOrEmpty(queryExpr.getText()) ? "*:*" : queryExpr.getText();
    String df = parserController.getDefField();
    QueryParserConfig config = parserController.getConfig();
    return searchModel.parseQuery(expr, df, curAnalyzer, config, rewrite);
  }

  private void populateResults(SearchResults res) {
    totalHits.setText(String.valueOf(res.getTotalHits()));
    if (res.getTotalHits() > 0) {
      start.setText(String.valueOf(res.getOffset() + 1));
      end.setText(String.valueOf(res.getOffset() + res.size()));
      resultList.addAll(res.getHits().stream().map(SearchResult::of).collect(Collectors.toList()));

      prev.setDisable(res.getOffset() == 0);
      next.setDisable(res.getTotalHits() <= res.getOffset() + res.size());

      if (!indexHandler.getState().readOnly() && indexHandler.getState().hasDirectoryReader()) {
        delAll.setDisable(false);
      }
    } else {
      start.setText("0");
      end.setText("0");
      prev.setDisable(true);
      next.setDisable(true);
      delAll.setDisable(true);
    }
  }

  private Stage confirmDialog;

  private void showDeleteConfirmDialog() throws Exception {
    confirmDialog = new DialogOpener<ConfirmController>(getParent()).show(
        confirmDialog,
        "Confirm Deletion",
        "/fxml/dialog/confirm.fxml",
        400, 200,
        (controller) -> {
          controller.setContent(MessageUtils.getLocalizedMessage("search.message.delete_confirm"));
          controller.setCallback(this::deleteAllDocs);
        },
        "/styles/confirm.css"
    );
  }

  private void deleteAllDocs() throws LukeException {
    Query query = searchModel.getCurrentQuery();
    if (query != null) {
      toolsModel.deleteDocuments(query);
      indexHandler.reOpen();
      showStatusMessage(MessageUtils.getLocalizedMessage("search.message.delete_success", query.toString()));
    }
    delAll.setDisable(true);
  }

  private Stage explanationDialog;

  private ContextMenu createResultTableMenu() {
    ContextMenu menu = new ContextMenu();
    MenuItem item1 = new MenuItem(MessageUtils.getLocalizedMessage("search.results.menu.explain"));
    item1.setOnAction(e -> runnableWrapper(() -> {
      SearchResult result = resultsTable.getSelectionModel().getSelectedItem();
      Explanation explanation = searchModel.explain(parse(false), result.getDocId());
      explanationDialog = new DialogOpener<ExplanationController>(getParent()).show(
          explanationDialog,
          "Explanation",
          "/fxml/dialog/search/explanation.fxml",
          600, 400,
          (controller) -> {
            controller.setDocNum(result.getDocId());
            controller.setExplanation(explanation);
          }
      );
    }));

    MenuItem item2 = new MenuItem(MessageUtils.getLocalizedMessage("search.results.menu.showdoc"));
    item2.setOnAction(e -> runnableWrapper(() -> {
      SearchResult result = resultsTable.getSelectionModel().getSelectedItem();
      getDocumentsController().displayDoc(result.getDocId());
      switchTab(LukeController.Tab.DOCUMENTS);
    }));

    menu.getItems().addAll(item1, item2);
    return menu;
  }

  @Override
  public void setParent(LukeController parent) {
    super.setParent(parent);
    analyzerController.setParent(parent);
    mltController.setParent(parent);
  }

  // -------------------------------------------------
  // methods for interaction with other controllers
  // -------------------------------------------------

  void setCurrentAnalyzer(Analyzer analyzer) {
    this.curAnalyzer = analyzer;
    analyzerController.setCurrentAnalyzer(analyzer);
    mltController.setCurrentAnalyzer(analyzer);
  }

  void searchByTerm(@Nonnull String fieldName, @Nonnull String termText) throws LukeException {
    termQuery.selectedProperty().setValue(true);
    toggleTermQuery();
    queryExpr.setText(String.format("%s:%s", fieldName, termText));
    resultList.clear();
    execSearch();
  }

  void mltSearch(int docNum) throws LukeException {
    mltDoc.setText(String.valueOf(docNum));
    settings.setExpandedPane(mltPane);
    execMLTSearch();
  }

}
