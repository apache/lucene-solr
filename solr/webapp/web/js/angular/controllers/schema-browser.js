/*
 Licensed to the Apache Software Foundation (ASF) under one or more
 contributor license agreements.  See the NOTICE file distributed with
 this work for additional information regarding copyright ownership.
 The ASF licenses this file to You under the Apache License, Version 2.0
 (the "License"); you may not use this file except in compliance with
 the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
*/

var cookie_schema_browser_autoload = 'schema-browser_autoload';

solrAdminApp.controller('SchemaBrowserController',
    function($scope, $routeParams, $location, $cookies, Luke, Constants) {
        $scope.resetMenu("schema-browser", Constants.IS_COLLECTION_PAGE);

        $scope.refresh = function () {
            Luke.schema({core: $routeParams.core}, function (schema) {
                Luke.index({core: $routeParams.core}, function (index) {

                    var data = mergeIndexAndSchemaData(index, schema.schema);

                    $scope.fieldsAndTypes = getFieldsAndTypes(data);
                    $scope.is = {};

                    var search = $location.search();
                    leftbar = {};
                    $scope.isField = $scope.isDynamicField = $scope.isType = false;
                    if (search.field) {
                        $scope.selectedType = "Field";
                        $scope.is.field = true;
                        $scope.name = search.field;
                        leftbar.fields = [$scope.name];
                        var field = data.fields[$scope.name];
                        leftbar.types = [field.type];
                        if (field.dynamicBase) leftbar.dynamicFields = [field.dynamicBase];
                        if (field.copySources && field.copySources.length>0) leftbar.copyFieldSources = field.copySources.sort();
                        if (field.copyDests && field.copyDests.length>0) leftbar.copyFieldDests = field.copyDests.sort();
                        $scope.fieldOrType = "field=" + $scope.name;
                    } else if (search["dynamic-field"]) {
                        $scope.selectedType = "Dynamic Field";
                        $scope.is.dynamicField = true;
                        $scope.name = search["dynamic-field"];
                        leftbar.dynamicFields = [$scope.name];
                        leftbar.types = [data.dynamic_fields[$scope.name].type];
                        $scope.fieldOrType = "dynamic-field=" + $scope.name;
                    } else if (search.type) {
                        $scope.selectedType = "Type";
                        $scope.is.type = true;
                        $scope.name = search.type;
                        leftbar.types = [$scope.name];
                        leftbar.fields = filterFields("fields", data, $scope.name);
                        leftbar.dynamicFields = filterFields("dynamic_fields", data, $scope.name);
                        $scope.fieldOrType = "type=" + $scope.name;
                    }
                    $scope.leftbar = leftbar;
                    $scope.core = $routeParams.core;
                    $scope.defaultSearchField = data.default_search_field;
                    $scope.uniqueKeyField = data.unique_key_field;
                    $scope.isDefaultSearchField = ($scope.selectedType == "Field" && $scope.name == $scope.defaultSearchField);
                    $scope.isUniqueKeyField = ($scope.selectedType == "Field" && $scope.name == $scope.uniqueKeyField);

                    $scope.display = getFieldProperties(index, $routeParams.core, $scope.is, $scope.name);
                    $scope.analysis = getAnalysisInfo(data, $scope.is, $scope.name);

                    $scope.isAutoload = $cookies[cookie_schema_browser_autoload] == "true";
                    if ($scope.isAutoload) {
                        $scope.toggleTerms();
                    }
                });
            });
        };
        $scope.refresh();

        $scope.selectFieldOrType = function() {
            $location.search($scope.fieldOrType);
        }

        $scope.toggleAnalyzer = function(analyzer) {
            analyzer.show = !analyzer.show;
        }

        $scope.loadTermInfo = function() {
            var params = {fl: $scope.name, core: $routeParams.core};
            if ($scope.topTermsCount) {
                params.numTerms = $scope.topTermsCount;
            }
            $scope.isLoadingTerms = true;
            Luke.field(params, function (data) {
                $scope.isLoadingTerms = false;
                $scope.termInfo = getTermInfo(data.fields[$scope.name]);
                if (!$scope.topTermsCount) {
                    $scope.topTermsCount = $scope.termInfo.termCount;
                }
            });
        }

        $scope.toggleTerms = function() {
            $scope.showTerms = !$scope.showTerms;

            if ($scope.showTerms) {
                $scope.loadTermInfo();
            }
        }

        $scope.loadAllTerms = function() {
            $scope.topTermsCount = $scope.termInfo.maxTerms;
            $scope.loadTermInfo();
        }

        $scope.toggleAutoload = function() {
            $scope.isAutoload = !$scope.isAutoload;
            $cookies[cookie_schema_browser_autoload] = $scope.isAutoload;
            console.log("cookie: " + $cookies[cookie_schema_browser_autoload]);
        }
    }
);

var getFieldsAndTypes = function(data) {
    var fieldsAndTypes = [];
    for (var field in data.fields) {
        fieldsAndTypes.push({
            group: "Fields",
            value: "field=" + field,
            label: field
        });
    }
    for (var field in data.dynamic_fields) {
        fieldsAndTypes.push({
            group: "Dynamic Fields",
            value: "dynamic-field=" + field,
            label: field
        });
    }
    for (var type in data.types) {
        fieldsAndTypes.push({
            group: "Types",
            value: "type=" + type,
            label: type
        });
    }
    return fieldsAndTypes;
};

var filterFields = function(type, data, name) {
    var fields = [];
    for (var i in data.types[name].fields) {
        var field = data.types[name].fields[i];
        if (data[type][field]) {
            fields.push(field)
        }
    }
    return fields.sort();
}

var mergeIndexAndSchemaData = function(index, schema) {

    var data = {
        default_search_field: null,
        unique_key_field: null,
        key: {},
        fields: {},
        dynamic_fields: {},
        types: {},
        relations: {
            f_df: {},
            f_t: {},
            df_f: {},
            df_t: {},
            t_f: {},
            t_df: {}
        }
    };

    data.fields = index.fields;

    data.key = index.info.key;

    data.default_search_field = schema.defaultSearchField;
    data.unique_key_field = schema.uniqueKeyField;

    data.dynamic_fields = schema.dynamicFields;
    data.types = schema.types;

    for (var field in schema.fields) {
        data.fields[field] =
            $.extend({}, data.fields[field], schema.fields[field]);
    }

    for (var field in data.fields) {
        var copy_dests = data.fields[field].copyDests;
        for (var i in copy_dests) {
            var copy_dest = copy_dests[i];
            if (!data.fields[copy_dest]) {
                data.fields[copy_dest] = {
                    partial: true,
                    copySources: []
                };
            }

            if (data.fields[copy_dest].partial) {
                data.fields[copy_dest].copySources.push(field);
            }
        }

        var copy_sources = data.fields[field].copySources;
        for (var i in copy_sources) {
            var copy_source = copy_sources[i];
            if (!data.fields[copy_source]) {
                data.fields[copy_source] = {
                    partial: true,
                    copyDests: []
                };
            }

            if (data.fields[copy_source].partial) {
                data.fields[copy_source].copyDests.push(field);
            }
        }

        data.relations.f_t[field] = data.fields[field].type;

        if (!data.relations.t_f[data.fields[field].type]) {
            data.relations.t_f[data.fields[field].type] = [];
        }
        data.relations.t_f[data.fields[field].type].push(field);

        if (data.fields[field].dynamicBase) {
            data.relations.f_df[field] = data.fields[field].dynamicBase;

            if (!data.relations.df_f[data.fields[field].dynamicBase]) {
                data.relations.df_f[data.fields[field].dynamicBase] = [];
            }
            data.relations.df_f[data.fields[field].dynamicBase].push(field);
        }
    }

    for (var dynamic_field in data.dynamic_fields) {
        data.relations.df_t[dynamic_field] = data.dynamic_fields[dynamic_field].type;

        if (!data.relations.t_df[data.dynamic_fields[dynamic_field].type]) {
            data.relations.t_df[data.dynamic_fields[dynamic_field].type] = [];
        }
        data.relations.t_df[data.dynamic_fields[dynamic_field].type].push(dynamic_field);
    }
    return data;
};

var getFieldProperties = function(data, core, is, field) {

    var display = {};

    display.partialState = is.field && !!data.fields[field].partial;

    display.columns = [];
    display.rows = [];
    var allFlags = "";

    var addRow = function(name, flags) {
        if (flags[0]!='(') {
            display.rows.push({name:name, flags:flags});
            for (var i in flags) {
                if (flags[i]!="-" && allFlags.indexOf(flags[i])<0) {
                    allFlags+=flags[i];
                }
            }
        } else {
            display.rows.push({name:name, comment:flags});
        }
    }

    // Identify the rows for our field property table
    if (is.field && data.fields[field]) {
        if (data.fields[field].flags) {
            addRow('Properties', data.fields[field].flags);
        }
        if (data.fields[field].schema) {
            addRow('Schema', data.fields[field].schema);
        }
        if (data.fields[field].index) {
            addRow('Index', data.fields[field].index);
        }
        display.docs = data.fields[field].docs;
        display.docsUrl = "#/" + core + "/query?q=" + field + ":[* TO *]";
        display.distinct = data.fields[field].distinct;
        display.positionIncrementGap = data.fields[field].positionIncrementGap;
        display.similarity = data.fields[field].similarity;
    } else if (is.dynamicField && data.dynamic_fields[field] && data.dynamic_fields[field].flags) {
        addRow('Properties', data.dynamic_fields[field].flags);
    }

    // identify columns in field property table:
    for (var key in data.info.key) {
        if (allFlags.indexOf(key)>=0) {
            display.columns.push({key: key, name: data.info.key[key]});
        }
    }

    // identify rows and cell values in field property table:
    for (var i in display.rows) {
        var row = display.rows[i];
        row.cells = [];

        for (var j in display.columns) {
            var flag = display.columns[j].key;
            row.cells.push({key: flag, value: row.flags.indexOf(flag)>=0});
        }
    }

    return display;
};

var getAnalysisInfo = function(data, is, name) {

    var analysis = {};

    if (is.field) {
        var type = data.relations.f_t[name];
        analysis.query = "analysis.fieldname=" + name;
    }
    else if (is.dynamicField) {
        var type = data.relations.df_t[name];
        analysis.query = "analysis.fieldtype=" + type;
    }
    else if (is.type) {
        var type = name;
        analysis.query = "analysis.fieldtype=" + name;
    }

    var processComponentType = function (label, key, componentTypeData) {
        if (componentTypeData) {
            var components = [];
                console.dir(componentTypeData);
            for (var componentName in componentTypeData) {
                console.log(componentName);
                var componentData = componentTypeData[componentName];
                var component = {className: componentData.className, args:[]};
                if (componentData.args) {
                    for (var argName in componentData.args) {
                        var argValue = componentData.args[argName];
                        if (argValue == "1" || argValue == "true") {
                            component.args.push({name: argName, booleanValue:true});
                        } else if (argValue == "0" || argValue == "false") {
                            component.args.push({name: argName, booleanValue:false});
                        } else {
                            component.args.push({name: argName, value:argValue});
                        }
                    }
                }
                components.push(component);
            }
            return {label: label, key: key, components: components};
        } else {
            return {label: label, key: key};
        }
    }

    var buildAnalyzer = function (analyzerData) {
        var analyzer = {};
        analyzer.className = analyzerData.className;
        analyzer.componentTypes = [];
        if (analyzerData.tokenizer) {
            analyzer.componentTypes.push(processComponentType("Char Filters", "charFilters", analyzerData.charFilters));
            analyzer.componentTypes.push(processComponentType("Tokenizer", "tokenizer", {tokenizer: analyzerData.tokenizer}));
            analyzer.componentTypes.push(processComponentType("Token Filters", "tokenFilters", analyzerData.filters));
        }
        return analyzer;
    }

    analysis.data = data.types[type];
    if (analysis.data) {
        analysis.analyzers = [
            {key: "index", name: "Index", detail: buildAnalyzer(analysis.data.indexAnalyzer)},
            {key: "query", name: "Query", detail: buildAnalyzer(analysis.data.queryAnalyzer)}
        ];
    }
    return analysis;
}

var getTermInfo = function(data) {

    var termInfo = {};
    if (data && data.topTerms) {
        termInfo.topTerms = [];

        var currentGroup = {count: 0}
        for (var i = 0; i < data.topTerms.length; i += 2) {
            var count = data.topTerms[i + 1];
            if (currentGroup.count != count) {
                currentGroup = {count: count, terms: []};
                termInfo.topTerms.push(currentGroup);
            }
            currentGroup.terms.push(data.topTerms[i]);
        }
        termInfo.termCount = data.topTerms.length / 2;
        termInfo.maxTerms = data.distinct;
    }

    if(data && data.histogram) {
        termInfo.histogram = [];
        termInfo.histogramMax = 0;
        for (var i = 0; i < data.histogram.length; i += 2) {
            termInfo.histogram.push({key: data.histogram[i], value: data.histogram[i + 1]});
            termInfo.histogramMax = Math.max(termInfo.histogramMax, data.histogram[i + 1]);
        }
    }
    return termInfo;
};

/*
        var get_width = function get_width()
        {
          return $( this ).width();
        }

  var max_width = 10 + Math.max.apply( Math, $( 'p', topterms_table_element ).map( get_width ).get() );
  topterms:
    p { width: {{maxWidth}}px !important; }
    ul { margin-left: {{max_width + 5 }}px !important; }

  var max_width = 10 + Math.max.apply( Math, $( 'dt', histogram_holder_element ).map( get_width ).get() );
  histogram_holder:
    ul { margin-left: {{maxWidth}}px !important; }
    li dt { left: {{-maxWidth}}px !important; width: {{maxWidth}}px !important; }
*/
