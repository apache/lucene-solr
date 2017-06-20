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

solrAdminApp.controller('SchemaController',
    function($scope, $routeParams, $location, $cookies, $timeout, Luke, Constants, Schema, Config) {
        $scope.resetMenu("schema", Constants.IS_COLLECTION_PAGE);

        $scope.refresh = function () {
            Luke.schema({core: $routeParams.core}, function (schema) {
                Luke.raw({core: $routeParams.core}, function (index) {
                    var data = mergeIndexAndSchemaData(index, schema.schema);

                    $scope.fieldsAndTypes = getFieldsAndTypes(data);
                    $scope.is = {};

                    var search = $location.search();
                    leftbar = {};
                    $scope.isField = $scope.isDynamicField = $scope.isType = false;
                    $scope.showing = true;
                    if (search.field) {
                        $scope.selectedType = "Field";
                        $scope.is.field = true;
                        $scope.name = search.field;
                        leftbar.fields = [$scope.name];
                        var field = data.fields[$scope.name];
                        leftbar.types = [field.type];
                        if (field.dynamicBase) leftbar.dynamicFields = [field.dynamicBase];
                        if (field.copySources && field.copySources.length>0) {
                            leftbar.copyFieldSources = sortedObjectArray(field.copySources.sort());
                        }
                        if (field.copyDests && field.copyDests.length>0) {
                            leftbar.copyFieldDests = sortedObjectArray(field.copyDests.sort());
                        }
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
                    } else {
                        $scope.showing = false;
                    }
                    $scope.leftbar = leftbar;
                    $scope.core = $routeParams.core;
                    $scope.uniqueKeyField = data.unique_key_field;
                    $scope.similarity = data.similarity; 
                    if ($scope.similarity && $scope.similarity.className) {
                        $scope.similarity.className = shortenPackages($scope.similarity.className); 
                    }
                    $scope.isUniqueKeyField = ($scope.selectedType == "Field" && $scope.name == $scope.uniqueKeyField);

                    $scope.display = getFieldProperties(data, $routeParams.core, $scope.is, $scope.name);
                    $scope.analysis = getAnalysisInfo(data, $scope.is, $scope.name);

                    $scope.isAutoload = $cookies[cookie_schema_browser_autoload] == "true";
                    if ($scope.isAutoload) {
                        $scope.toggleTerms();
                    }

                    $scope.types = Object.keys(schema.schema.types);
                });
            });
            Config.get({core: $routeParams.core}, function(data) {
                $scope.isSchemaUpdatable = (data.config.hasOwnProperty('schemaFactory') == false || data.config.schemaFactory.class == "ManagedIndexSchemaFactory");
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

        $scope.hideAll = function() {
            $scope.showAddField = false;
            $scope.showAddDynamicField = false;
            $scope.showAddCopyField = false;
        }

        $scope.toggleAddField = function() {
            if ($scope.showAddField && $scope.adding == "field") {
                $scope.hideAll();
            } else {
                $scope.hideAll();
                $scope.showAddField = true;
                $scope.adding = "field";

                $scope.newField = {
                    stored: "true",
                    indexed: "true"
                }
                delete $scope.addErrors;
            }
        }

        $scope.addField = function() {
            delete $scope.addErrors;
            var data = {"add-field": $scope.newField};
            Schema.post({core: $routeParams.core}, data, function(data) {
                if (data.errors) {
                    $scope.addErrors = data.errors[0].errorMessages;
                    if (typeof $scope.addErrors === "string") {
                        $scope.addErrors = [$scope.addErrors];
                    }
                } else {
                    $scope.added = true;
                    $timeout(function() {
                        $scope.showAddField = false;
                        $scope.added = false;
                        $scope.refresh();
                    }, 1500);
                }
            });
        }

        $scope.toggleAddDynamicField = function() {
            if ($scope.showAddField && $scope.adding == "dynamicField") {
                $scope.hideAll();
            } else {
                $scope.hideAll();
                $scope.showAddField = true;
                $scope.adding = "dynamicField";

                $scope.newField = {
                    stored: "true",
                    indexed: "true"
                }
                delete $scope.addErrors;
            }
        }

        $scope.addDynamicField = function() {
            delete $scope.addErrors;
            var data = {"add-dynamic-field": $scope.newField};
            Schema.post({core: $routeParams.core}, data, function(data) {
                if (data.errors) {
                    $scope.addErrors = data.errors[0].errorMessages;
                    if (typeof $scope.addErrors === "string") {
                        $scope.addErrors = [$scope.addErrors];
                    }
                } else {
                    $scope.added = true;
                    $timeout(function() {
                        $scope.showAddField = false;
                        $scope.added = false;
                        $scope.refresh();
                    }, 1500);
                }
            });
        }

        $scope.toggleAddCopyField = function() {
            if ($scope.showAddCopyField) {
                $scope.hideAll();
            } else {
                $scope.hideAll();
                $scope.showAddCopyField = true;

                $scope.copyField = {};
                delete $scope.addCopyFieldErrors;
            }
        }
        $scope.addCopyField = function() {
            delete $scope.addCopyFieldErrors;
            var data = {"add-copy-field": $scope.copyField};
            Schema.post({core: $routeParams.core}, data, function(data) {
                if (data.errors) {
                    $scope.addCopyFieldErrors = data.errors[0].errorMessages;
                    if (typeof $scope.addCopyFieldErrors === "string") {
                        $scope.addCopyFieldErrors = [$scope.addCopyFieldErrors];
                    }
                } else {
                    $scope.showAddCopyField = false;
                    $timeout($scope.refresh, 1500);
                }
            });
        }

        $scope.toggleDelete = function() {
            if ($scope.showDelete) {
                $scope.showDelete = false;
            } else {
                if ($scope.is.field) {
                    $scope.deleteData = {'delete-field': {name: $scope.name}};
                } else if ($scope.is.dynamicField) {
                    $scope.deleteData = {'delete-dynamic-field': {name: $scope.name}};
                } else {
                    alert("TYPE NOT KNOWN");
                }
                $scope.showDelete = true;
            }
        }

        $scope.delete = function() {
            Schema.post({core: $routeParams.core}, $scope.deleteData, function(data) {
               if (data.errors) {
                   $scope.deleteErrors = data.errors[0].errorMessages;
                   if (typeof $scope.deleteErrors === "string") {
                       $scope.deleteErrors = [$scope.deleteErrors];
                   }
               } else {
                   $scope.deleted = true;
                   $timeout(function() {
                       $location.search("");
                     }, 1500
                   );
               }
            });
        }
        $scope.toggleDeleteCopyField = function(field) {
            field.show = !field.show;
            delete field.errors;
        }
        $scope.deleteCopyField = function(field, source, dest) {
            data = {'delete-copy-field': {source: source, dest: dest}};
            Schema.post({core: $routeParams.core}, data, function(data) {
               if (data.errors) {
                   field.errors = data.errors[0].errorMessages;
                   if (typeof $scope.deleteErrors === "string") {
                       field.errors = [field.errors];
                   }
               } else {
                   field.deleted = true;
                   $timeout($scope.refresh, 1500);
               }
            });
        }
    }
);

var getFieldsAndTypes = function(data) {
    var fieldsAndTypes = [];
    var fields = Object.keys(data.fields).sort();
    for (var i in fields) {
        fieldsAndTypes.push({
            group: "Fields",
            value: "field=" + fields[i],
            label: fields[i]
        });
    }
    var dynamic_fields = Object.keys(data.dynamic_fields).sort();
    for (var i in dynamic_fields) {
        fieldsAndTypes.push({
            group: "Dynamic Fields",
            value: "dynamic-field=" + dynamic_fields[i],
            label: dynamic_fields[i]
        });
    }
    var types = Object.keys(data.types).sort();
    for (var i in types) {
        fieldsAndTypes.push({
            group: "Types",
            value: "type=" + types[i],
            label: types[i]
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
        unique_key_field: null,
        similarity: null,
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

    data.unique_key_field = schema.uniqueKeyField;
    data.similarity = schema.similarity;

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

var getFieldProperties = function(data, core, is, name) {

    var display = {};

    display.partialState = is.field && !!data.fields[name].partial;

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
    if (is.field && data.fields[name]) {
        if (data.fields[name].flags) {
            addRow('Properties', data.fields[name].flags);
        }
        if (data.fields[name].schema) {
            addRow('Schema', data.fields[name].schema);
        }
        if (data.fields[name].index) {
            addRow('Index', data.fields[name].index);
        }
        display.docs = data.fields[name].docs;
        display.docsUrl = "#/" + core + "/query?q=" + name + ":[* TO *]";
        display.distinct = data.fields[name].distinct;
        display.positionIncrementGap = data.fields[name].positionIncrementGap;
        if (data.types[data.fields[name].type]) {
          display.similarity = data.types[data.fields[name].type].similarity;
        } else {
          display.similarity = null;
        }
    } else if (is.dynamicField && data.dynamic_fields[name] && data.dynamic_fields[name].flags) {
        addRow('Properties', data.dynamic_fields[name].flags);
        display.similarity = data.types[data.dynamic_fields[name].type].similarity;
    } else if (is.type && data.types[name]) {
        display.similarity = data.types[name].similarity;
    }
    if (display.similarity && display.similarity.className) {
        display.similarity.className = shortenPackages(display.similarity.className);
    }

    // identify columns in field property table:
    for (var key in data.key) {
        if (allFlags.indexOf(key)>=0) {
            display.columns.push({key: key, name: data.key[key]});
        }
    }

    // identify rows and cell values in field property table:
    for (var i in display.rows) {
        var row = display.rows[i];
        row.cells = [];

        if (!row.flags) {
            continue; // Match the special case in the LukeRequestHandler
        }

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
            for (var componentName in componentTypeData) {
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

var sortedObjectArray = function(list) {
    var objarr = [];
    for (var i in list) {
      objarr.push({"name": list[i]});
    }
    return objarr;
};

var shortenPackages = function(className) {
    return className.replace("org.apache.solr", "o.a.s").replace("org.apache.lucene", "o.a.l");
};
