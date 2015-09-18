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

var dataimport_timeout = 2000;

solrAdminApp.controller('DataImportController',
    function($scope, $rootScope, $routeParams, $location, $timeout, $interval, $cookies, Mbeans, DataImport, Constants) {
        $scope.resetMenu("dataimport", Constants.IS_COLLECTION_PAGE);

        $scope.refresh = function () {
            Mbeans.info({core: $routeParams.core, cat: 'QUERYHANDLER'}, function (data) {
                var mbeans = data['solr-mbeans'][1];
                $scope.handlers = [];
                for (var key in mbeans) {
                    if (mbeans[key]['class'] !== key && mbeans[key]['class'] === 'org.apache.solr.handler.dataimport.DataImportHandler') {
                        $scope.handlers.push(key);
                    }
                }
                $scope.hasHandlers = $scope.handlers.length > 0;

                if (!$routeParams.handler) {
                    $location.path("/" + $routeParams.core + "/dataimport/" + $scope.handlers[0]);
                } else {
                    $scope.currentHandler = $routeParams.handler;
                }
            });

            DataImport.config({core: $routeParams.core}, function (data) {
                try {
                    var xml = $.parseXML(data.config);
                } catch (err) {
                    $scope.hasHandlers = false;
                    return;
                }
                $scope.config = data.config;
                $scope.entities = [];
                $('document > entity', xml).each(function (i, element) {
                    $scope.entities.push($(element).attr('name'));
                });
            });

            $scope.lastUpdate = "unknown";
            $scope.lastUpdateUTC = "";

            $scope.refreshStatus();
        };

        $scope.toggleDebug = function () {
            $scope.isDebugMode = !$scope.isDebugMode;
            $scope.showConfiguration = true;
        }

        $scope.toggleConfiguration = function () {
            $scope.showConfiguration = !$scope.showConfiguration;
        }

        $scope.toggleRawStatus = function () {
            $scope.showRawStatus = !$scope.showRawStatus;
        }

        $scope.toggleRawDebug = function () {
            $scope.showRawDebug = !$scope.showRawDebug;
        }

        $scope.reload = function () {
            DataImport.reload({core: $routeParams.core}, function () {
                $scope.reloaded = true;
                $timeout(function () {
                    $scope.reloaded = false;
                }, 5000);
                $scope.refresh();
            });
        }

        $scope.form = {
            command: "full-import",
            verbose: false,
            clean: true,
            commit: true,
            optimize: false,
            showDebug: false,
            custom: "",
            core: $routeParams.core
        };

        $scope.submit = function () {
            var params = {};
            for (var key in $scope.form) {
                params[key] = $scope.form[key];
            }
            if (params.custom.length) {
                var customParams = $scope.form.custom.split("&");
                for (var i in customParams) {
                    var parts = customParams[i].split("=");
                    params[parts[0]] = parts[1];
                }
            }
            delete params.custom;

            if (params.isDebugMode) {
                params.dataConfig = $scope.rawConfig;
            }
            delete params.showDebug;
            params.core = $routeParams.core;

            DataImport.post(params, function (data) {
                $scope.rawResponse = JSON.stringify(data, null, 2);
                $scope.refreshStatus();
            });
        };

        $scope.abort = function () {
            $scope.isAborting = true;
            DataImport.abort({core: $routeParams.core}, function () {
                $timeout(function () {
                    $scope.isAborting = false;
                    $scope.refreshStatus();
                }, 4000);
            });
        }

        $scope.refreshStatus = function () {

            console.log("Refresh Status");

            $scope.isStatusLoading = true;
            DataImport.status({core: $routeParams.core}, function (data) {
                if (data[0] == "<") {
                    $scope.hasHandlers = false;
                    return;
                }

                var now = new Date();
                $scope.lastUpdate = now.toTimeString().split(' ').shift();
                $scope.lastUpdateUTC = now.toUTCString();
                var messages = data.statusMessages;
                var messagesCount = 0;
                for( var key in messages ) { messagesCount++; }

                if (data.status == 'busy') {
                    $scope.status = "indexing";

                    $scope.timeElapsed = data.statusMessages['Time Elapsed'];
                    $scope.elapsedSeconds = parseSeconds($scope.timeElapsed);

                    var info = $scope.timeElapsed ? 'Indexing since ' + $scope.timeElapsed : 'Indexing ...';
                    $scope.info = showInfo(messages, true, info, $scope.elapsedSeconds);

                } else if (messages.RolledBack) {
                    $scope.status = "failure";
                    $scope.info = showInfo(messages, true);
                } else if (messages.Aborted) {
                    $scope.status = "aborted";
                    $scope.info = showInfo(messages, true, 'Aborting current Import ...');
                } else if (data.status == "idle" && messagesCount != 0) {
                    $scope.status = "success";
                    $scope.info = showInfo(messages, true);
                } else {
                    $scope.status = "idle";
                    $scope.info = showInfo(messages, false, 'No information available (idle)');
                }

                delete data.$promise;
                delete data.$resolved;

                $scope.rawStatus = JSON.stringify(data, null, 2);

                $scope.isStatusLoading = false;
                $scope.statusUpdated = true;
                $timeout(function () {
                    $scope.statusUpdated = false;
                }, dataimport_timeout / 2);
            });
        };

        $scope.updateAutoRefresh = function () {
            $scope.autorefresh = !$scope.autorefresh;
            $cookies.dataimport_autorefresh = $scope.autorefresh ? true : null;
            if ($scope.autorefresh) {
                $scope.refreshTimeout = $interval($scope.refreshStatus, dataimport_timeout);
                var onRouteChangeOff = $scope.$on('$routeChangeStart', function() {
                    $interval.cancel($scope.refreshTimeout);
                    onRouteChangeOff();
                });

            } else if ($scope.refreshTimeout) {
                $interval.cancel($scope.refreshTimeout);
            }
            $scope.refreshStatus();
        };

        $scope.refresh();

});

var showInfo = function (messages, showFull, info_text, elapsed_seconds) {

    var info = {};
    if (info_text) {
        info.text = info_text;
    } else {
        info.text = messages[''] || '';
        // format numbers included in status nicely
        /* @todo this pretty printing is hard to work out how to do in an Angularesque way:
        info.text = info.text.replace(/\d{4,}/g,
            function (match, position, string) {
                return app.format_number(parseInt(match, 10));
            }
        );
        */

        var time_taken_text = messages['Time taken'];
        info.timeTaken = parseSeconds(time_taken_text);
    }
    info.showDetails = false;

    if (showFull) {
        if (!elapsed_seconds) {
            var time_taken_text = messages['Time taken'];
            elapsed_seconds = parseSeconds(time_taken_text);
        }

        info.showDetails = true;

        var document_config = {
            'Requests': 'Total Requests made to DataSource',
            'Fetched': 'Total Rows Fetched',
            'Skipped': 'Total Documents Skipped',
            'Processed': 'Total Documents Processed'
        };

        info.docs = [];
        for (var key in document_config) {
            var value = parseInt(messages[document_config[key]], 10);
            var doc = {desc: document_config[key], name: key, value: value};
            if (elapsed_seconds && key != 'Skipped') {
                doc.speed = Math.round(value / elapsed_seconds);
            }
            info.docs.push(doc);
        }

        var dates_config = {
            'Started': 'Full Dump Started',
            'Aborted': 'Aborted',
            'Rolledback': 'Rolledback'
        };

        info.dates = [];
        for (var key in dates_config) {
            var value = messages[dates_config[key]];
            if (value) {
                value = value.replace(" ", "T")+".000Z";
                console.log(value);
                var date = {desc: dates_config[key], name: key, value: value};
                info.dates.push(date);
            }
        }
    }
    return info;
}

var parseSeconds = function(time) {
    var seconds = 0;
    var arr = new String(time || '').split('.');
    var parts = arr[0].split(':').reverse();

    for (var i = 0; i < parts.length; i++) {
        seconds += ( parseInt(parts[i], 10) || 0 ) * Math.pow(60, i);
    }

    if (arr[1] && 5 <= parseInt(arr[1][0], 10)) {
        seconds++; // treat more or equal than .5 as additional second
    }
    return seconds;
}
