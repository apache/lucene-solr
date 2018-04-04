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

solrAdminApp.controller('ReplicationController',
    function($scope, $rootScope, $routeParams, $interval, $timeout, Replication, Constants) {
        $scope.resetMenu("replication", Constants.IS_CORE_PAGE);

        $scope.iterationCount = 1;

        $scope.refresh = function() {
            Replication.details({core:$routeParams.core}, function(response) {
                var timeout;
                var interval;
                if ($scope.interval) $interval.cancel($scope.interval);
                $scope.isSlave = (response.details.isSlave === 'true');
                if ($scope.isSlave) {
                    $scope.progress = getProgressDetails(response.details.slave);
                    $scope.iterations = getIterations(response.details.slave);
                    $scope.versions = getSlaveVersions(response.details);
                    $scope.settings = getSlaveSettings(response.details);
                    if ($scope.settings.isReplicating) {
                        timeout = $timeout($scope.refresh, 1000);
                    } else if(!$scope.settings.isPollingDisabled && $scope.settings.pollInterval) {
                        interval = $scope.interval = $interval(function() {
                            $scope.settings.tick--;
                        }, 1000, $scope.settings.tick);
                        timeout = $timeout($scope.refresh, 1000*(1+$scope.settings.tick));
                    }
                } else {
                    $scope.versions = getMasterVersions(response.details);
                }
                $scope.master = getMasterSettings(response.details, $scope.isSlave);

                var onRouteChangeOff = $scope.$on('$routeChangeStart', function() {
                    if (interval) $interval.cancel(interval);
                    if (timeout) $timeout.cancel(timeout);
                    onRouteChangeOff();
                });
            });

        };

        $scope.execute = function(command) {
            Replication.command({core:$routeParams.core, command:command}, function(data){$scope.refresh()});
        }

        $scope.showIterations = function() { $scope.iterationCount = 100000}; // limitTo should accept undefined, but doesn't work.
        $scope.hideIterations = function() { $scope.iterationCount = 1};

        $scope.refresh();
    });

var getProgressDetails = function(progress) {

    progress.timeRemaining = parseSeconds(progress.timeRemaining);
    progress.totalPercent = parseInt(progress.totalPercent);
    if (progress.totalPercent === 0) {
        progress.totalPercentWidth = "1px";
    } else {
        progress.totalPercentWidth = progress.totalPercent + "%";
    }
    progress.currentFileSizePercent = parseInt(progress.currentFileSizePercent);

    if (!progress.indexReplicatedAtList) {
        progress.indexReplicatedAtList = [];
    }

    if (!progress.replicationFailedAtList) {
        progress.replicationFailedAtList = [];
    }
    return progress;
};

var getIterations = function(slave) {

    var iterations = [];

    var find = function(list, date) {
        return list.filter(function(e) {return e.date == date});
    };

    for (var i in slave.indexReplicatedAtList) {
        var date = slave.indexReplicatedAtList[i];
        var iteration = {date:date, status:"replicated", latest: false};
        if (date == slave.indexReplicatedAt) {
            iteration.latest = true;
        }
        iterations.push(iteration);
    }

    for (var i in slave.replicationFailedAtList) {
        var failedDate = slave.replicationFailedAtList[i];
        var matchingIterations = find(iterations, failedDate);
        if (matchingIterations[0]) {
            iteration = matchingIterations[0];
            iteration.status = "failed";
        } else {
            iteration = {date: failedDate, status:"failed", latest:false};
            iterations.push(iteration);
        }
        if (failedDate == slave.replicationFailedAt) {
            iteration.latest = true;
        }
    }
    iterations.sort(function(a,b){ return a.date> b.date;}).reverse();
    return iterations;
};

var getMasterVersions = function(data) {
    versions = {masterSearch:{}, master:{}};

    versions.masterSearch.version = data.indexVersion;
    versions.masterSearch.generation = data.generation;
    versions.masterSearch.size = data.indexSize;

    versions.master.version = data.master.replicableVersion || '-';
    versions.master.generation = data.master.replicableGeneration || '-';
    versions.master.size = '-';

    return versions;
};

var getSlaveVersions = function(data) {
    versions = {masterSearch: {}, master: {}, slave: {}};

    versions.slave.version = data.indexVersion;
    versions.slave.generation = data.generation;
    versions.slave.size = data.indexSize;

    versions.master.version = data.slave.masterDetails.replicableVersion || '-';
    versions.master.generation = data.slave.masterDetails.replicableGeneration || '-';
    versions.master.size = '-';

    versions.masterSearch.version = data.slave.masterDetails.indexVersion;
    versions.masterSearch.generation = data.slave.masterDetails.generation;
    versions.masterSearch.size = data.slave.masterDetails.indexSize;

    versions.changedVersion = data.indexVersion !== data.slave.masterDetails.indexVersion;
    versions.changedGeneration = data.generation !== data.slave.masterDetails.generation;

    return versions;
};

var parseDateToEpoch = function(date) {
    // ["Sat Mar 03 11:00:00 CET 2012", "Sat", "Mar", "03", "11:00:00", "CET", "2012"]
    var parts = date.match( /^(\w+)\s+(\w+)\s+(\d+)\s+(\d+\:\d+\:\d+)\s+(\w+)\s+(\d+)$/ );

    // "Sat Mar 03 2012 10:37:33"
    var d = new Date( parts[1] + ' ' + parts[2] + ' ' + parts[3] + ' ' + parts[6] + ' ' + parts[4] );
    return d.getTime();
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

var getSlaveSettings = function(data) {
    var settings = {};
    settings.masterUrl = data.slave.masterUrl;
    settings.isPollingDisabled = data.slave.isPollingDisabled == 'true';
    settings.pollInterval = data.slave.pollInterval;
    settings.isReplicating = data.slave.isReplicating == 'true';
    settings.nextExecutionAt = data.slave.nextExecutionAt;

    if(settings.isReplicating) {
        settings.isApprox = true;
        settings.tick = parseSeconds(settings.pollInterval);
    } else if (!settings.isPollingDisabled && settings.pollInterval) {
        if( settings.nextExecutionAt ) {
            settings.nextExecutionAtEpoch = parseDateToEpoch(settings.nextExecutionAt);
            settings.currentTime = parseDateToEpoch(data.slave.currentDate);

            if( settings.nextExecutionAtEpoch > settings.currentTime) {
                settings.isApprox = false;
                settings.tick = ( settings.nextExecutionAtEpoch - settings.currentTime) / 1000;
            }
        }
    }
    return settings;
};

var getMasterSettings = function(details, isSlave) {
    var master = {};
    var masterData = isSlave ? details.slave.masterDetails.master : details.master;
    master.replicationEnabled = masterData.replicationEnabled == "true";
    master.replicateAfter = masterData.replicateAfter.join(", ");

    if (masterData.confFiles) {
        master.files = [];
        var confFiles = masterData.confFiles.split(',');
        for (var i=0; i<confFiles.length; i++) {
            var file = confFiles[i];
            var short = file;
            var title = file;
            if (file.indexOf(":")>=0) {
                title = file.replace(':', ' » ');
                var parts = file.split(':');
                if (isSlave) {
                    short = parts[1];
                } else {
                    short = parts[0];
                }
            }
            master.files.push({title:title, name:short});
        }
    }
    return master;
}
