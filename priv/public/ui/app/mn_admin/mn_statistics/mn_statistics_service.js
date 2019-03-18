(function () {
  "use strict";

  angular
    .module('mnStatisticsNewService', ["mnServersService", "mnUserRolesService", 'mnPoll', "mnStatisticsDescriptionService"])
    .factory('mnStatisticsNewService', mnStatisticsNewServiceFactory);

  function mnStatisticsNewServiceFactory($http, $q, mnServersService, mnPoller, $rootScope, mnStatisticsDescriptionService, mnUserRolesService) {
    var mnStatisticsNewService = {
      prepareNodesList: prepareNodesList,
      export: {
        scenarios: []
      },
      doGetStats: doGetStats,
      getStatSourcePath: getStatSourcePath,
      subscribeToChartStats: subscribeToChartStats,
      unsubscribeChartStats: unsubscribeChartStats,
      addUpdateScenario: addUpdateScenario,
      addUpdateGroup: addUpdateGroup,
      addUpdateChart: addUpdateChart,
      saveScenarios: saveScenarios,
      readByPath: readByPath,
      getStatsV2: getStatsV2,
      getStatsUnits: getStatsUnits,
      getStatsTitle: getStatsTitle,
      tickMultiFormat: d3.time.format.multi([
        ["%-I:%M%p", function (d) {return d.getMinutes(); }], // not the beginning of the hour
        ["%-I%p", function (d) { return d.getHours(); }], // not midnight
        ["%b %-d", function (d) { return d.getDate() != 1; }], // not the first of the month
        ["%b %-d", function (d) { return d.getMonth(); }], // not Jan 1st
        ["%Y", function () { return true; }]
      ])
    };

    var pollers = {};
    var chartScopes = {};
    var rootScopes = {};

    return mnStatisticsNewService;

    function getStatsTitle(stats) {
      return _.map(_.values(stats), function (descPath) {
        var desc = mnStatisticsNewService
            .readByPath(mnStatisticsDescriptionService.stats, descPath);
        return desc.title;
      }).join(", ");
    }

    function getStatsUnits(stats) {
      var units = {};
      angular.forEach(stats, function (descPath, name) {
        if (!descPath) {
          return;
        }
        var desc = mnStatisticsNewService
            .readByPath(mnStatisticsDescriptionService.stats, descPath);
        units[desc.unit] = true;
      });
      return units;
    }

    function getStatsV2(config, zoom, bucket) {
      var requests = [];
      var data = {
        startTS: 0 - Number(zoom),
        bucket: bucket,
        step: 1
      };
      if (config.specificStat) {
        angular.forEach(config.stats, function (descPath, statName) {
          data.statName = statName;
        });
        requests.push(
          $http({type: "GET",
                 url: "/_uistats/v2",
                 params: Object.assign({}, data)
                }));
      } else {
        angular.forEach(config.stats, function (descPath, statName) {
          requests.push(
            $http({type: "GET",
                   url: "/_uistats/v2",
                   params: Object.assign({statName: statName, host: "aggregate"}, data)
                  }));
        });
      }

      return $q.all(requests);
    }

    function readByPath(obj, path) {
      var paths = path.split('.');
      var current = obj;
      var i;

      for (i = 0; i < paths.length; ++i) {
        if (current[paths[i]] == undefined) {
          return undefined;
        } else {
          current = current[paths[i]];
        }
      }
      return current;
    }

    function unsubscribeChartStats(config, scopeToRemove, bucket) {
      var statID = getStatSourcePath(config, bucket);
      _.remove(chartScopes[statID], function (scope) {
        return scope === scopeToRemove;
      });

      if (!chartScopes[statID].length) {
        rootScopes[statID].$destroy();
        rootScopes[statID] = null;
        pollers[statID] = null;
      }
    }

    function addUpdateChart(newChart, group) {
      var charts = group.charts;

      var statName;
      if (newChart.specificStat) {
        _.forEach(newChart.stats, function (value, key) {
          statName = key;
        });
      }

      if (newChart.id) {
        var index = _.findIndex(charts, {'id': newChart.id});
        charts[index] = newChart;
      } else {
        var chartId = new Date().getTime();
        charts = charts || [];
        newChart.id = chartId;
        charts.push(newChart);
      }

      return saveScenarios();
    }

    function saveScenarios() {
      return mnUserRolesService.getUserProfile().then(function (profile) {
        profile.scenarios = mnStatisticsNewService.export.scenarios;
        return mnUserRolesService.putUserProfile(profile);
      })
    }

    function addUpdateGroup(newGroup) {
      newGroup.id = new Date().getTime();
      mnStatisticsNewService.export.scenarios.selected.groups.push(newGroup);
      return saveScenarios();
    }

    function addUpdateScenario(newSenario, doDelete) {
      var scenarios = mnStatisticsNewService.export.scenarios;

      if (newSenario.id) {
        var index = _.findIndex(scenarios, {'id': newSenario.id});

        if (doDelete) {
          scenarios.splice(index, 1);
        } else {
          scenarios[index].name = newSenario.name;
          scenarios[index].desc = newSenario.desc;
        }
      } else {
        newSenario.id = new Date().getTime();
        scenarios.push(newSenario);
      }

      return saveScenarios();
    }

    function getStatSourcePath(chart, bucket) {
      var string = bucket;

      if (chart.specificStat) {
        angular.forEach(chart.stats, function (descPath, statName) {
          string += statName;
        });
      } else {
        string += chart.node;
      }

      return string;
    }

    function subscribeToChartStats(config, chartScope, bucket) {
      var config1 = _.clone(config, true);
      var statID = getStatSourcePath(config1, bucket);


      rootScopes[statID] = rootScopes[statID] || $rootScope.$new();
      chartScopes[statID] = chartScopes[statID] || [];
      chartScopes[statID].push(chartScope);

      if (!pollers[statID]) {
        pollers[statID] =
          new mnPoller(rootScopes[statID], function (previousResult) {
            return mnStatisticsNewService.doGetStats(config1, previousResult);
          })
          .setInterval(function (response) {
            return response.data.interval;
          })
          .subscribe(function (value) {
            chartScopes[statID].forEach(function (scope) {
              scope["mnChartStats"] = value;
            });
          })
          .reloadOnScopeEvent("reloadChartPoller")
          .cycle();
      } else {
        pollers[statID].reload();
      }
    }

    function prepareNodesList(params) {
      return mnServersService.getNodes().then(function (nodes) {
        var rv = {};
        rv.nodesNames = _(nodes.active).filter(function (node) {
          return !(node.clusterMembership === 'inactiveFailed') && !(node.status === 'unhealthy');
        }).pluck("hostname").value();

        rv.nodesNames.selected = params.statsHostname || rv.nodesNames[0];
        return rv;
      });
    }

    function doGetStats(chartConfig, previousResult) {
      var reqParams = {
        zoom: mnStatisticsNewService.export.scenarios.selected.zoom,
        bucket: chartConfig.bucket
      };
      if (chartConfig.specificStat) {
        angular.forEach(chartConfig.stats, function (descPath, statName) {
          reqParams.statName = statName;
        });
      } else {
        if (chartConfig.node !== "all") {
          reqParams.node = chartConfig.node;
        }
      }
      if (previousResult && !previousResult.status) {
        reqParams.haveTStamp = previousResult.stats.lastTStamp;
      }
      return $http({
        url: '/_uistats',
        method: 'GET',
        params: reqParams
      }).then(function (resp) {
        if (previousResult && !previousResult.status) {
          resp.data = maybeApplyDelta(previousResult, resp.data);
        }
        // stats.serverDate = mnParseHttpDateFilter(data[0].headers('date')).valueOf();
        // stats.clientDate = (new Date()).valueOf();
        var samples = {};
        angular.forEach(resp.data.stats, function (subSamples, subName) {
          var timestamps = subSamples.timestamp;
          for (var k in subSamples) {
            if (k == "timestamp") {
              continue;
            }
            samples[k] = subSamples[k];
            samples[k].timestamps = timestamps;
          }
        });
        resp.data.samples = samples;
        return resp;
      });
    }

    function maybeApplyDelta(prevValue, value) {
      var stats = value.stats;
      var prevStats = prevValue.stats || {};
      for (var kind in stats) {
        var newSamples = restoreOpsBlock(prevStats[kind],
                                         stats[kind],
                                         value.samplesCount + 1);
        stats[kind] = newSamples;
      }
      return value;
    }

    function restoreOpsBlock(prevSamples, samples, keepCount) {
      var prevTS = prevSamples.timestamp;
      if (samples.timestamp && samples.timestamp.length == 0) {
        // server was unable to return any data for this "kind" of
        // stats
        if (prevSamples && prevSamples.timestamp && prevSamples.timestamp.length > 0) {
          return prevSamples;
        }
        return samples;
      }
      if (prevTS == undefined ||
          prevTS.length == 0 ||
          prevTS[prevTS.length-1] != samples.timestamp[0]) {
        return samples;
      }
      var newSamples = {};
      for (var keyName in samples) {
        var ps = prevSamples[keyName];
        if (!ps) {
          ps = [];
          ps.length = keepCount;
        }
        newSamples[keyName] = ps.concat(samples[keyName].slice(1)).slice(-keepCount);
      }
      return newSamples;
    }

  }
})();