%% @author Couchbase <info@couchbase.com>
%% @copyright 2015 Couchbase, Inc.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%      http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%
%% @doc this module implements upgrade of erlang XDCR configuration to goxdcr
%%

-module(goxdcr_upgrade).
-include("ns_common.hrl").

-export([upgrade/0]).

upgrade() ->
    0 = run_upgrade().

run_upgrade() ->
    Config = ns_config:get(),
    {Name, Cmd, Args, Opts} = ns_ports_setup:create_goxdcr_upgrade_spec(Config),
    Log = proplists:get_value(log, Opts),
    true = Log =/= undefined,

    Logger = start_logger(Name, Log),

    Opts0 = proplists:delete(log, Opts),
    Opts1 = Opts0 ++ [{args, Args}, {line, 8192}],

    Port = open_port({spawn_executable, Cmd}, Opts1),
    send_json(Port, build_json(), 8191),
    process_upgrade_output(Port, Logger).

send_json(Port, Data, LineLen) ->
    Len = byte_size(Data),
    case Len > LineLen of
        true ->
            ToSend = binary:part(Data, 0, LineLen),
            Port ! {self(), {command, ToSend}},
            Port ! {self(), {command, <<"\n">>}},
            send_json(Port, binary:part(Data, LineLen, Len - LineLen), LineLen);
        false ->
            Port ! {self(), {command, Data}},
            Port ! {self(), {command, <<"\n">>}},
            Port ! {self(), {command, <<"\n">>}}
    end.

process_upgrade_output(Port, Logger) ->
    receive
        {Port, {data, {_, Msg}}} ->
            ale:debug(Logger, [Msg, $\n]),
            process_upgrade_output(Port, Logger);
        {Port, {exit_status, Status}} ->
            Status;
        Msg ->
            ?log_error("Got unexpected message"),
            exit({unexpected_message, Msg})
    end.

start_logger(Name, Log) ->
    Sink = Logger = Name,
    ok = ns_server:start_disk_sink(Sink, Log),
    ale:stop_logger(Logger),
    ok = ale:start_logger(Logger, debug, ale_noop_formatter),
    ok = ale:add_sink(Logger, Sink, debug),
    Logger.

build_json() ->
    RemoteClusters = menelaus_web_remote_clusters:get_remote_clusters(),
    ClustersData = lists:map(fun (KV) ->
                                     menelaus_web_remote_clusters:build_remote_cluster_info(KV, true)
                             end, RemoteClusters),

    RepsData =
        lists:map(fun (Props) ->
                          Id = misc:expect_prop_value(id, Props),
                          {ok, Doc} = xdc_rdoc_api:get_full_replicator_doc(Id),
                          {Props ++ menelaus_web_xdc_replications:build_replication_settings(Doc)}
                  end, xdc_rdoc_api:find_all_replication_docs()),

    GlobalSettings = menelaus_web_xdc_replications:build_global_replication_settings(),

    ejson:encode({[{remoteClusters, ClustersData},
                   {replicationDocs, RepsData},
                   {replicationSettings, {GlobalSettings}}
                  ]}).
