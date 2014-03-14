%% @author Couchbase <info@couchbase.com>
%% @copyright 2014 Couchbase, Inc.
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
%% @doc Web server for menelaus - log collection across the whole cluster.

-module(menelaus_collect_logs).

-include_lib("eunit/include/eunit.hrl").

-include("ns_common.hrl").

%% ====================================================================
%% API functions
%% ====================================================================
-export([handle_collect_logs_start/1,
         handle_collect_logs_cancel/1]).

-ifdef(EUNIT).
%% needed to mock ns_config in tests
-include("ns_config.hrl").

-export([test/0]).
-endif.


%% Handles collectLogs/start POST request (i.e. start log collection).
handle_collect_logs_start(Req) ->
    Config = ns_config:get(),
    JSON = menelaus_util:parse_json(Req),
    ValidateOnly = proplists:get_value("just_validate", Req:parse_qs()) =:= "1",
    ErrorReturnCode = case ValidateOnly of
        true  -> 200;
        false -> 400
    end,
    try parse_validate_collect_logs_start(Config, JSON) of
        {ok, Args} ->
            case ValidateOnly of
                false ->
                    ?log_info("handle_collect_logs_post OK: ~p~n", [Args] ),
                    % TODO: Actually start log collection here.
                    menelaus_util:reply_json(Req, [], 200);
                true ->
                    menelaus_util:reply_json(Req, {struct,
                                                   [{errors, null}]}, 200)
            end
    catch throw:{collect_logs_parse_error, Errors} ->
            menelaus_util:reply_json(Req, {struct, [{errors, Errors}]},
                                     ErrorReturnCode)
    end.

%% Handles collectLogs/cancel POST request (i.e. cancel log collection).
handle_collect_logs_cancel(Req) ->
    menelaus_util:reply_json(Req, {struct, [{list, [ <<"handle_logs_collect">>,
                                                     <<"CANCEL">>]}]}).

%% ====================================================================
%% Internal functions
%% ====================================================================

%% Given a JSON document containing the arguments passed for collect_log_start,
%% parse and validate them.
parse_validate_collect_logs_start(Config, JSON) ->
    case JSON of
        {struct, JPlist} ->
            % Nodes - list of nodes to collect logs from.
            Nodes = parse_validate_nodes(Config, JPlist),

            % Upload, and if so arguments.
            Upload = parse_validate_upload_options(JPlist),

            % If we got this far then all is well
            {ok, [{nodes, Nodes}, {upload, Upload}] };

        _ -> erlang:throw({collect_logs_parse_error,
                           [<<"Invalid JSON - not a struct">>]})
    end.


parse_validate_nodes(Config, JPlist) ->
    {value, ClusterNodes} = ns_config:search(Config, nodes_wanted),
    case proplists:get_value(<<"from">>, JPlist) of
        <<"allnodes">> ->
            ClusterNodes;
        <<"nodes">> ->
            Nodes = proplists:get_value(<<"nodes">>, JPlist),
            % Check that all nodes specified are valid cluster nodes.
            case validate_node_list(Nodes, ClusterNodes) of
                {ok, L} -> L;
                _ -> erlang:throw({collect_logs_parse_error,
                                  {struct, [{<<"nodes">>, <<"Invalid node(s)">>}]}})
            end
    end.

parse_validate_upload_options(JPlist) ->
    case proplists:get_bool(<<"upload">>, JPlist) of
        false ->
            false;

        true ->
            % Upload host - manditory if we are uploading.
            Host = case proplists:get_value(<<"upload_host">>, JPlist) of
                       undefined ->
                           erlang:throw({collect_logs_parse_error,
                                        {struct, [{<<"upload_host">>,
                                                   <<"Argument is manditory if 'Upload' is selected">>}]}});
                       H ->
                           HostStr0 = string:to_lower(binary_to_list(H)),
                           % urlsplit requires '//' to be present to denote the proto
                           HostStr = case string:str(HostStr0, "//") of
                                         0 -> "//" ++ HostStr0;
                                         _ -> HostStr0
                                     end,
                           {Proto, Hostname, _, _, _ } =
                               mochiweb_util:urlsplit(HostStr),

                           % Check FQDN. Pretty naive, just ensure hostname
                           % component has a period in it.
                           case string:chr(Hostname, $. ) of
                               0 -> erlang:throw({collect_logs_parse_error,
                                                 {struct, [{<<"upload_host">>,
                                                            <<"Argument not a FQDN">>}]}});
                               _ -> ok
                           end,

                           % Check for https:// prefix. Protocol can be omitted (and
                           % will default to https) but cannot be something else.
                           case Proto of
                               "https" ->
                                   Proto ++ "://" ++ Hostname;
                               "" ->
                                   "https://" ++ Hostname;
                               _ ->
                                   erlang:throw({collect_logs_parse_error,
                                                {struct, [{<<"upload_host">>,
                                                           <<"'Invalid protocol - must be https://">>}]}})

                           end
                   end,

            % Customer
            Customer = case proplists:get_value(<<"customer">>, JPlist) of
                           undefined ->
                               erlang:throw({collect_logs_parse_error,
                                   {struct, [{<<"customer">>,
                                              <<"Argument missing and 'upload' specified">>}]}});
                           C ->
                               % Spaces are permitted, but should be replaced
                               % %% by underscore before giving to cbcollect
                               C1 = binary:replace(C, <<" ">>, <<"_">>, [global]),
                               case re:run(binary_to_list(C1), "^[A-Za-z0-9_\.\-]{1,50}$", [{capture, none}]) of
                                   match -> C1;
                                   nomatch ->
                                       erlang:throw({collect_logs_parse_error,
                                           {struct, [{<<"customer">>,
                                                      <<"Invalid argument - can only contain the following characters: 'A-Za-z0-9_.-' with a maximum length of 50">>}]}})
                               end
                       end,

            % Ticket (optional, so permit undefined, but is present must be non-null.
            Ticket = case proplists:get_value(<<"ticket">>, JPlist) of
                         undefined -> [];
                         <<"">> ->
                            erlang:throw({collect_logs_parse_error,
                                {struct, [{<<"ticket">>,
                                           <<"Argument must be non-empty if specified">>}]}});
                         T      ->
                             ?log_debug("Ticket:~p~n", [T]),
                             case re:run(binary_to_list(T), "^[0-9]{1,7}$", [{capture, none}]) of
                                 match ->
                                     [{ticket, T}];
                                 nomatch ->
                                     erlang:throw({collect_logs_parse_error,
                                         {struct, [{<<"ticket">>,
                                                    <<"Argument must be numeric and between 1 and 7 digits">>}]}})
                             end
                     end,

            [{host, list_to_binary(Host)}, {customer, Customer}] ++ Ticket
    end.


% Given a list of OTP nodes, return {ok, Nodes} if all nodes exist in the cluster;
% else false.
validate_node_list(Nodes, ClusterNodes) ->
    case is_list(Nodes) of
        true ->
            case Nodes of
                % Must specify at least one node to collect from.
                [] ->
                    erlang:throw({collect_logs_parse_error,
                        {struct, [{<<"nodes">>,
                                   <<"At least one node must be selected">>}]}});
                _ ->
                    try
                        NodesList = [list_to_binary(atom_to_list(N)) || N <- ClusterNodes],
                        NodesSet = sets:from_list(NodesList),
                        ValidNodes = [validate_single_node(NodesSet, N) || N <- Nodes],
                        {ok, ValidNodes}
                    catch throw:collect_logs_parse_error ->
                        false
                    end
            end;
        false ->
            erlang:throw({collect_logs_parse_error,
                {struct, [{<<"nodes">>,
                           <<"Argument must be a list">>}]}})
    end.

validate_single_node(NodesSet, N) ->
    case sets:is_element(N, NodesSet) of
        true ->
            list_to_existing_atom(binary_to_list(N));
        false ->
            erlang:throw(collect_logs_parse_error)
    end.

%%
%% Tests
%%

-ifdef(EUNIT).

test() ->
    eunit:test({module, ?MODULE},
               [verbose]).

handle_collect_logs_post_test() ->
    ?assertEqual(true, true),
    ok.

parse_validate_collect_logs_start_test() ->
    % Mock config with two nodes in it:
    Config = #config{dynamic=[[{nodes_wanted,
                                ['n_0@127.0.0.1', 'n_0@127.0.0.2']}]]},

    % Positive test - allnodes, no upload; all params valid.
    Test1 = {struct, [{<<"from">>, <<"allnodes">>}]},
    ?assertMatch({ok, [{nodes, ['n_0@127.0.0.1', 'n_0@127.0.0.2']}, _]},
                 parse_validate_collect_logs_start(Config, Test1)),

    % Positive test - specific node list(valid), no upload.
    Test2 = {struct, [{<<"from">>, <<"nodes">>},
                      {<<"nodes">>, [ <<"n_0@127.0.0.2">> ]}]},
    ?assertMatch({ok, [{nodes, ['n_0@127.0.0.2']}, _]},
                 parse_validate_collect_logs_start(Config, Test2)),

    % Negative test - specific node list; one invalid.
    Test3 = {struct, [{<<"from">>, <<"nodes">>},
                      {<<"nodes">>, [ <<"n_0@127.0.0.1">>,
                                      <<"n_0@not_a_present_node">> ]}]},
    ?assertThrow({collect_logs_parse_error, _},
                 parse_validate_collect_logs_start(Config, Test3)),

    % Negative test - specific node list; empty.
    Test4 = {struct, [{<<"from">>, <<"nodes">>}, {<<"nodes">>, []}]},
    ?assertThrow({collect_logs_parse_error, _},
                 parse_validate_collect_logs_start(Config, Test4)),

    % Positive test - specific node list; upload, required params present.
    Test5 = {struct, [{<<"from">>, <<"nodes">>},
                      {<<"nodes">>, [ <<"n_0@127.0.0.1">>,
                                      <<"n_0@127.0.0.2">> ]},
                      {<<"upload">>,true},
                      {<<"upload_host">>,
                       <<"HtTpS://customers.couchbase.com">>},
                      {<<"customer">>,<<"random">>},
                      {<<"ticket">>, <<"12345">>}]},
    ?assertMatch({ok, [{nodes, ['n_0@127.0.0.1', 'n_0@127.0.0.2']},
                       {upload, [ {host,
                                   <<"https://customers.couchbase.com">>},
                                  {customer, <<"random">>},
                                  {ticket, <<"12345">>}]}
                      ]},
                 parse_validate_collect_logs_start(Config, Test5)),

    % Negative test - upload; missing upload_host.
    Test6 = {struct, [{<<"from">>, <<"nodes">>},
                      {<<"nodes">>, [ <<"n_0@127.0.0.1">> ]},
                      {<<"upload">>,true},
                      {<<"customer">>,<<"random inc.">>}]},
    ?assertThrow({collect_logs_parse_error, _},
                 parse_validate_collect_logs_start(Config, Test6)),

    % negative test - invalid upload_host (not FQDN).
    Test7 = {struct, [{<<"from">>, <<"nodes">>},
                      {<<"nodes">>, [ <<"n_0@127.0.0.1">> ]},
                      {<<"upload">>,true},
                      {<<"upload_host">>, <<"customers">>},
                      {<<"customer">>,<<"random inc.">>}]},
    ?assertThrow({collect_logs_parse_error, _},
                 parse_validate_collect_logs_start(Config, Test7)),

    % negative test - invalid upload_host (not https:// protocol).
    Test8 = {struct, [{<<"from">>, <<"nodes">>},
                      {<<"nodes">>, [ <<"n_0@127.0.0.1">> ]},
                      {<<"upload">>,true},
                      {<<"upload_host">>,
                       <<"http://customers.couchbase.com">>},
                      {<<"customer">>,<<"random inc.">>}]},
    ?assertThrow({collect_logs_parse_error, _},
                 parse_validate_collect_logs_start(Config, Test8)),

    % negative test - ticket specified but empty.
    Test9 = {struct, [{<<"from">>, <<"allnodes">>},
                      {<<"upload">>,true},
                      {<<"upload_host">>,
                       <<"https://customers.couchbase.com">>},
                      {<<"customer">>,<<"random inc.">>},
                      {<<"ticket">>, <<"">>}]},
    ?assertThrow({collect_logs_parse_error, _},
                 parse_validate_collect_logs_start(Config, Test9)),

    % postive test - ensure spaces in customer are changed to underscore.
    Test10 = {struct, [{<<"from">>, <<"allnodes">>},
        {<<"upload">>,true},
        {<<"upload_host">>,
            <<"HtTpS://customers.couchbase.com">>},
        {<<"customer">>,<<"random company with spaces">>}]},
    ?assertMatch({ok, [{nodes, ['n_0@127.0.0.1', 'n_0@127.0.0.2']},
        {upload, [ {host, <<"https://customers.couchbase.com">>},
                   {customer, <<"random_company_with_spaces">>}]}]},
                 parse_validate_collect_logs_start(Config, Test10)),
    ok.

-endif.
