%% @author Couchbase <info@couchbase.com>
%% @copyright 2018 Couchbase, Inc.
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
%% @doc handlers for audit related REST API's

-module(menelaus_web_audit).

-include("cut.hrl").
-include("ns_common.hrl").

-export([handle_get/1,
         handle_post/1]).

-import(menelaus_util,
        [reply_json/2,
         reply/2,
         validate_has_params/1,
         validate_boolean/2,
         validate_dir/2,
         validate_integer/2,
         validate_range/5,
         validate_range/4,
         validate_by_fun/3,
         validate_any_value/2,
         validate_unsupported_params/1,
         execute_if_validated/4]).

handle_get(Req) ->
    menelaus_util:assert_is_enterprise(),
    menelaus_util:assert_is_40(),

    Props = pre_process_get(ns_audit_cfg:get_global()),

    Json =
        lists:filtermap(fun ({K, V}) ->
                                case key_config_to_api(K) of
                                    undefined ->
                                        false;
                                    ApiK ->
                                        {true,
                                         {ApiK, (ns_audit_cfg:jsonifier(K))(V)}}
                                end
                        end, Props),

    reply_json(Req, {Json}).

handle_post(Req) ->
    menelaus_util:assert_is_enterprise(),
    menelaus_util:assert_is_40(),

    Args = Req:parse_post(),
    execute_if_validated(
      fun (Values) ->
              Values1 = pre_process_post(Values),
              ?log_debug("BLAH ~p", [Values1]),
              ns_audit_cfg:set_global(
                [{key_api_to_config(ApiK), V} || {ApiK, V} <- Values1]),
              reply(Req, 200)
      end, Req, Args, validators()).

key_api_to_config(auditdEnabled) ->
    auditd_enabled;
key_api_to_config(rotateInterval) ->
    rotate_interval;
key_api_to_config(rotateSize) ->
    rotate_size;
key_api_to_config(logPath) ->
    log_path;
key_api_to_config(disabledUsers) ->
    disabled_users;
key_api_to_config(X) ->
    X.

key_config_to_api(auditd_enabled) ->
    auditdEnabled;
key_config_to_api(rotate_interval) ->
    rotateInterval;
key_config_to_api(rotate_size) ->
    rotateSize;
key_config_to_api(log_path) ->
    logPath;
key_config_to_api(X) ->
    case cluster_compat_mode:is_cluster_vulcan() of
        true ->
            key_config_to_api_vulcan(X);
        false ->
            undefined
    end.

key_config_to_api_vulcan(actually_disabled) ->
    disabled;
key_config_to_api_vulcan(disabled_users) ->
    disabledUsers;
key_config_to_api_vulcan(uuid) ->
    uuid;
key_config_to_api_vulcan(_) ->
    undefined.

pre_process_get(Props) ->
    case cluster_compat_mode:is_cluster_vulcan() of
        true ->
            Enabled = proplists:get_value(enabled, Props),
            Disabled = proplists:get_value(disabled, Props),
            Descriptors = ns_audit_cfg:get_descriptors(ns_config:latest()),

            %% though POST API stores all configurable events as either enabled
            %% or disabled, we anticipate that the list of configurable events
            %% might change
            ActuallyDisabled =
                calculate_disabled(Enabled, Disabled, Descriptors),

            [{actually_disabled, ActuallyDisabled} | Props];
        false ->
            Props
    end.

pre_process_post(Props) ->
    case cluster_compat_mode:is_cluster_vulcan() of
        true ->
            Disabled = proplists:get_value(disabled, Props),
            Descriptors = ns_audit_cfg:get_descriptors(ns_config:latest()),

            %% all configurable events are stored either in enabled or
            %% disabled list, to reduce an element of surprise in case
            %% if the defaults will change after the upgrade
            ActuallyDisabled = calculate_disabled([], Disabled, Descriptors),
            ActuallyEnabled =
                [Id || {Id, _} <- Descriptors] -- ActuallyDisabled,

            misc:update_proplist(Props,
                                 [{enabled, ActuallyEnabled},
                                  {disabled, ActuallyDisabled}]);
        false ->
            Props
    end.

calculate_disabled(Enabled, Disabled, Descriptors) ->
    lists:filtermap(
      fun ({Id, P}) ->
              IsEnabledByDefault = proplists:get_value(enabled, P),
              case lists:member(Id, Enabled) orelse
                  (IsEnabledByDefault andalso
                   not lists:member(Id, Disabled)) of
                  true ->
                      false;
                  false ->
                      {true, Id}
              end
      end, Descriptors).

validate_events(Name, Descriptors, State) ->
    validate_by_fun(
      fun (Value) ->
              Events = string:tokens(Value, ","),
              IntEvents = [(catch erlang:list_to_integer(E)) || E <- Events],
              case lists:all(fun is_integer/1, IntEvents) of
                  true ->
                      case lists:filter(orddict:is_key(_, Descriptors),
                                        IntEvents) of
                          IntEvents ->
                              {value, IntEvents};
                          Other ->
                              {error,
                               io_lib:format(
                                 "Following events are either unknown or not "
                                 "modifiable ~p", [IntEvents -- Other])}
                      end;
                  false ->
                      {error, "All event id's must be integers"}
              end
      end, Name, State).

validate_users(Name, State) ->
    validate_by_fun(
      fun (Value) ->
              Users = string:tokens(Value, ","),
              UsersParsed = [{U, string:tokens(Value, "/")} || U <- Users],
              UsersFound =
                  lists:map(
                    fun ({U, [N, S]}) ->
                            Identity = {N, menelaus_web_rbac:domain_to_atom(S)},
                            case menelaus_users:user_exists(Identity) of
                                true ->
                                    Identity;
                                false ->
                                    {error, U}
                            end;
                        ({U, _}) ->
                            {error, U}
                    end, lists:zip(Users, UsersParsed)),
              case [E || {error, E} <- UsersFound] of
                  [] ->
                      {value, UsersFound};
                  BadUsers ->
                      {error,
                       io_lib:format("Unrecognized users ~p", [BadUsers])}
              end
      end, Name, State).

validator_vulcan(State) ->
    case cluster_compat_mode:is_cluster_vulcan() of
        false ->
            State;
        true ->
            functools:chain(State, validators_vulcan())
    end.

validators_vulcan() ->
    Descriptors = orddict:from_list((ns_audit_cfg:get_descriptors(
                                       ns_config:latest()))),
    [validate_any_value(disabled, _),
     validate_events(disabled, Descriptors, _),
     validate_any_value(disabledUsers, _),
     validate_users(disabledUsers, _)].

validators() ->
    [validate_has_params(_),
     validate_boolean(auditdEnabled, _),
     validate_any_value(logPath, _),
     validate_dir(logPath, _),
     validate_integer(rotateInterval, _),
     validate_range(
       rotateInterval, 15*60, 60*60*24*7,
       fun (Name, _Min, _Max) ->
               io_lib:format("The value of ~p must be in range from 15 minutes "
                             "to 7 days", [Name])
       end, _),
     validate_by_fun(
       fun (Value) ->
               case Value rem 60 of
                   0 ->
                       ok;
                   _ ->
                       {error, "Value must not be a fraction of minute"}
               end
       end, rotateInterval, _),
     validate_integer(rotateSize, _),
     validate_range(rotateSize, 0, 500*1024*1024, _),
     validator_vulcan(_),
     validate_unsupported_params(_)].
