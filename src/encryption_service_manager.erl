%% @author Couchbase <info@couchbase.com>
%% @copyright 2016 Couchbase, Inc.
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
%% Responsible for:
%% 1. Updating encryption service with latest encrypted_data_keys
%%
-module(encryption_service_manager).

-behavior(gen_server).

-include("ns_common.hrl").

-export([start_link/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-export([upgrade_config_to_46/1]).

init([]) ->
    Self = self(),
    ns_pubsub:subscribe_link(ns_config_events, fun config_change_detector/2, Self),
    case cluster_compat_mode:is_cluster_46() of
        true ->
            Self ! notify_service;
        false ->
            ok
    end,
    {ok, []}.

upgrade_config_to_46(Config) ->
    encrypt_memcached_passwords(Config) ++
        case ns_config:search(Config, encrypted_data_key) of
            {value, _} ->
                [];
            false ->
                ?log_debug("Store created encrypted data key in config"),
                {ok, DataKey} = encryption_service:get_encrypted_data_key(),
                [{set, encrypted_data_key, DataKey}]
        end.

encrypt_memcached_passwords(Config) ->
    ns_config:fold(
          fun ({node, _Node, memcached} = K, Props, Acc) ->
                  Password = proplists:get_value(admin_pass, Props),
                  EncryptedPassword = encryption_service:encrypt_config_string(Password),
                  NewProps = lists:keyreplace(admin_pass, 1, Props, {admin_pass, EncryptedPassword}),
                  [{set, K, NewProps} | Acc];
              (_, _, Acc) ->
                  Acc
          end, [], Config).

config_change_detector({encrypted_data_key, _}, Parent) ->
    Parent ! notify_service,
    Parent;
config_change_detector(_, Parent) ->
    Parent.

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

handle_call(_, _From, State) ->
    {reply, unknown_call, State}.

handle_cast(_, State) ->
    {noreply, State}.

handle_info(notify_service, State) ->
    misc:flush(notify_service),
    {value, Key} = ns_config:search(encrypted_data_key),
    encryption_service:set_encrypted_data_key(Key),
    {noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
