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
-module(encryption_service).

-behaviour(gen_server).

-include("ns_common.hrl").

-export([start_link/0]).
-export([init/1, handle_call/3, handle_cast/2,
         handle_info/2, terminate/2, code_change/3]).

-export([set_password/1,
         set_initial_cookie/0,
         get_encrypted_data_key/0,
         set_encrypted_data_key/1,
         decrypt/1,
         encrypt/1]).

-record(state, {
          gosecrets :: pid(),
          password :: string()
         }).

data_key_store_path() ->
    filename:join(path_config:component_path(data, "config"), "encrypted_data_keys").

get_encrypted_data_key() ->
    gen_server:call({?MODULE, ns_server:get_babysitter_node()}, get_encrypted_data_key, infinity).

babysitter_init_path() ->
    filename:join(path_config:component_path(data), "babysitter_init").

set_password(Password) ->
    gen_server:call(?MODULE, {set_password, Password}, infinity).


set_encrypted_data_key(DataKey) ->
    ok = misc:atomic_write_file(data_key_store_path(), DataKey),
    {?MODULE, ns_server:get_babysitter_node()} ! update_encrypted_data_key.

encrypt(Data) ->
    gen_server:call({?MODULE, ns_server:get_babysitter_node()}, {encrypt, Data}, infinity).

decrypt(Data) ->
    gen_server:call({?MODULE, ns_server:get_babysitter_node()}, {decrypt, Data}, infinity).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

generate_cookie() ->
    {A1, A2, A3} = erlang:now(),
    random:seed(A1, A2, A3),
    list_to_atom(misc:rand_str(16)).

set_initial_cookie() ->
    Cookie = generate_cookie(),
    erlang:set_cookie(node(), Cookie),
    case os:getenv("CB_WAIT_FOR_MASTER_PASSWORD") of
        "true" ->
            ns_config_default:ensure_data_dir(),
            Content = misc:node_name_short() ++ ":" ++ atom_to_list(Cookie),
            ok = misc:atomic_write_file(babysitter_init_path(),
                                        list_to_binary(Content));
        _ ->
            ok
    end.

reset_cookie() ->
    Cookie = generate_cookie(),
    erlang:set_cookie(node(), Cookie).

prompt_the_password(EncryptedDataKey, State) ->
    ?log_debug("Waiting for the master password to be supplied"),
    {Resp, ReplyTo} =
        receive
            {'$gen_call', From, {set_password, P}} ->
                ok = set_password(P, State),
                case EncryptedDataKey of
                    undefined ->
                        {ok, From};
                    _ ->
                        Ret = call_gosecrets({set_data_key, EncryptedDataKey}, State),
                        case Ret of
                            ok ->
                                {ok, From};
                            Error ->
                                ?log_error("Incorrect master password. Error: ~p", [Error]),
                                {auth_failure, From}
                        end
                end
        end,
    gen_server:reply(ReplyTo, Resp),
    Resp.

set_password(Password, State) ->
    ?log_debug("Sending password to gosecrets"),
    call_gosecrets({set_password, Password}, State).

init([]) ->
    Path = data_key_store_path(),
    EncryptedDataKey =
        case file:read_file(Path) of
            {ok, DataKey} ->
                ?log_debug("Encrypted data key retrieved from ~p", [Path]),
                DataKey;
            {error, enoent} ->
                ?log_debug("Encrypted data key is not found in ~p", [Path]),
                undefined
        end,
    State = start_gosecrets(#state{}),
    case os:getenv("CB_WAIT_FOR_MASTER_PASSWORD") of
        "true" ->
            case prompt_the_password(EncryptedDataKey, State) of
                ok ->
                    reset_cookie(),
                    ok;
                auth_failure ->
                    exit(incorrect_master_password)
            end;
        _ ->
            Password =
                case os:getenv("CB_MASTER_PASSWORD") of
                    false ->
                        "";
                    S ->
                        S
                end,
            ok = set_password(Password, State)
    end,
    EncryptedDataKey1 =
        case EncryptedDataKey of
            undefined ->
                ?log_debug("Create new data key."),
                {ok, NewDataKey} = call_gosecrets(create_data_key, State),
                NewDataKey;
            _ ->
                EncryptedDataKey
        end,
    case call_gosecrets({set_data_key, EncryptedDataKey1}, State) of
        ok ->
            ok;
        Error ->
            ?log_error("Incorrect master password. Error: ~p", [Error]),
            exit(incorrect_master_password)
    end,
    {ok, State}.

handle_call(get_encrypted_data_key, _From, State) ->
    {reply, call_gosecrets(get_data_key, State), State};
handle_call({set_password, _}, _From, State) ->
    {reply, {error, not_allowed}, State};
handle_call({encrypt, Data}, _From, State) ->
    {reply, call_gosecrets({encrypt, Data}, State), State};
handle_call({decrypt, Data}, _From, State) ->
    {reply, call_gosecrets({decrypt, Data}, State), State};
handle_call(_, _From, State) ->
    {reply, {error, not_allowed}, State}.

handle_cast(_, State) ->
    {noreply, State}.

handle_info(update_encrypted_data_key, State) ->
    Path = data_key_store_path(),
    {ok, DataKey} = file:read_file(Path),
    ?log_debug("Encrypted data key retrieved from ~p", [Path]),
    ok = call_gosecrets({set_data_key, DataKey}, State),
    {noreply, State};
handle_info(_, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

start_gosecrets(State = #state{gosecrets = undefined}) ->
    Parent = self(),
    Pid = proc_lib:spawn_link(
           fun () ->
                   process_flag(trap_exit, true),
                   Path = path_config:component_path(bin, "gosecrets"),
                   ?log_debug("Starting ~p", [Path]),
                   Port = open_port({spawn_executable, Path}, [{packet, 2}, binary, exit_status, hide]),
                   Parent ! initialized,
                   gosecrets_loop(Port, Parent)
           end),
    ?log_debug("Gosecrets loop spawned with pid = ~p", [Pid]),
    receive
        initialized ->
            ?log_debug("Gosecrets loop initialized."),
            ok
    end,
    State#state{gosecrets = Pid}.

call_gosecrets(Msg, #state{gosecrets = Pid}) ->
    Pid ! {call, Msg},
    receive
        {reply, <<"S">>} ->
            ok;
        {reply, <<"S", Data/binary>>} ->
            {ok, Data};
        {reply, <<"E", Data/binary>>} ->
            {error, binary_to_list(Data)}
    end.

gosecrets_loop(Port, Parent) ->
    receive
        {call, Msg} ->
            Port ! {self(), {command, encode(Msg)}},
            receive
                Exit = {'EXIT', _, _} ->
                    gosecret_process_exit(Port, Exit);
                {Port, {data, Data}} ->
                    Parent ! {reply, Data}
            end,
            gosecrets_loop(Port, Parent);
        Exit = {'EXIT', _, _} ->
            gosecret_process_exit(Port, Exit)
    end.

gosecret_process_exit(Port, Exit) ->
    ?log_debug("Received exit ~p for port ~p", [Exit, Port]),
    gosecret_do_process_exit(Port, Exit).

gosecret_do_process_exit(Port, {'EXIT', Port, Reason}) ->
    exit({port_terminated, Reason});
gosecret_do_process_exit(_Port, {'EXIT', _, Reason}) ->
    exit(Reason).

encode({set_password, Password}) ->
    BinaryPassoword = list_to_binary(Password),
    <<1, BinaryPassoword/binary>>;
encode(create_data_key) ->
    <<2>>;
encode({set_data_key, DataKey}) ->
    <<3, DataKey/binary>>;
encode(get_data_key) ->
    <<4>>;
encode({encrypt, Data}) ->
    <<5, Data/binary>>;
encode({decrypt, Data}) ->
    <<6, Data/binary>>.
