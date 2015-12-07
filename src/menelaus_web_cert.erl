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
%% @doc REST api's for handling ssl certificates

-module(menelaus_web_cert).

-include("ns_common.hrl").

-export([handle_cluster_certificate/1,
         handle_regenerate_certificate/1,
         handle_upload_cluster_ca/1,
         handle_set_node_certificate/1]).

-import(menelaus_util,
        [read_file/2,
         validate_required/2,
         validate_unsupported_params/1,
         execute_if_validated/3,
         reply/2]).

handle_cluster_certificate(Req) ->
    menelaus_web:assert_is_enterprise(),

    case proplists:get_value("extended", Req:parse_qs()) of
        "true" ->
            handle_cluster_certificate_extended(Req);
        _ ->
            handle_cluster_certificate_simple(Req)
    end.

handle_cluster_certificate_simple(Req) ->
    Cert = case ns_server_cert:cluster_ca() of
               {GeneratedCert, _} ->
                   GeneratedCert;
               {UploadedCAProps, _, _} ->
                   proplists:get_value(pem, UploadedCAProps)
           end,
    menelaus_util:reply_ok(Req, "text/plain", Cert).

handle_cluster_certificate_extended(Req) ->
    Res = case ns_server_cert:cluster_ca() of
              {GeneratedCert, _} ->
                  [{type, generated},
                   {pem, GeneratedCert}];
              {UploadedCAProps, _, _} ->
                  [{type, uploaded} | UploadedCAProps]
          end,
    CertJson = lists:map(fun ({K, V}) when is_list(V) ->
                                 {K, list_to_binary(V)};
                             (Pair) ->
                                 Pair
                         end, Res),
    menelaus_util:reply_json(Req, {[{cert, {CertJson}}]}).

handle_regenerate_certificate(Req) ->
    menelaus_web:assert_is_enterprise(),

    ns_server_cert:generate_and_set_cert_and_pkey(),
    ns_ssl_services_setup:sync_local_cert_and_pkey_change(),
    ?log_info("Completed certificate regeneration"),
    ns_audit:regenerate_certificate(Req),
    handle_cluster_certificate_simple(Req).

validation_error_message(empty_cert) ->
    <<"Certificate should not be empty">>;
validation_error_message(not_valid_at_this_time) ->
    <<"Certificate is not valid at this time">>;
validation_error_message(malformed_cert) ->
    <<"Malformed certificate">>;
validation_error_message(non_cert_entries) ->
    <<"Certificate contains malformed entries.">>;
validation_error_message(too_many_entries) ->
    <<"Only one certificate per request is allowed.">>.

reply_error(Req, Error) ->
    menelaus_util:reply_json(Req, {[{error, validation_error_message(Error)}]}, 400).

handle_upload_cluster_ca(Req) ->
    menelaus_web:assert_is_enterprise(),
    menelaus_web:assert_is_watson(),

    case Req:recv_body() of
        undefined ->
            reply_error(Req, empty_cert);
        PemEncodedCA ->
            case ns_server_cert:set_cluster_ca(PemEncodedCA) of
                ok ->
                    handle_cluster_certificate_extended(Req);
                {error, Error} ->
                    reply_error(Req, Error)
            end
    end.


validate_set_node_certificate(Args) ->
    R1 = validate_required(chain, {Args, [], []}),
    R2 = read_file(chain, R1),
    R3 = validate_required(pkey, R2),
    R4 = read_file(pkey, R3),
    validate_unsupported_params(R4).

set_node_certificate_error(no_cluster_ca) ->
    {<<"_">>, <<"Cluster CA needs to be set before setting node certificate.">>};
set_node_certificate_error({bad_cert, {Error, Subject}}) ->
    ErrorMessage = io_lib:format("Certificate validation error: ~p. Certificate: ~p",
                                 [Error, Subject]),
    {chain, iolist_to_binary(ErrorMessage)}.

handle_set_node_certificate(Req) ->
    menelaus_web:assert_is_enterprise(),
    menelaus_web:assert_is_watson(),

    Args = Req:parse_post(),
    execute_if_validated(
      fun (Values) ->
              Chain = proplists:get_value(chain, Values),
              PKey = proplists:get_value(pkey, Values),

              case ns_server_cert:set_node_certificate_chain(Chain, PKey) of
                  ok ->
                      reply(Req, 200);
                  {error, Error} ->
                      menelaus_util:reply_json(
                        Req, {[{errors, {[set_node_certificate_error(Error)]}}]}, 400)
              end
      end, Req, validate_set_node_certificate(Args)).
