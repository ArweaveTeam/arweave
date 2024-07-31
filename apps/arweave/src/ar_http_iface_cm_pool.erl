-module(ar_http_iface_cm_pool).

-export([handle_post_solution/2, handle_get_jobs/3]).

-include_lib("arweave/include/ar_config.hrl").

%%%===================================================================
%%% Public interface.
%%%===================================================================

handle_post_solution(Req, Pid) ->
    case ar_node:is_joined() of
		false ->
			not_joined(Req);
		true ->
            APISecret = case {ar_pool:is_server(), ar_coordination:is_exit_peer()} of
                {true, _} ->
                    pool;
                {_, true} ->
                    cm;
                _ ->
                    error
            end,
            case validate_request(APISecret, Req) of
                true ->
                    handle_post_solution2(Req, Pid);
                FailureResponse ->
                    FailureResponse
            end
    end.

handle_get_jobs(EncodedPrevOutput, Req, Pid) ->
    case ar_node:is_joined() of
		false ->
			not_joined(Req);
		true ->
			case ar_util:safe_decode(EncodedPrevOutput) of
				{ok, PrevOutput} ->
					handle_get_jobs2(PrevOutput, Req);
				{error, invalid} ->
					{400, #{}, jiffy:encode(#{ error => invalid_prev_output }), Req}
			end
	end;


%%%===================================================================
%%% Internal functions.
%%%===================================================================

handle_post_solution2(Req, Pid) ->
    Peer = ar_http_util:arweave_peer(Req),
	case read_complete_body(Req, Pid) of
		{ok, Body, Req2} ->
			case catch ar_serialize:json_map_to_solution(
					jiffy:decode(Body, [return_maps])) of
				{'EXIT', _} ->
					{400, #{}, jiffy:encode(#{ error => invalid_json }), Req2};
				Solution ->
					ar_mining_router:received_solution(Solution,
						[{peer, ar_util:format_peer(Peer)}]),
					Response = ar_mining_router:route_solution(Solution),
					JSON = ar_serialize:solution_response_to_json_struct(Response),
					{200, #{}, ar_serialize:jsonify(JSON), Req2}
			end;
		{error, body_size_too_large} ->
			{413, #{}, <<"Payload too large">>, Req};
		{error, timeout} ->
			{500, #{}, <<"Handler timeout">>, Req}
	end.

handle_get_jobs2(PrevOutput, Req) ->
    {ok, Config} = application:get_env(arweave, config),
	CMExitNode = ar_coordination:is_exit_peer() andalso ar_pool:is_client(),
	case {Config#config.is_pool_server, CMExitNode} of
		{false, false} ->
			{501, #{}, jiffy:encode(#{ error => configuration }), Req};
		{true, _} ->
			case check_internal_api_secret(Req) of
				{reject, {Status, Headers, Body}} ->
					{Status, Headers, Body, Req};
				pass ->
					Jobs = ar_pool:generate_jobs(PrevOutput),
					JSON = ar_serialize:jsonify(ar_serialize:jobs_to_json_struct(Jobs)),
					{200, #{}, JSON, Req}
			end;
		{_, true} ->
			case check_cm_api_secret(Req) of
				{reject, {Status, Headers, Body}} ->
					{Status, Headers, Body, Req};
				pass ->
					Jobs = ar_pool:get_cached_jobs(PrevOutput),
					JSON = ar_serialize:jsonify(ar_serialize:jobs_to_json_struct(Jobs)),
					{200, #{}, JSON, Req}
			end
	end.

validate_request(APISecret, Req) ->
    SecretCheck = case APISecret of
        pool ->
            check_internal_api_secret(Req);
        cm ->
            check_cm_api_secret(Req);
        _ ->
            {501, #{}, jiffy:encode(#{ error => configuration }), Req}
    end,
    case SecretCheck of
        pass ->
            true;
        {reject, {Status, Headers, Body}} ->
            {Status, Headers, Body, Req};
        _ ->
            SecretCheck
    end.