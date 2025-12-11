-module(ar_http_iface_alarms).

-export([
         alarm_reached_connections_threshold/2,
         cb_reached_connection_threshold_alarm/4
]).

-include_lib("arweave/include/ar.hrl").

alarm_reached_connections_threshold(Ratio, MaxConnections) ->
    %% Ranch threshold needs to be an integer
    Threshold = floor(Ratio * MaxConnections), 
    #{type => num_connections, 
      threshold => Threshold,
      callback => fun cb_reached_connection_threshold_alarm/4}.

%%{?MODULE, cb_reached_connection_threshold_alarm}}.

cb_reached_connection_threshold_alarm(Ref, AlarmName, CallerPid, ActiveConns) ->
    io:format("called"),
    ?LOG_WARNING([
                  {event, reached_connections_threshold_alarm},
                  {ref, Ref},
                  {alarm_name, AlarmName},
                  {module, ?MODULE},
                  {caller_pid, CallerPid},
                  {active_connections, ActiveConns}
 ]).

    
