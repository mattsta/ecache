-module(ecache_reaper).

-behaviour(gen_server).

-export([start/2, start_link/2]).
-export([start_link/1]).
-deprecated({start_link, 1, next_major_release}).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(reaper, {name, size}).
-define(TIMEOUT, 4000).

start_link(Name) -> start_link(Name, 8).

start_link(Name, Size) -> gen_server:start_link(?MODULE, [Name, Size], []).

start(Name, Size) -> gen_server:start(?MODULE, [Name, Size], []).

%%%----------------------------------------------------------------------
%%% Callback functions from gen_server
%%%----------------------------------------------------------------------

shrink_cache_to_size(Name, CurrentSize, Size) ->
    CurrentSize > Size andalso shrink_cache_to_size(Name, gen_server:call(Name, reap_oldest), Size).

init([Name, Size]) -> {ok, #reaper{name = Name, size = Size}, ?TIMEOUT}.

handle_call(Arbitrary, _From, State) -> {reply, {arbitrary, Arbitrary}, State, ?TIMEOUT}.

handle_cast(_Request, State) -> {noreply, State, ?TIMEOUT}.

terminate(_Reason, _State) -> ok.

handle_info(timeout, #reaper{name = Name, size = Size} = State) ->
    shrink_cache_to_size(Name, ecache:total_size(Name), Size),
    {noreply, State, ?TIMEOUT};
handle_info(Info, State) ->
    error_logger:warning_msg("Other info of: ~p~n", [Info]),
    {noreply, State, ?TIMEOUT}.

code_change(_OldVsn, State, _Extra) -> {ok, State}.
