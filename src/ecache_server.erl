-module(ecache_server).

-behaviour(gen_server).

-export([start_link/3, start_link/4]).
-export([start_link/5, start_link/6]).
-deprecated([{start_link, 5, next_major_release}, {start_link, 6, next_major_release}]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(cache, {name :: atom(),
                datum_index :: ets:tid(),
                table_pad = 0 :: non_neg_integer(),
                data_module :: module(),
                reaper :: undefined|pid(),
                data_accessor :: atom(),
                size = 8 :: unlimited|non_neg_integer(),
                found = 0 :: non_neg_integer(),
                launched = 0 :: non_neg_integer(),
                policy = mru :: ecache:policy(),
                ttl = timer:minutes(5) :: unlimited|non_neg_integer()}).

-record(datum, {key :: term(),
                mgr, % ???
                data :: term(),
                started :: integer(),
                reaper :: undefined|pid(),
                last_active :: integer(),
                ttl = unlimited :: unlimited|non_neg_integer(),
                type = mru :: ecache:policy(),
                remaining_ttl = unlimited:: unlimited|non_neg_integer()}).

% make 8 MB cache
-spec start_link(Name::atom(), Mod::module(), Fun::atom()) -> {ok, pid()} | {error, term()}.
start_link(Name, Mod, Fun) -> start_link(Name, Mod, Fun, #{}).

% make MRU policy cache
-spec start_link(Name::atom(), Mod::module(), Fun::atom(),
                 Size::unlimited|pos_integer(), Time::unlimited|pos_integer()) ->
          {ok, pid()} | {error, term()}.
start_link(Name, Mod, Fun, Size, Time) -> start_link(Name, Mod, Fun, #{size => Size, time => Time}).

-spec start_link(Name::atom(), Mod::module(), Fun::atom(),
                 Size::unlimited|pos_integer(), Time::unlimited|pos_integer(), Policy::ecache:policy()) ->
          {ok, pid()} | {error, term()}.
start_link(Name, Mod, Fun, Size, Time, Policy) ->
    start_link(Name, Mod, Fun, #{size => Size, time => Time, policy => Policy}).

-spec start_link(Name::atom(), Mod::module(), Fun::atom(), Opts::ecache:options()) -> {ok, pid()} | {error, term()}.
start_link(Name, Mod, Fun, Opts) when is_map(Opts) ->
    #cache{size = DSize, ttl = DTime, policy = DPolicy} = #cache{},
    #{size := Size, time := Time, policy := Policy} = maps:merge(#{size => DSize, time => DTime, policy => DPolicy},
                                                                 Opts),
    gen_server:start_link({local, Name}, ?MODULE, [Name, Mod, Fun, Size, Time, Policy], []);
start_link(Name, Mod, Fun, Opts) when is_list(Opts) -> start_link(Name, Mod, Fun, maps:from_list(Opts));
% make 5 minute expiry cache
start_link(Name, Mod, Fun, Size) when Size =:= unlimited; is_integer(Size), Size > 0 ->
    start_link(Name, Mod, Fun, #{size => Size}).

%%%----------------------------------------------------------------------
%%% Callback functions from gen_server
%%%----------------------------------------------------------------------

-spec init([atom()|module()|unlimited|pos_integer()|ecache:policy()] | #cache{}) -> {ok, #cache{}}.
init([Name, Mod, Fun, Size, Time, Policy]) when is_atom(Mod), is_atom(Fun), is_atom(Policy),
                                                Time =:= unlimited orelse is_integer(Time) andalso Time > 0 ->
    process_flag(trap_exit, true),
    Index = ets:new(Name, [set, compressed, public, % public because we spawn writers
                           {keypos, #datum.key},    % use Key stored in record
                           {read_concurrency, true}]),
    init(#cache{name = Name,
                datum_index = Index,
                table_pad = ets:info(Index, memory),
                data_module = Mod,
                data_accessor = Fun,
                size = Size,
                policy = Policy,
                ttl = Time});
init(#cache{size = unlimited} = State) -> {ok, State};
init(#cache{name = Name, size = Size} = State) when is_integer(Size), Size > 0 ->
    SizeBytes = Size * (1024 * 1024),
    {ok, State#cache{reaper = start_reaper(Name, SizeBytes), size = SizeBytes}}.

-spec handle_call(term(), {pid(), term()}, #cache{}) -> {reply, term(), #cache{}} | {noreply, #cache{}}.
handle_call({get, Key}, From, #cache{data_module = M, data_accessor = F} = State) ->
    generic_get(key(Key), From, State, M, F, Key);
handle_call({generic_get, M, F, Key}, From, State) -> generic_get(key(M, F, Key), From, State, M, F, Key);
handle_call(total_size, _From, #cache{} = State) -> {reply, cache_bytes(State), State};
handle_call(stats, _From,
            #cache{datum_index = Index, found = Found, launched = Launched, policy = Policy, ttl = TTL} = State) ->
    EtsInfo = ets:info(Index),
    {reply,
     [{cache_name, proplists:get_value(name, EtsInfo)},
      {memory_size_bytes, cache_bytes(State, proplists:get_value(memory, EtsInfo))},
      {datum_count, proplists:get_value(size, EtsInfo)},
      {found, Found}, {launched, Launched},
      {policy, Policy}, {ttl, TTL}],
     State};
handle_call(empty, _From, #cache{datum_index = Index} = State) ->
    kill_reapers(Index),
    ets:delete_all_objects(Index),
    {reply, ok, State};
handle_call(reap_oldest, From, #cache{datum_index = Index} = State) ->
    spawn(fun() ->
              DatumNow = #datum{last_active = timestamp()},
              LeastActive = ets:foldl(fun(#datum{last_active = LA} = A, #datum{last_active = Acc}) when LA < Acc -> A;
                                         (_, Acc) -> Acc
                                      end, DatumNow, Index),
              LeastActive =:= DatumNow orelse delete_object(Index, LeastActive),
              gen_server:reply(From, cache_bytes(State))
          end),
    {noreply, State};
handle_call({rand, Type, Count}, From, #cache{datum_index = Index} = State) ->
    spawn(fun() ->
              AllKeys = get_all_keys(Index),
              Length = length(AllKeys),
              gen_server:reply(From,
                               lists:map(if
                                             Type =:= data -> fun(K) ->
                                                                  {ok, Data} = fetch_data(K, Index),
                                                                  Data
                                                              end;
                                             Type =:= keys -> fun unkey/1
                                         end,
                                         if
                                             Length =< Count -> AllKeys;
                                             true -> lists:map(fun(K) -> lists:nth(K, AllKeys) end,
                                                               lists:map(fun(_) -> rand:uniform(Length) end,
                                                                         lists:seq(1, Count)))
                                         end))
          end),
    {noreply, State};
handle_call(Arbitrary, _From, State) -> {reply, {arbitrary, Arbitrary}, State}.

-spec handle_cast(term(), #cache{}) -> {noreply, #cache{}}.
handle_cast(launched, #cache{launched = Launched} = State) -> {noreply, State#cache{launched = Launched + 1}};
handle_cast(found, #cache{found = Found} = State) -> {noreply, State#cache{found = Found + 1}};
handle_cast({dirty, Id, NewData}, #cache{datum_index = Index} = State) ->
    replace_datum(key(Id), NewData, Index),
    {noreply, State};
handle_cast({dirty, Id}, #cache{datum_index = Index} = State) ->
    delete_datum(Index, key(Id)),
    {noreply, State};
handle_cast({generic_dirty, M, F, A}, #cache{datum_index = Index} = State) ->
    delete_datum(Index, key(M, F, A)),
    {noreply, State};
handle_cast(Req, State) ->
    error_logger:warning_msg("Other cast of: ~p~n", [Req]),
    {noreply, State}.

-spec handle_info(term(), #cache{}) -> {noreply, #cache{}}.
handle_info({destroy, _DatumPid, ok}, State) -> {noreply, State};
handle_info({'EXIT', Pid, _Reason}, #cache{reaper = Pid, name = Name, size = Size} = State) ->
    {noreply, State#cache{reaper = start_reaper(Name, Size)}};
handle_info({'EXIT', _Pid, _Reason}, State) -> {noreply, State};
handle_info({'DOWN', _Ref, process, _Pid, _Reason}, State) -> {noreply, State};
handle_info(Info, State) ->
    error_logger:warning_msg("Other info of: ~p~n", [Info]),
    {noreply, State}.

-spec terminate(term(), #cache{}) -> ok.
terminate(_Reason, #cache{reaper = Pid}) when is_pid(Pid) -> gen_server:stop(Pid);
terminate(_Reason, _State) -> ok.

-spec code_change(term(), State, term()) -> {ok, State} when State::#cache{}.
code_change(_OldVsn, State, _Extra) -> {ok, State}.

-compile({inline, [key/1, key/3]}).
-compile({inline, [unkey/1]}).
% keys are tagged/boxed so you can't cross-pollute a cache when using
% memoize mode versus the normal one-key-per-arg mode.
% Implication: *always* add key(Key) to keys from a user.  Don't pass user
% created keys directly to ets.
% The boxing overhead on 64 bit systems is: atom + tuple = 8 + 16 = 24 bytes
% The boxing overhead on 32 bit systems is: atom + tuple = 4 +  8 = 12 bytes
key(M, F, A) -> {ecache_multi, {M, F, A}}.
key(Key)     -> {ecache_plain, Key}.
unkey({ecache_plain, Key}) -> Key;
unkey({ecache_multi, {_, _, _} = MFA}) -> MFA.

%% ===================================================================
%% Private
%% ===================================================================

cache_bytes(#cache{datum_index = Index} = State) -> cache_bytes(State, ets:info(Index, memory)).

cache_bytes(#cache{table_pad = TabPad}, Mem) -> (Mem - TabPad) * erlang:system_info(wordsize).

delete_datum(Index, Key) ->
    case ets:take(Index, Key) of
        [#datum{reaper = Reaper}] -> kill_reaper(Reaper);
        _ -> true
    end.

delete_object(Index, #datum{reaper = Reaper} = Datum) ->
    kill_reaper(Reaper),
    ets:delete_object(Index, Datum).

-compile({inline, [create_datum/4]}).
create_datum(DatumKey, Data, TTL, Type) ->
    Timestamp = timestamp(),
    #datum{key = DatumKey, data = Data, type = Type,
           started = Timestamp, ttl = TTL, remaining_ttl = TTL, last_active = Timestamp}.

reap_after(Index, Key, LifeTTL) ->
    receive
        {update_ttl, NewTTL} -> reap_after(Index, Key, NewTTL)
    after LifeTTL -> ets:delete(Index, Key)
    end.

launch_datum_ttl_reaper(_, _, #datum{remaining_ttl = unlimited} = Datum) -> Datum;
launch_datum_ttl_reaper(Index, Key, #datum{remaining_ttl = TTL} = Datum) ->
    Datum#datum{reaper = spawn(fun() ->
                                   link(ets:info(Index, owner)),
                                   reap_after(Index, Key, TTL)
                               end)}.

-ifdef(OTP_RELEASE).
-if (?OTP_RELEASE >= 21).
-define(EXCEPTION(Class, Reason, Stacktrace), Class:Reason:Stacktrace).
-define(GET_STACK(Stacktrace), Stacktrace).
-endif.
-endif.
-ifndef(EXCEPTION).
-define(EXCEPTION(Class, Reason, _), Class:Reason).
-endif.
-ifndef(GET_STACK).
-define(GET_STACK(_), erlang:get_stacktrace()).
-endif.

launch_datum(Key, Index, Module, Accessor, TTL, Policy, UseKey) ->
    try Module:Accessor(Key) of
        CacheData ->
            ets:insert(Index, launch_datum_ttl_reaper(Index, UseKey, create_datum(UseKey, CacheData, TTL, Policy))),
            {ok, CacheData}
    catch
        ?EXCEPTION(How, What, Stacktrace) -> {ecache_datum_error, {{How, What}, ?GET_STACK(Stacktrace)}}
    end.

-compile({inline, [ping_reaper/2]}).
ping_reaper(Reaper, NewTTL) when is_pid(Reaper) -> Reaper ! {update_ttl, NewTTL};
ping_reaper(_, _) -> ok.

update_ttl(Index, #datum{key = Key, ttl = unlimited}) ->
    ets:update_element(Index, Key, {#datum.last_active, timestamp()});
update_ttl(Index, #datum{key = Key, started = Started, ttl = TTL, type = actual_time, reaper = Reaper}) ->
    Timestamp = timestamp(),
    % Get total time in seconds this datum has been running.  Convert to ms.
    % If we are less than the TTL, update with TTL-used (TTL in ms too) else, we ran out of time.  expire on next loop.
    TTLRemaining = case time_diff(Timestamp, Started) of
                       StartedNowDiff when StartedNowDiff < TTL -> TTL - StartedNowDiff;
                       _ -> 0
                   end,
    ping_reaper(Reaper, TTLRemaining),
    ets:update_element(Index, Key, [{#datum.last_active, Timestamp}, {#datum.remaining_ttl, TTLRemaining}]);
update_ttl(Index, #datum{key = Key, ttl = TTL, reaper = Reaper}) ->
    ping_reaper(Reaper, TTL),
    ets:update_element(Index, Key, {#datum.last_active, timestamp()}).

fetch_data(Key, Index) when is_tuple(Key) ->
    case ets:lookup(Index, Key) of
        [#datum{mgr = undefined, data = Data} = Datum] ->
            update_ttl(Index, Datum),
            {ok, Data};
        [#datum{mgr = P}] when is_pid(P) -> {ecache, P};
        [] -> {ecache, notfound}
    end.

replace_datum(Key, Data, Index) when is_tuple(Key) ->
    ets:update_element(Index, Key, [{#datum.data, Data}, {#datum.last_active, timestamp()}]).

get_all_keys(Index) -> get_all_keys(Index, [ets:first(Index)]).

get_all_keys(_, ['$end_of_table'|Acc]) -> Acc;
get_all_keys(Index, [Key|_] = Acc) -> get_all_keys(Index, [ets:next(Index, Key)|Acc]).

generic_get(UseKey, From, #cache{datum_index = Index} = State, M, F, Key) ->
    case fetch_data(UseKey, Index) of
        {ok, _} = R -> {reply, R, State#cache{found = State#cache.found + 1}};
        {ecache, notfound} ->
            #cache{ttl = TTL, policy = Policy} = State,
            P = self(),
            ets:insert_new(Index,
                           #datum{key = UseKey,
                                  mgr = spawn(fun() ->
                                                  R = case launch_datum(Key, Index, M, F, TTL, Policy, UseKey) of
                                                          {ok, _} = Data ->
                                                              gen_server:cast(P, launched),
                                                              Data;
                                                          Error ->
                                                               ets:delete(Index, UseKey),
                                                               {error, Error}
                                                      end,
                                                  gen_server:reply(From, R),
                                                  exit(R)
                                              end)}),
            {noreply, State};
        {ecache, Launcher} = R when is_pid(Launcher) -> {reply, R, State}
    end.

start_reaper(Name, Size) ->
    {ok, Pid} = ecache_reaper:start_link(Name, Size),
    Pid.

timestamp() -> erlang:monotonic_time(milli_seconds).

time_diff(T2, T1) -> T2 - T1.
-compile({inline, [time_diff/2]}).

kill_reapers(Index) ->
    lists:foreach(fun kill_reaper/1, ets:select(Index, [{#datum{reaper = '$1', _ = '_'}, [], ['$1']}])).
-compile({inline, [kill_reapers/1]}).

kill_reaper(Reaper) -> not is_pid(Reaper) orelse exit(Reaper, kill).
