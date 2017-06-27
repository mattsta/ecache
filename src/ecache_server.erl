-module(ecache_server).

-behaviour(gen_server).

-export([start_link/3, start_link/4, start_link/5, start_link/6]).
-export([init/1, handle_call/3, handle_cast/2,
         handle_info/2, terminate/2, code_change/3]).

-record(cache, {name :: atom(),
                datum_index :: ets:tid(),
                table_pad = 0 :: non_neg_integer(),
                data_module :: module(),
                reaper :: undefined|pid(),
                data_accessor :: atom(),
                size :: unlimited|non_neg_integer(),
                found = 0 :: non_neg_integer(),
                launched = 0 :: non_neg_integer(),
                policy = mru :: mru|actual_time,
                ttl = unlimited :: unlimited|non_neg_integer(),
                update_key_locks = #{} :: #{term() => pid()}}).

-record(datum, {key, mgr, data, started, reaper,
                last_active, ttl, type = mru, remaining_ttl}).

% make 8 MB cache
start_link(Name, Mod, Fun) -> start_link(Name, Mod, Fun, 8).

% make 5 minute expiry cache
start_link(Name, Mod, Fun, Size) -> start_link(Name, Mod, Fun, Size, 300000).

% make MRU policy cache
start_link(Name, Mod, Fun, Size, Time) -> start_link(Name, Mod, Fun, Size, Time, mru).

start_link(Name, Mod, Fun, Size, Time, Policy) ->
    gen_server:start_link({local, Name}, ?MODULE, [Name, Mod, Fun, Size, Time, Policy], []).

%%%----------------------------------------------------------------------
%%% Callback functions from gen_server
%%%----------------------------------------------------------------------

init([Name, Mod, Fun, Size, Time, Policy]) ->
    Index = ets:new(Name, [set, compressed,
                           public,      % public because we spawn writers
                           {keypos, #datum.key}, % use Key stored in record
                           {read_concurrency, true}]),
    init(#cache{name = Name,
                datum_index = Index,
                table_pad = ets:info(Index, memory),
                data_module = Mod,
                data_accessor = Fun,
                ttl = Time,
                policy = Policy,
                size = if
                           Size =:= unlimited -> unlimited;
                           true -> Size * (1024 * 1024)
                       end});
init(#cache{size = unlimited} = State) -> {ok, State};
init(#cache{name = Name, size = SizeBytes} = State) ->
    {ok, Reaper} = ecache_reaper:start(Name, SizeBytes),
    {ok, State#cache{reaper = erlang:monitor(process, Reaper)}}.

handle_call({generic_get, M, F, Key}, From, #cache{datum_index = Index} = State) ->
    P = self(),
    case fetch_data(key(M, F, Key), Index) of
        {ecache, notfound} ->
            {noreply,
             case State#cache.update_key_locks of
                 #{Key := CurrentLockPid} when is_pid(CurrentLockPid) ->
                     spawn(fun() ->
                               Ref = monitor(process, CurrentLockPid),
                               receive
                                   {'DOWN', Ref, process, _, _} -> gen_server:reply(From, gen_server:call(P, {get, Key}))
                               end
                           end),
                     State;
                 Locks ->
                     #cache{ttl = TTL, policy = Policy} = State,
                     State#cache{update_key_locks = Locks#{Key => spawn(fun() ->
                                                                            Data = launch_memoize_datum(Key, Index, M, F,
                                                                                                        TTL, Policy),
                                                                            gen_server:cast(P, {launched, Key}),
                                                                            gen_server:reply(From, Data)
                                                                        end)}}
            end};
        Data ->
            spawn(fun() -> gen_server:cast(P, found) end),
            {reply, Data, State}
    end;
handle_call({get, Key}, From, #cache{datum_index = Index} = State) ->
    P = self(),
    case fetch_data(key(Key), Index) of
        {ecache, notfound} ->
            {noreply,
             case State#cache.update_key_locks of
                 #{Key := CurrentLockPid} when is_pid(CurrentLockPid) ->
                     spawn(fun() ->
                               Ref = erlang:monitor(process, CurrentLockPid),
                               receive
                                   {'DOWN', Ref, process, _, _} -> gen_server:reply(From, gen_server:call(P, {get, Key}))
                               end
                           end),
                     State;
                 Locks ->
                     #cache{data_module = Module, data_accessor = Accessor, ttl = TTL, policy = Policy} = State,
                     State#cache{update_key_locks = Locks#{Key => spawn(fun() ->
                                                                            Data = launch_datum(Key, Index, Module,
                                                                                                Accessor, TTL, Policy),
                                                                            gen_server:cast(P, {launched, Key}),
                                                                            gen_server:reply(From, Data)
                                                                        end)}}
             end};
        Data ->
            spawn(fun() -> gen_server:cast(P, found) end),
            {reply, Data, State}
    end;
% NB: total_size using ETS includes ETS overhead.  An empty table still
% has a size.
handle_call(total_size, _From, #cache{} = State) -> {reply, cache_bytes(State), State};
handle_call(stats, _From, #cache{datum_index = Index, found = Found, launched = Launched} = State) ->
    EtsInfo = ets:info(Index),
    {reply,
     [{cache_name, proplists:get_value(name, EtsInfo)},
      {memory_size_bytes, cache_bytes(State, proplists:get_value(memory, EtsInfo))},
      {datum_count, proplists:get_value(size, EtsInfo)},
      {found, Found}, {launched, Launched}],
     State};
handle_call(empty, _From, #cache{datum_index = Index} = State) ->
    lists:foreach(fun([Reaper]) when is_pid(Reaper) -> exit(Reaper, kill);
                     (_) -> ok
                  end, ets:match(Index, #datum{_ = '_', reaper = '$1'})),
    ets:delete_all_objects(Index),
    {reply, ok, State};
handle_call(reap_oldest, _From, #cache{datum_index = Index} = State) ->
    DatumNow = #datum{last_active = os:timestamp()},
    LeastActive = ets:foldl(fun(#datum{last_active = LA} = A, #datum{last_active = Acc}) when LA < Acc -> A;
                               (_, Acc) -> Acc
                            end, DatumNow, Index),
    LeastActive =:= DatumNow orelse delete_object(Index, LeastActive),
    {reply, ok, State};
handle_call({rand, Type, Count}, From, #cache{datum_index = Index} = State) ->
    spawn(fun() ->
              AllKeys = get_all_keys(Index),
              Length = length(AllKeys),
              gen_server:reply(From,
                               lists:map(if
                                             Type =:= data -> fun(K) -> fetch_data(K, Index) end;
                                             Type =:= keys -> fun unkey/1
                                         end,
                                         if
                                             Length =< Count -> AllKeys;
                                             true -> lists:map(fun(K) -> lists:nth(K, AllKeys) end,
                                                               lists:map(fun(_) -> crypto:rand_uniform(1, Length) end,
                                                                         lists:seq(1, Count)))
                                         end))
          end),
    {noreply, State};
handle_call(Arbitrary, _From, State) -> {reply, {arbitrary, Arbitrary}, State}.

handle_cast({dirty, Id, NewData}, #cache{datum_index = Index} = State) ->
    replace_datum(key(Id), NewData, Index),
    {noreply, State};
handle_cast(found, #cache{found = Found} = State) -> {noreply, State#cache{found = Found + 1}};
handle_cast({launched, DatumKey}, #cache{launched = Launched, update_key_locks = Locks} = State) ->
    {noreply, State#cache{launched = Launched + 1, update_key_locks = maps:remove(DatumKey, Locks)}};
handle_cast({dirty, Id}, #cache{datum_index = Index} = State) ->
    delete_datum(Index, key(Id)),
    {noreply, State};
handle_cast({generic_dirty, M, F, A}, #cache{datum_index = Index} = State) ->
    delete_datum(Index, key(M, F, A)),
    {noreply, State}.

handle_info({destroy,_DatumPid, ok}, State) -> {noreply, State};
handle_info({'DOWN', _Ref, process, Reaper, _Reason}, #cache{reaper = Reaper, name = Name, size = Size} = State) ->
    {NewReaper, _Mon} = ecache_reaper:start_link(Name, Size),
    {noreply, State#cache{reaper = NewReaper}};
handle_info({'DOWN', _Ref, process, _Pid, _Reason}, State) -> {noreply, State};
handle_info(Info, State) ->
    error_logger:info_report("Other info of: ~p~n", [Info]),
    {noreply, State}.

terminate(_Reason, _State) -> ok.

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
        [#datum{reaper = Reaper}] when is_pid(Reaper) -> exit(Reaper, kill);
        _ -> true
    end.

delete_object(Index, #datum{reaper = Reaper} = Datum) ->
    is_pid(Reaper) andalso exit(Reaper, kill),
    ets:delete_object(Index, Datum).

-compile({inline, [create_datum/4]}).
create_datum(DatumKey, Data, TTL, Type) ->
    Timestamp = os:timestamp(),
    #datum{key = DatumKey, data = Data, type = Type,
           started = Timestamp, ttl = TTL, remaining_ttl = TTL, last_active = Timestamp}.

reap_after(EtsIndex, Key, LifeTTL) ->
    receive
        {update_ttl, NewTTL} -> reap_after(EtsIndex, Key, NewTTL)
    after LifeTTL ->
        ets:delete(EtsIndex, Key),
        exit(self(), kill)
    end.

launch_datum_ttl_reaper(_, _, #datum{remaining_ttl = unlimited} = Datum) -> Datum;
launch_datum_ttl_reaper(EtsIndex, Key, #datum{remaining_ttl = TTL} = Datum) ->
    Datum#datum{reaper = spawn_link(fun() -> reap_after(EtsIndex, Key, TTL) end)}.


-compile({inline, [datum_error/2]}).
datum_error(How, What) -> {ecache_datum_error, {How, What}}.

launch_datum(Key, EtsIndex, Module, Accessor, TTL, Policy) ->
    try Module:Accessor(Key) of
        CacheData ->
            UseKey = key(Key),
            ets:insert(EtsIndex,
                       launch_datum_ttl_reaper(EtsIndex, UseKey, create_datum(UseKey, CacheData, TTL, Policy))),
            CacheData
    catch
        How:What -> datum_error({How, What}, erlang:get_stacktrace())
    end.

launch_memoize_datum(Key, EtsIndex, Module, Accessor, TTL, Policy) ->
    try Module:Accessor(Key) of
        CacheData ->
            UseKey = key(Module, Accessor, Key),
            ets:insert(EtsIndex,
                       launch_datum_ttl_reaper(EtsIndex, UseKey, create_datum(UseKey, CacheData, TTL, Policy))),
            CacheData
    catch
        How:What -> datum_error({How, What}, erlang:get_stacktrace())
    end.

-compile({inline, [ping_reaper/2]}).
ping_reaper(Reaper, NewTTL) when is_pid(Reaper) -> Reaper ! {update_ttl, NewTTL};
ping_reaper(_, _) -> ok.

update_ttl(Index, #datum{key = Key, ttl = unlimited}) ->
    ets:update_element(Index, Key, {#datum.last_active, os:timestamp()});
update_ttl(Index, #datum{key = Key, started = Started, ttl = TTL, type = actual_time, reaper = Reaper}) ->
    Timestamp = os:timestamp(),
    % Get total time in seconds this datum has been running.  Convert to ms.
    % If we are less than the TTL, update with TTL-used (TTL in ms too) else, we ran out of time.  expire on next loop.
    TTLRemaining = case timer:now_diff(Timestamp, Started) div 1000 of
                       StartedNowDiff when StartedNowDiff < TTL -> TTL - StartedNowDiff;
                       _ -> 0
                   end,
    ping_reaper(Reaper, TTLRemaining),
    ets:update_element(Index, Key, [{#datum.last_active, Timestamp}, {#datum.remaining_ttl, TTLRemaining}]);
update_ttl(Index, #datum{key = Key, ttl = TTL, reaper = Reaper}) ->
    ping_reaper(Reaper, TTL),
    ets:update_element(Index, Key, {#datum.last_active, os:timestamp()}).

fetch_data(Key, Index) when is_tuple(Key) ->
    case ets:lookup(Index, Key) of
        [Datum] ->
            update_ttl(Index, Datum),
            Datum#datum.data;
        [] -> {ecache, notfound}
    end.

replace_datum(Key, Data, Index) when is_tuple(Key) ->
  ets:update_element(Index, Key, [{#datum.data, Data}, {#datum.last_active, os:timestamp()}]),
  Data.

get_all_keys(EtsIndex) -> get_all_keys(EtsIndex, ets:first(EtsIndex), []).

get_all_keys(_, '$end_of_table', Accum) -> Accum;
get_all_keys(EtsIndex, NextKey, Accum) -> get_all_keys(EtsIndex, ets:next(EtsIndex, NextKey), [NextKey|Accum]).
