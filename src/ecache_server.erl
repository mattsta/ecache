-module(ecache_server).

-behaviour(gen_server).

-export([start_link/2, start_link/3, start_link/4]).
-export([start_link/5, start_link/6]).
-deprecated([{start_link, 5, next_major_release}, {start_link, 6, next_major_release}]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(state, {name :: atom(),
                table :: ets:tid(),
                table_pad = 0 :: non_neg_integer(),
                data_fun :: undefined|fun((term()) -> any()),
                reaper :: undefined|pid(),
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

-spec start_link(Name::atom(), Fun::fun((term()) -> any())) -> {ok, pid()} | {error, term()}.
start_link(Name, Fun) -> start_link(Name, Fun, #{}).

-spec start_link(Name::atom(), Fun::fun((term()) -> any()), Opts::ecache:options()) -> {ok, pid()} | {error, term()};
                (Name::atom(), {Mod::module(), Fun::atom()}, Opts::ecache:options()) -> {ok, pid()} | {error, term()};
                (Name::atom(), Mod::module(), Fun::atom()) -> {ok, pid()} | {error, term()}.
start_link(Name, {Mod, Fun}, Opts) when is_atom(Mod), is_atom(Fun) -> start_link(Name, fun Mod:Fun/1, Opts);
start_link(Name, Fun, Opts) when is_atom(Name), is_function(Fun, 1), is_map(Opts) ->
    #state{size = DSize, ttl = DTime, policy = DPolicy} = #state{},
    #{size := Size,
      time := Time,
      policy := Policy,
      compressed := Comp} = maps:merge(#{size => DSize,
                                         time => DTime,
                                         policy => DPolicy,
                                         compressed => true},
                                       Opts),
    gen_server:start_link({local, Name}, ?MODULE,
                          {#state{name = Name, data_fun = Fun, size = Size, policy = Policy, ttl = Time}, Comp},
                          []);
start_link(Name, Fun, Opts) when is_list(Opts) -> start_link(Name, Fun, maps:from_list(Opts));
% make 8 MB cache
start_link(Name, Mod, Fun) -> start_link(Name, {Mod, Fun}, #{}).

% make MRU policy cache
-spec start_link(Name::atom(), Mod::module(), Fun::atom(),
                 Size::unlimited|pos_integer(), Time::unlimited|pos_integer()) ->
          {ok, pid()} | {error, term()}.
start_link(Name, Mod, Fun, Size, Time) -> start_link(Name, {Mod, Fun}, #{size => Size, time => Time}).

-spec start_link(Name::atom(), Mod::module(), Fun::atom(),
                 Size::unlimited|pos_integer(), Time::unlimited|pos_integer(), Policy::ecache:policy()) ->
          {ok, pid()} | {error, term()}.
start_link(Name, Mod, Fun, Size, Time, Policy) ->
    start_link(Name, {Mod, Fun}, #{size => Size, time => Time, policy => Policy}).

-spec start_link(Name::atom(), Mod::module(), Fun::atom(), Opts::ecache:options()) -> {ok, pid()} | {error, term()};
                (Name::atom(), Mod::module(), Fun::atom(), Size::unlimited|pos_integer()) ->
          {ok, pid()} | {error, term()}.
start_link(Name, Mod, Fun, Opts) when is_map(Opts); is_list(Opts) -> start_link(Name, {Mod, Fun}, Opts);
% make 5 minute expiry cache
start_link(Name, Mod, Fun, Size) -> start_link(Name, {Mod, Fun}, #{size => Size}).

%%%----------------------------------------------------------------------
%%% Callback functions from gen_server
%%%----------------------------------------------------------------------

-spec init({#state{}, Comp::boolean()}) -> {ok, #state{}}.
init({#state{name = Name, policy = Policy, ttl = Time} = Cache, Comp})
  when Policy =:= actual_time orelse Policy =:= mru, Time =:= unlimited orelse is_integer(Time) andalso Time > 0 ->
    process_flag(trap_exit, true),
    TabOpts = [set,
               public,               % public because we spawn writers
               {keypos, #datum.key}, % use Key stored in record
               {read_concurrency, true}],
    T = ets:new(Name,
                if
                    Comp -> [compressed|TabOpts];
                    true -> TabOpts
                end),
    {ok, init_size(Cache#state{table = T, table_pad = ets:info(T, memory)})}.

init_size(#state{size = unlimited} = State) -> State;
init_size(#state{name = Name, size = Size} = State) when is_integer(Size), Size > 0 ->
    SizeBytes = Size * (1024 * 1024),
    State#state{reaper = start_reaper(Name, SizeBytes), size = SizeBytes}.

-spec handle_call(term(), {pid(), term()}, #state{}) -> {reply, term(), #state{}} | {noreply, #state{}}.
handle_call({get, Key}, From, #state{data_fun = F} = State) -> generic_get(key(Key), From, State, F, Key);
handle_call({generic_get, M, F, Key}, From, State) -> generic_get(key(M, F, Key), From, State, fun M:F/1, Key);
handle_call(total_size, _From, #state{} = State) -> {reply, cache_bytes(State), State};
handle_call(stats, _From, #state{table = T, found = Found, launched = Launched, policy = Policy, ttl = TTL} = State) ->
    EtsInfo = ets:info(T),
    {reply,
     [{cache_name, proplists:get_value(name, EtsInfo)},
      {memory_size_bytes, cache_bytes(State, proplists:get_value(memory, EtsInfo))},
      {datum_count, proplists:get_value(size, EtsInfo)},
      {found, Found}, {launched, Launched},
      {policy, Policy}, {ttl, TTL}],
     State};
handle_call(empty, _From, #state{table = T} = State) ->
    kill_reapers(T),
    ets:delete_all_objects(T),
    {reply, ok, State};
handle_call(reap_oldest, From, #state{table = T} = State) ->
    spawn(fun() ->
              DatumNow = #datum{last_active = timestamp()},
              LeastActive = ets:foldl(fun(#datum{last_active = LA} = A, #datum{last_active = Acc}) when LA < Acc -> A;
                                         (_, Acc) -> Acc
                                      end, DatumNow, T),
              LeastActive =:= DatumNow orelse delete_object(T, LeastActive),
              gen_server:reply(From, cache_bytes(State))
          end),
    {noreply, State};
handle_call({rand, Type, Count}, From, #state{table = T} = State) ->
    spawn(fun() ->
              AllKeys = get_all_keys(T),
              Length = length(AllKeys),
              gen_server:reply(From,
                               lists:map(if
                                             Type =:= data -> fun(K) ->
                                                                  {ok, Data} = fetch_data(K, T),
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

-spec handle_cast(term(), #state{}) -> {noreply, #state{}}.
handle_cast(launched, #state{launched = Launched} = State) -> {noreply, State#state{launched = Launched + 1}};
handle_cast(found, #state{found = Found} = State) -> {noreply, State#state{found = Found + 1}};
handle_cast({dirty, Id, NewData}, #state{table = T} = State) ->
    replace_datum(key(Id), NewData, T),
    {noreply, State};
handle_cast({dirty, Id}, #state{table = T} = State) ->
    delete_datum(T, key(Id)),
    {noreply, State};
handle_cast({generic_dirty, M, F, A}, #state{table = T} = State) ->
    delete_datum(T, key(M, F, A)),
    {noreply, State};
handle_cast(Req, State) ->
    error_logger:warning_msg("Other cast of: ~p~n", [Req]),
    {noreply, State}.

-spec handle_info(term(), #state{}) -> {noreply, #state{}}.
handle_info({destroy, _DatumPid, ok}, State) -> {noreply, State};
handle_info({'EXIT', Pid, _Reason}, #state{reaper = Pid, name = Name, size = Size} = State) ->
    {noreply, State#state{reaper = start_reaper(Name, Size)}};
handle_info({'EXIT', _Pid, _Reason}, State) -> {noreply, State};
handle_info({'DOWN', _Ref, process, _Pid, _Reason}, State) -> {noreply, State};
handle_info(Info, State) ->
    error_logger:warning_msg("Other info of: ~p~n", [Info]),
    {noreply, State}.

-spec terminate(term(), #state{}) -> ok.
terminate(_Reason, #state{reaper = Pid}) when is_pid(Pid) -> gen_server:stop(Pid);
terminate(_Reason, _State) -> ok.

-spec code_change(term(), State, term()) -> {ok, State} when State::#state{}.
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

cache_bytes(#state{table = T} = State) -> cache_bytes(State, ets:info(T, memory)).

cache_bytes(#state{table_pad = TabPad}, Mem) -> (Mem - TabPad) * erlang:system_info(wordsize).

delete_datum(T, Key) ->
    case ets:take(T, Key) of
        [#datum{reaper = Reaper}] -> kill_reaper(Reaper);
        _ -> true
    end.

delete_object(T, #datum{reaper = Reaper} = Datum) ->
    kill_reaper(Reaper),
    ets:delete_object(T, Datum).

-compile({inline, [create_datum/4]}).
create_datum(DatumKey, Data, TTL, Type) ->
    Timestamp = timestamp(),
    #datum{key = DatumKey, data = Data, type = Type,
           started = Timestamp, ttl = TTL, remaining_ttl = TTL, last_active = Timestamp}.

reap_after(T, Key, LifeTTL) ->
    receive
        {update_ttl, NewTTL} -> reap_after(T, Key, NewTTL)
    after LifeTTL -> ets:delete(T, Key)
    end.

launch_datum_ttl_reaper(_, _, #datum{remaining_ttl = unlimited} = Datum) -> Datum;
launch_datum_ttl_reaper(T, Key, #datum{remaining_ttl = TTL} = Datum) ->
    Datum#datum{reaper = spawn(fun() ->
                                   link(ets:info(T, owner)),
                                   reap_after(T, Key, TTL)
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

launch_datum(Key, T, F, TTL, Policy, UseKey) ->
    try F(Key) of
        CacheData ->
            ets:insert(T, launch_datum_ttl_reaper(T, UseKey, create_datum(UseKey, CacheData, TTL, Policy))),
            {ok, CacheData}
    catch
        ?EXCEPTION(How, What, Stacktrace) -> {ecache_datum_error, {{How, What}, ?GET_STACK(Stacktrace)}}
    end.

-compile({inline, [ping_reaper/2]}).
ping_reaper(Reaper, NewTTL) when is_pid(Reaper) -> Reaper ! {update_ttl, NewTTL};
ping_reaper(_, _) -> ok.

update_ttl(T, #datum{key = Key, ttl = unlimited}) -> ets:update_element(T, Key, {#datum.last_active, timestamp()});
update_ttl(T, #datum{key = Key, started = Started, ttl = TTL, type = actual_time, reaper = Reaper}) ->
    Timestamp = timestamp(),
    % Get total time in seconds this datum has been running.  Convert to ms.
    % If we are less than the TTL, update with TTL-used (TTL in ms too) else, we ran out of time.  expire on next loop.
    TTLRemaining = case time_diff(Timestamp, Started) of
                       StartedNowDiff when StartedNowDiff < TTL -> TTL - StartedNowDiff;
                       _ -> 0
                   end,
    ping_reaper(Reaper, TTLRemaining),
    ets:update_element(T, Key, [{#datum.last_active, Timestamp}, {#datum.remaining_ttl, TTLRemaining}]);
update_ttl(T, #datum{key = Key, ttl = TTL, reaper = Reaper}) ->
    ping_reaper(Reaper, TTL),
    ets:update_element(T, Key, {#datum.last_active, timestamp()}).

fetch_data(Key, T) when is_tuple(Key) ->
    case ets:lookup(T, Key) of
        [#datum{mgr = undefined, data = Data} = Datum] ->
            update_ttl(T, Datum),
            {ok, Data};
        [#datum{mgr = P}] when is_pid(P) -> {ecache, P};
        [] -> {ecache, notfound}
    end.

replace_datum(Key, Data, T) when is_tuple(Key) ->
    ets:update_element(T, Key, [{#datum.data, Data}, {#datum.last_active, timestamp()}]).

get_all_keys(T) -> get_all_keys(T, [ets:first(T)]).

get_all_keys(_, ['$end_of_table'|Acc]) -> Acc;
get_all_keys(T, [Key|_] = Acc) -> get_all_keys(T, [ets:next(T, Key)|Acc]).

generic_get(UseKey, From, #state{table = T} = State, F, Key) ->
    case fetch_data(UseKey, T) of
        {ok, _} = R -> {reply, R, State#state{found = State#state.found + 1}};
        {ecache, notfound} ->
            #state{ttl = TTL, policy = Policy} = State,
            P = self(),
            ets:insert_new(T, #datum{key = UseKey,
                                     mgr = spawn(fun() ->
                                                     R = case launch_datum(Key, T, F, TTL, Policy, UseKey) of
                                                             {ok, _} = Data ->
                                                                 gen_server:cast(P, launched),
                                                                 Data;
                                                             Error ->
                                                                 ets:delete(T, UseKey),
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

kill_reapers(T) -> lists:foreach(fun kill_reaper/1, ets:select(T, [{#datum{reaper = '$1', _ = '_'}, [], ['$1']}])).
-compile({inline, [kill_reapers/1]}).

kill_reaper(Reaper) -> not is_pid(Reaper) orelse exit(Reaper, kill).
