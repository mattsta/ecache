-module(ecache).

-export([child_spec/2, child_spec/3, child_spec/4]).
-export([dirty/2, dirty/3, dirty_memoize/4, empty/1, get/2, memoize/4]).
-export([stats/1, total_size/1]).
-export([rand/2, rand_keys/2]).

-export([cache_sup/4, cache_ttl_sup/5]).
-deprecated([{cache_sup, 4, next_major_release}, {cache_ttl_sup, 5, next_major_release}]).

-define(TIMEOUT, infinity).

-type policy() :: mru|actual_time.
-type options() :: #{size => unlimited|pos_integer(), time => unlimited|pos_integer(),
                     policy => policy(), compressed => boolean()} |
                   [{size, unlimited|pos_integer()}|{time, unlimited|pos_integer()}|
                    {policy , policy()}|{compressed, boolean()}].

-export_type([policy/0, options/0]).

%% ===================================================================
%% Supervisory helpers
%% ===================================================================

-define(DEFAULT_OPTS, #{size => unlimited, time => unlimited, policy => actual_time, compressed => false}).

-spec child_spec(Name::atom(), Fun::fun((term()) -> any())|{module(), atom()}) -> supervisor:child_spec().
child_spec(Name, Fun) -> child_spec(Name, Fun, #{}).

-spec child_spec(Name::atom(), Fun::fun((term()) -> any()), Opts::options()) -> supervisor:child_spec();
                (Name::atom(), {Mod::module(), Fun::atom()}, Opts::options()) -> supervisor:child_spec();
                (Name::atom(), Mod::module(), Fun::atom()) -> supervisor:child_spec().
child_spec(Name, Fun, Opts) when is_function(Fun, 1), is_atom(Name), is_map(Opts) ->
    #{id => Name,
      start => {ecache_server, start_link, [Name, Fun, maps:merge(?DEFAULT_OPTS, Opts)]},
      shutdown => brutal_kill};
child_spec(Name, Fun, Opts) when is_list(Opts) -> child_spec(Name, Fun, maps:from_list(Opts));
child_spec(Name, {Mod, Fun}, Opts) when is_atom(Mod), is_atom(Fun) -> child_spec(Name, fun Mod:Fun/1, Opts);
child_spec(Name, Mod, Fun) -> child_spec(Name, Mod, Fun, #{}).

-spec child_spec(Name::atom(), Mod::module(), Fun::atom(), Opts::options()) -> supervisor:child_spec().
child_spec(Name, Mod, Fun, Opts) when is_atom(Name), is_atom(Mod), is_atom(Fun), is_map(Opts) ->
    #{id => Name,
      start => {ecache_server, start_link, [Name, Mod, Fun, maps:merge(?DEFAULT_OPTS, Opts)]},
      shutdown => brutal_kill};
child_spec(Name, Mod, Fun, Opts) when is_list(Opts) -> child_spec(Name, Mod, Fun, maps:from_list(Opts)).

-spec cache_sup(Name, Mod::module(), Fun::atom(), Opts::options()) ->
          {Name, {ecache_server, start_link, [term()], permanent, brutal_kill, worker, [ecache_server]}}
        when Name::atom().
cache_sup(Name, Mod, Fun, Opts) ->
    {Name, {ecache_server, start_link, [Name, Mod, Fun, Opts]}, permanent, brutal_kill, worker, [ecache_server]}.

-spec cache_ttl_sup(Name::atom(), Mod::module(), Fun::atom(),
                    Size::unlimited|pos_integer(), TTL::unlimited|pos_integer()) ->
          {Name, {ecache_server, start_link, [term()], permanent, brutal_kill, worker, [ecache_server]}}
        when Name::atom().
cache_ttl_sup(Name, Mod, Fun, Size, TTL) ->
    {Name, {ecache_server, start_link, [Name, Mod, Fun, #{size => Size, time => TTL}]},
     permanent, brutal_kill, worker, [ecache_server]}.

%% ===================================================================
%% Calls into ecache_server
%% ===================================================================

-spec get(Name::atom(), Key::term()) -> term().
get(Name, Key) -> get_value(Name, {get, Key}).

-spec memoize(MemoizeCacheServer::atom(), Module::module(), Fun::atom(), Key::term()) -> term().
memoize(MemoizeCacheServer, Module, Fun, Key) -> get_value(MemoizeCacheServer, {generic_get, Module, Fun, Key}).

-spec dirty_memoize(MemoizeCacheServer::atom(), Module::module(), Fun::atom(), Key::term()) -> ok.
dirty_memoize(MemoizeCacheServer, Module, Fun, Key) ->
    gen_server:cast(MemoizeCacheServer, {generic_dirty, Module, Fun, Key}).

-spec empty(Name::atom()) -> ok.
empty(Name) -> gen_server:call(Name, empty).

-spec total_size(Name::atom()) -> non_neg_integer().
total_size(Name) -> gen_server:call(Name, total_size).

-spec stats(Name::atom()) ->
          [{cache_name, atom()}|
           {memory_size_bytes, pos_integer()}|
           {datum_count, non_neg_integer()}|
           {found, non_neg_integer()}|
           {launched, non_neg_integer()}|
           {policy, policy()}|
           {ttl, unlimited|pos_integer()}].
stats(Name) -> gen_server:call(Name, stats).

-spec dirty(Name::atom(), Key::term(), NewData::term()) -> ok.
dirty(Name, Key, NewData) -> gen_server:cast(Name, {dirty, Key, NewData}).

-spec dirty(Name::atom(), Key::term()) -> ok.
dirty(Name, Key) -> gen_server:cast(Name, {dirty, Key}).

-spec rand(Name::atom(), Count::non_neg_integer()) -> [term()].
rand(Name, Count) -> gen_server:call(Name, {rand, data, Count}).

-spec rand_keys(Name::atom(), Count::non_neg_integer()) -> [term()].
rand_keys(Name, Count) -> gen_server:call(Name, {rand, keys, Count}).

get_value(Name, Req) ->
    case gen_server:call(Name, Req, ?TIMEOUT) of
        {ok, Data} -> Data;
        {error, Error} -> Error;
        {ecache, Launcher} ->
            Ref = monitor(process, Launcher),
            receive
                {'DOWN', Ref, process, _, {ok, Data}} ->
                    gen_server:cast(Name, found),
                    Data;
                {'DOWN', Ref, process, _, {error, Error}} -> Error;
                {'DOWN', Ref, process, _, _noproc} -> get_value(Name, Req)
            end
    end.
