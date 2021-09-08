ecache: Erlang ETS Based TTL Cache
==================================

![Erlang CI](https://github.com/Ledest/ecache/workflows/Erlang%20CI/badge.svg)
[![Build Status](https://secure.travis-ci.org/Ledest/ecache.png)](http://travis-ci.org/Ledest/ecache)

ecache stores your cache items in ets.  Each cache item gets its own monitoring
process to auto-delete the cache item when the TTL expires.

ecache has the API of [pcache](http://github.com/mattsta/pcache) but stores data in ets
instead of storing data in individual processes.

Usage
-----
The cache server is designed to memoize a specific Module:Fun. The key in
a cache is the Argument passed to Module:Fun/1.

Start a cache:

```erlang
CacheName = my_cache,
M = database,
F = get_result,
Size = 16,     % 16 MB cache
Time = 300000, % 300,000 ms = 300s = 5 minute TTL
Server = ecache_server:start_link(CacheName, M, F, #{size => Size, time => Time}).
```

The TTL is an idle timer.  When an entry is accessed, the TTL for the entry is reset.
A cache with a five minute TTL expires entries when nothing touches an entry for five minutes.

You can update a cached value by marking an entry dirty.  You can mark an entry dirty with
no arguments so it immediately gets deleted (the next request will request the data from
your backing function again) or you can dirty an entry with a new value in-place.

### Example of Cache Usage

```erlang
Result = ecache:get(my_cache, <<"bob">>).
ecache:dirty(my_cache, <<"bob">>, <<"newvalue">>).  % replace entry for <<"bob">>
ecache:dirty(my_cache, <<"bob">>).  % remove entry from cache
ecache:empty(my_cache).  % remove all entries from cache
RandomValues = ecache:rand(my_cache, 12).
RandomKeys = ecache:rand_keys(my_cache, 12).
```

### Example of Caching Arbitrary M:F/1 Calls

Bonus feature: Instead of creating one cache per backing function, you can also
memoize any arbitrary M:F/1 call.

```erlang
Result = ecache:memoize(my_cache, OtherMod, OtherFun, Arg).
ecache:dirty_memoize(my_cache, OtherMod, OtherFun, Arg).  % remove entry from cache
```

`ecache:memoize/4` helps us get around annoying issues of one-cache-per-mod-fun.
Your root cache Mod:Fun could point to a function you never use if you only want to use
`ecache:memoize/4` functionality.

### Supervisor Help

Nobody likes writing supervisor entries by hand, so we provide a supervisor entry helper.
This is quite useful because many production applications will have 5 to 100 individual cache pools.

```erlang
SupervisorWorkerTuple = ecache:child_spec(Name, M, F, #{size => Size}).
```

For more examples, see https://github.com/Ledest/ecache/blob/master/test/ecache_tests.erl


Status
------
ecache is pcache but converted to use ets instead of processes.  ecache is more efficient, allows compression (because cache entries are stored in ets and ets supports compression), and is easier to understand.

### Version 1.0 (actually version 0.3.1)

Initial conversion from pcache.  Supports Erlang versions before 18.0 (uses `erlang:now()` for TTL math).

### Version 2.0

Modern release.  Provides proper stack traces if your underlying cache functions fail.  Uses `os:timestamp()` instead of `now()` for TTL math.


Building
--------
        rebar compile

Testing
-------
        rebar eunit suite=ecache

TODO
----
### Add tests for

* Other TTL variation
* Reaper

### Future features

* Cache pools?  Cross-server awareness?
* Expose per-entry TTL to external setting/updating
* Expose ETS configuration to make compression and read/write optimizations settable per-cache.
