from redis.client import list_or_args

from rediscluster.client import RedisCluster
from rediscluster.client import StrictRedisCluster
from rediscluster.exceptions import RedisClusterException
from rediscluster.pipeline import StrictClusterPipeline


class PrefixedStrictRedisCluster(StrictRedisCluster):

    def __init__(self, *args, key_prefix, **kwargs):
        self.key_prefix = key_prefix
        super().__init__(*args, **kwargs)

    def make_key(self, key):
        if str(key)[0] == ':':
            return f'{self.key_prefix}{key}'

        return f'{self.key_prefix}:1:{key}'

    def make_keys(self, keys):
        return [self.make_key(key) for key in keys]

    def flushall(self):
        raise RedisClusterException("method PrefixedRedisCluster.flushall() is not implemented")

    def flushdb(self):
        raise RedisClusterException("method PrefixedRedisCluster.flushall() is not implemented")

    # BASIC KEY COMMANDS
    def append(self, key, value):
        return super().append(self.make_keys(key), value)

    def bitcount(self, key, start=None, end=None):
        return super().bitcount(self.make_keys(key), start=start, end=end)

    def bitop(self, operation, dest, *keys):
        return super().bitop(operation, self.make_key(dest), *self.make_keys(keys))

    def bitpos(self, key, bit, start=None, end=None):
        return super().bitpos(self.make_key(key), bit, start=start, end=end)

    def decr(self, name, amount=1):
        return super().decr(self.make_key(name), amount=amount)

    def delete(self, *names):
        return super().delete(self.make_keys(names))

    def dump(self, name):
        return super().dump(self.make_key(name))

    def exists(self, name):
        return super().exists(self.make_key(name))
    __contains__ = exists

    def expire(self, name, time):
        return super().expire(self.make_key(name), time)

    def expireat(self, name, when):
        return super().expireat(self.make_key(name), when)

    def get(self, name):
        return super().get(self.make_key(name))

    def getbit(self, name, offset):
        return super().getbit(self.make_key(name), offset)

    def getrange(self, key, start, end):
        return super().getrange(self.make_key(key), start, end)

    def getset(self, name, value):
        return super().getset(self.make_key(name), value)

    def incr(self, name, amount=1):
        return super().incr(self.make_key(name), amount=amount)

    def incrbyfloat(self, name, amount=1.0):
        return super().incrbyfloat(self.make_key(name), amount=amount)

    def keys(self, pattern='*'):
        return super().keys(pattern=self.make_key(pattern))

    def move(self, name, db):
        return super().move(self.make_key(name), db)

    def persist(self, name):
        return super().persist(self.make_key(name))

    def pexpire(self, name, time):
        return super().pexpire(self.make_key(name), time)

    def pexpireat(self, name, when):
        return super().pexpire(self.make_key(name), when)

    def psetex(self, name, time_ms, value):
        return super().psetex(self.make_key(name), time_ms, value)

    def pttl(self, name):
        return super().pttl(self.make_key(name))

    def restore(self, name, ttl, value, replace=False):
        return super().restore(self.make_key(name), ttl, value, replace=replace)

    def set(self, name, value, ex=None, px=None, nx=False, xx=False):
        return super().set(self.make_key(name), value, ex=ex, px=px, nx=nx, xx=xx)

    def setbit(self, name, offset, value):
        return super().setbit(self.make_key(name), offset, value)

    def setex(self, name, time, value):
        return super().setex(self.make_key(name), time, value)

    def setnx(self, name, value):
        return super().setnx(self.make_key(name), value)

    def setrange(self, name, offset, value):
        return super().setrange(self.make_key(name), offset, value)

    def strlen(self, name):
        return super().strlen(self.make_key(name))

    def substr(self, name, start, end=-1):
        return super().substr(self.make_key(name), start, end=-1)

    def touch(self, *args):
        return super().touch(*self.make_keys(args))

    def ttl(self, name):
        return super().ttl(self.make_key(name))

    def type(self, name):
        return super().type(self.make_key(name))

    # LIST COMMANDS
    def blpop(self, keys, timeout=0):
        return super().blpop(self.make_keys(keys), timeout=timeout)

    def brpop(self, keys, timeout=0):
        return super().brpop(self.make_keys(keys), timeout=timeout)

    def brpoplpush(self, src, dst, timeout=0):
        return super().brpoplpush(self.make_key(src), self.make_key(dst), timeout=timeout)

    def lindex(self, name, index):
        return super().lindex(self.make_key(name), index)

    def linsert(self, name, where, refvalue, value):
        return super().linsert(self.make_key(name), where, refvalue, value)

    def llen(self, name):
        return super().llen(self.make_key(name))

    def lpop(self, name):
        return super().lpop(self.make_key(name))

    def lpush(self, name, *values):
        return super().lpush(self.make_key(name), *values)

    def lpushx(self, name, value):
        return super().lpushx(self.make_key(name), value)

    def lrange(self, name, start, end):
        return super().lrange(self.make_key(name), start, end)

    def lrem(self, name, count, value):
        return super().lrem(self.make_key(name), count, value)

    def lset(self, name, index, value):
        return super().lset(self.make_key(name), index, value)

    def ltrim(self, name, start, end):
        return super().ltrim(self.make_key(name), start, end)

    def rpop(self, name):
        return super().rpop(self.make_key(name))

    def rpoplpush(self, src, dst):
        return super().rpoplpush(self.make_key(src), self.make_key(dst))

    def rpush(self, name, *values):
        return super().rpush(self.make_key(name), *values)

    def rpushx(self, name, value):
        return super().rpush(self.make_key(name), value)

    def sort(self, name, start=None, num=None, by=None, get=None, desc=False, alpha=False, store=None, groups=False):
        return super().sort(
            self.make_key(name), start=start, num=num, by=by, get=get,
            desc=desc, alpha=alpha, store=store, groups=groups
        )

    # SCAN COMMANDS
    def scan(self, cursor=0, match=None, count=None):
        match = match or '*'
        return super().scan(cursor=cursor, match=self.make_key(match), count=count)

    def scan_iter(self, match=None, count=None):
        match = match or '*'
        return super().scan_iter(match=self.make_key(match), count=count)

    def sscan(self, name, cursor=0, match=None, count=None):
        match = match or '*'
        return super().sscan(self.make_key(name), cursor=cursor, match=self.make_key(match), count=count)

    def hscan(self, name, cursor=0, match=None, count=None):
        match = match or '*'
        return super().hscan(self.make_key(name), cursor=cursor, match=self.make_key(match), count=count)

    def zscan(self, name, cursor=0, match=None, count=None, score_cast_func=float):
        match = match or '*'
        return super().zscan(
            self.make_key(name), cursor=cursor, match=self.make_key(match), count=count, score_cast_func=score_cast_func
        )

    # SET COMMANDS
    def sadd(self, name, *values):
        return super().sadd(self.make_key(name), *values)

    def scard(self, name):
        return super().scard(self.make_key(name))

    def sdiff(self, keys, *args):
        keys = list_or_args(keys, args)
        return super().sdiff(self.make_keys(keys))

    def sdiffstore(self, dest, keys, *args):
        keys = list_or_args(keys, args)
        return super().sdiffstore(self.make_key(dest), self.make_keys(keys))

    def sinter(self, keys, *args):
        keys = list_or_args(keys, args)
        return super().sinter(self.make_keys(keys))

    def sinterstore(self, dest, keys, *args):
        keys = list_or_args(keys, args)
        return super().sinterstore(self.make_key(dest), self.make_keys(keys))

    def sismember(self, name, value):
        return super().sismember(self.make_key(name), value)

    def smembers(self, name):
        return super().smembers(self.make_key(name))

    def smove(self, src, dst, value):
        return super().smove(self.make_key(src), self.make_key(dst), value)

    def spop(self, name):
        return super().spop(self.make_key(name))

    def srandmember(self, name, number=None):
        return super().srandmember(self.make_key(name), number=number)

    def srem(self, name, *values):
        return super().srem(self.make_key(name), *values)

    def sunion(self, keys, *args):
        keys = list_or_args(keys, args)
        return super().sunion(self.make_keys(keys))

    def sunionstore(self, dest, keys, *args):
        keys = list_or_args(keys, args)
        return super().sunion(self.make_key(dest), self.make_keys(keys))

    # SORTED SET COMMANDS
    # TODO: everything after


class PrefixedStrictClusterPipeline(StrictClusterPipeline, PrefixedStrictRedisCluster):

    def __init__(self, *args, key_prefix, **kwargs):
        self.key_prefix = key_prefix
        super().__init__(*args, **kwargs)


class PrefixedRedisCluster(PrefixedStrictRedisCluster, RedisCluster):

    def pipeline(self, transaction=None, shard_hint=None):
        if shard_hint:
            raise RedisClusterException("shard_hint is deprecated in cluster mode")

        if transaction:
            raise RedisClusterException("transaction is deprecated in cluster mode")

        return PrefixedStrictClusterPipeline(
            connection_pool=self.connection_pool,
            startup_nodes=self.connection_pool.nodes.startup_nodes,
            response_callbacks=self.response_callbacks,
            key_prefix=self.key_prefix,
        )
