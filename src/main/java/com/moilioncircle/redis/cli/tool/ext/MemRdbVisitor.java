package com.moilioncircle.redis.cli.tool.ext;

import com.moilioncircle.redis.cli.tool.cmd.glossary.Escape;
import com.moilioncircle.redis.cli.tool.cmd.glossary.Type;
import com.moilioncircle.redis.replicator.Replicator;
import com.moilioncircle.redis.replicator.event.Event;
import com.moilioncircle.redis.replicator.io.RedisInputStream;
import com.moilioncircle.redis.replicator.rdb.datatype.DB;

import java.io.File;
import java.io.IOException;
import java.util.List;

/**
 * @author Baoyi Chen
 */
public class MemRdbVisitor extends BaseRdbVisitor {
    public MemRdbVisitor(Replicator replicator, File out, Long db, List<String> regexs, Long top, List<Type> types, Escape escape) throws Exception {
        super(replicator, out, db, regexs, top, types, escape);
    }
    
    @Override
    protected Event doApplyZSet(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException {
        return null;
    }
    
    @Override
    protected Event doApplySet(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException {
        return null;
    }
    
    @Override
    protected Event doApplyList(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException {
        return null;
    }
    
    @Override
    protected Event doApplyString(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException {
        return null;
    }
    
    @Override
    protected Event doApplyZSet2(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException {
        return null;
    }
    
    @Override
    protected Event doApplyHash(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException {
        return null;
    }
    
    @Override
    protected Event doApplyHashZipMap(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException {
        return null;
    }
    
    @Override
    protected Event doApplyListZipList(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException {
        return null;
    }
    
    @Override
    protected Event doApplySetIntSet(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException {
        return null;
    }
    
    @Override
    protected Event doApplyZSetZipList(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException {
        return null;
    }
    
    @Override
    protected Event doApplyHashZipList(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException {
        return null;
    }
    
    @Override
    protected Event doApplyListQuickList(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException {
        return null;
    }
    
    @Override
    protected Event doApplyModule(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException {
        return null;
    }
    
    @Override
    protected Event doApplyModule2(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException {
        return null;
    }
    
    @Override
    protected Event doApplyStreamListPacks(RedisInputStream in, DB db, int version, byte[] key, boolean contains, int type) throws IOException {
        return null;
    }
}