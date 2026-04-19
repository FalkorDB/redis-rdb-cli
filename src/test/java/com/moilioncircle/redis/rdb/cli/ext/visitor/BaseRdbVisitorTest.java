package com.moilioncircle.redis.rdb.cli.ext.visitor;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;

import org.junit.Test;

import com.moilioncircle.redis.rdb.cli.conf.Configure;
import com.moilioncircle.redis.rdb.cli.filter.Filter;
import com.moilioncircle.redis.replicator.io.RedisInputStream;

public class BaseRdbVisitorTest {
    
    private final BaseRdbVisitor visitor = new BaseRdbVisitor(null, Configure.bind(), new Filter() {
        @Override
        public boolean contains(long db) {
            return true;
        }
        
        @Override
        public boolean contains(long db, int type, String key, boolean hasTTL) {
            return true;
        }
    }) {
    };
    
    @Test
    public void testApplyVersion13() throws Exception {
        RedisInputStream in = new RedisInputStream(new ByteArrayInputStream("0013".getBytes()));
        assertEquals(13, visitor.applyVersion(in));
    }
    
    @Test(expected = UnsupportedOperationException.class)
    public void testApplyVersion14Unsupported() throws Exception {
        RedisInputStream in = new RedisInputStream(new ByteArrayInputStream("0014".getBytes()));
        visitor.applyVersion(in);
    }
}
