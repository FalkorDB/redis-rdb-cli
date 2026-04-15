/*
 * Copyright 2018-2019 Baoyi Chen
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.moilioncircle.redis.rdb.cli.glossary;

import static com.moilioncircle.redis.rdb.cli.util.Collections.isEmpty;
import static com.moilioncircle.redis.replicator.FileType.RDB;

import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

import com.moilioncircle.redis.rdb.cli.cmd.Args;
import com.moilioncircle.redis.rdb.cli.conf.Configure;
import com.moilioncircle.redis.rdb.cli.ext.XRedisReplicator;
import com.moilioncircle.redis.rdb.cli.ext.rdt.BackupRdbVisitor;
import com.moilioncircle.redis.rdb.cli.ext.rdt.MergeRdbVisitor;
import com.moilioncircle.redis.rdb.cli.ext.rdt.SplitRdbVisitor;
import com.moilioncircle.redis.rdb.cli.io.FilesOutputStream;
import com.moilioncircle.redis.rdb.cli.util.Outputs;
import com.moilioncircle.redis.rdb.cli.util.Strings;
import com.moilioncircle.redis.rdb.cli.util.XUris;
import com.moilioncircle.redis.replicator.DefaultReplFilter;
import com.moilioncircle.redis.replicator.RedisURI;
import com.moilioncircle.redis.replicator.Replicator;
import com.moilioncircle.redis.replicator.io.CRCOutputStream;
import com.moilioncircle.redis.replicator.io.RedisInputStream;
import com.moilioncircle.redis.replicator.rdb.BaseRdbEncoder;
import com.moilioncircle.redis.replicator.rdb.BaseRdbParser;
import com.moilioncircle.redis.replicator.util.ByteArray;
import com.moilioncircle.redis.replicator.util.Tuples;
import com.moilioncircle.redis.replicator.util.type.Tuple2;

/**
 * @author Baoyi Chen
 */
public enum Action {
    NONE,
    SPLIT,
    MERGE,
    BACKUP;
    
    public List<Tuple2<Replicator, String>> dress(Configure configure, Args.RdtArgs arg) throws Exception {
        List<Tuple2<Replicator, String>> list = new ArrayList<>();
        switch (this) {
            case MERGE:
                if (isEmpty(arg.merge)) return list;
                CRCOutputStream out = Outputs.newCRCOutput(arg.output, configure.getOutputBufferSize());
                int version = 0;
                long totalUsedMem = 0;
                for (File file : arg.merge) {
                    RedisURI uri = XUris.fromFile(file);
                    if (uri.getFileType() == null || uri.getFileType() != RDB) {
                        throw new UnsupportedOperationException("Invalid options: '--merge <file>...' must be rdb file.");
                    }
                    
                    version = maxVersion(version, file);
                    totalUsedMem += scanUsedMem(file);
                    
                    Replicator r = new XRedisReplicator(uri, configure, DefaultReplFilter.RDB);
                    r.setRdbVisitor(new MergeRdbVisitor(r, configure, arg, () -> out));
                    
                    list.add(Tuples.of(r, file.getName()));
                }
                
                list.get(list.size() - 1).getV1().addCloseListener(r -> {
                    Outputs.writeQuietly(0xFF, out);
                    Outputs.writeQuietly(out.getCRC64(), out);
                    Outputs.closeQuietly(out);
                });
                
                // header & version
                out.write("REDIS".getBytes());
                out.write(Strings.lappend(version, 4, '0').getBytes());
                if (totalUsedMem > 0) {
                    writeUsedMemAux(out, totalUsedMem);
                }
                return list;
            case SPLIT:
                Replicator r = new XRedisReplicator(arg.split, configure, DefaultReplFilter.RDB);
                List<String> lines = Files.readAllLines(arg.config.toPath());
                r.setRdbVisitor(new SplitRdbVisitor(r, configure, arg, () -> new FilesOutputStream(arg.output, lines, configure)));
                list.add(Tuples.of(r, null));
                return list;
            case BACKUP:
                r = new XRedisReplicator(arg.backup, configure, DefaultReplFilter.RDB);
                r.setRdbVisitor(new BackupRdbVisitor(r, configure, arg, () -> Outputs.newCRCOutput(arg.output, configure.getOutputBufferSize())));
                list.add(Tuples.of(r, null));
                return list;
            case NONE:
                return list;
            default:
                throw new AssertionError("Unsupported action '" + this + "'");
        }
    }
    
    private long scanUsedMem(File file) {
        try (RedisInputStream in = new RedisInputStream(new FileInputStream(file))) {
            in.skip(9); // skip "REDIS" magic (5 bytes) + version (4 bytes)
            BaseRdbParser parser = new BaseRdbParser(in);
            while (true) {
                int opcode = in.read();
                if (opcode != 0xFA) break; // not an aux field opcode
                byte[] key = parser.rdbLoadEncodedStringObject().first();
                byte[] value = parser.rdbLoadEncodedStringObject().first();
                if ("used-mem".equals(new String(key, StandardCharsets.UTF_8))) {
                    return Long.parseLong(new String(value, StandardCharsets.UTF_8));
                }
            }
        } catch (Exception ignore) {
        }
        return 0L;
    }
    
    private void writeUsedMemAux(OutputStream out, long usedMem) throws IOException {
        BaseRdbEncoder encoder = new BaseRdbEncoder();
        byte[] keyBytes = "used-mem".getBytes();
        byte[] valueBytes = Long.toString(usedMem).getBytes();
        out.write(0xFA); // RDB_OPCODE_AUX
        encoder.rdbSavePlainStringObject(new ByteArray(keyBytes), out);
        encoder.rdbSavePlainStringObject(new ByteArray(valueBytes), out);
    }
    
    private int maxVersion(int version, File file) throws IOException {
        try (RedisInputStream in = new RedisInputStream(new FileInputStream(file))) {
            in.skip(5); // skip REDIS
            return Math.max(version, Integer.parseInt(in.readString(4)));
        } catch (EOFException e) {
            return version;
        }
    }
}
