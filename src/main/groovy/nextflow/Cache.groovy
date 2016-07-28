/*
 * Copyright (c) 2013-2016, Centre for Genomic Regulation (CRG).
 * Copyright (c) 2013-2016, Paolo Di Tommaso and the respective authors.
 *
 *   This file is part of 'Nextflow'.
 *
 *   Nextflow is free software: you can redistribute it and/or modify
 *   it under the terms of the GNU General Public License as published by
 *   the Free Software Foundation, either version 3 of the License, or
 *   (at your option) any later version.
 *
 *   Nextflow is distributed in the hope that it will be useful,
 *   but WITHOUT ANY WARRANTY; without even the implied warranty of
 *   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *   GNU General Public License for more details.
 *
 *   You should have received a copy of the GNU General Public License
 *   along with Nextflow.  If not, see <http://www.gnu.org/licenses/>.
 */

package nextflow
import java.nio.file.Path
import java.nio.file.Paths

import com.google.common.hash.HashCode
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import groovyx.gpars.agent.Agent
import nextflow.processor.TaskContext
import nextflow.processor.TaskEntry
import nextflow.processor.TaskHandler
import nextflow.processor.TaskProcessor
import nextflow.trace.TraceRecord
import nextflow.util.CacheHelper
import nextflow.util.KryoHelper
import org.iq80.leveldb.DB
import org.iq80.leveldb.Options
import org.iq80.leveldb.impl.Iq80DBFactory
/**
 * Manages nextflow cache DB
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
@Slf4j
@CompileStatic
class Cache implements Closeable {

    private DB db

    private UUID uniqueId

    private Path baseDir

    private Path dataDir

    private Agent writer

    private RandomAccessFile index

    private final int KEY_SIZE


    Cache(UUID uniqueId) {
        this(uniqueId, Paths.get('.'))
    }

    /** Only for test purpose */
    Cache(UUID uniqueId, Path home) {
        this.KEY_SIZE = CacheHelper.hasher('x').hash().asBytes().size()
        this.uniqueId = uniqueId
        this.baseDir = home
        this.dataDir = baseDir.resolve(".cache/${uniqueId.toString()}")
        this.writer = new Agent()
    }

    private void openDb() {
        // make sure the db path exists
        dataDir.mkdirs()
        // open a LevelDB instance
        db = Iq80DBFactory.factory.open(dataDir.resolve('db').toFile(), new Options().createIfMissing(true))
    }

    /**
     * Initialise the database structure on the underlying file system
     *
     * @return The {@link Cache} instance itself
     */
    Cache open() {
        openDb()
        def file = dataDir.resolve('index').toFile(); file.delete()
        index = new RandomAccessFile(file, 'rw')
        return this
    }

    Cache openForRead() {
        openDb()
        def file = dataDir.resolve('index').toFile()
        index = new RandomAccessFile(file, 'r')
        return this
    }

    /**
     * Retrieve a task runtime information from the cache DB
     *
     * @param taskHash The {@link HashCode} of the task to retrieve
     * @param processor The {@link TaskProcessor} instance to be assigned to the retrieved task
     * @return A {link TaskEntry} instance or {@code null} if a task for the given hash does not exist
     */
    TaskEntry getTaskEntry(HashCode taskHash, TaskProcessor processor) {

        def payload = db.get(taskHash.asBytes())
        if( !payload )
            return null

        final record = (List)KryoHelper.deserialize(payload)
        TraceRecord trace = TraceRecord.deserialize( (byte[])record[0] )
        TaskContext ctx = record[1]!=null ? TaskContext.deserialize(processor, (byte[])record[1]) : null

        return new TaskEntry(trace,ctx)
    }

    /**
     * Save task runtime information to th cache DB
     *
     * @param handler A {@link TaskHandler} instance
     */
    void putTaskEntry( TaskHandler handler ) {

        final task = handler.task
        final proc = task.processor
        final key = task.hash.asBytes()

        final trace = handler.getTraceRecord()
        // save the context map for caching purpose
        // only the 'cache' is active and
        TaskContext ctx = proc.isCacheable() && task.hasCacheableValues() ? task.context : null

        def entry = new ArrayList(2)
        entry.add( trace.serialize() )
        entry.add( ctx != null ? ctx.serialize() : null )

        // -- save in the db
        db.put( key, KryoHelper.serialize(entry) )
    }

    void putTaskAsync( TaskHandler handler ) {
        writer.send { putTaskEntry(handler) }
    }

    void putTaskIndex( TaskHandler handler ) {
        writer.send { index.write(handler.task.hash.asBytes()) }
    }

    Cache eachRecord( Closure closure ) {
        assert closure

        def key = new byte[KEY_SIZE]
        while( index.read(key) != -1) {

            final payload = (byte[])db.get(key)
            if( !payload ) {
                log.trace "Unable to retrieve cache record for key: ${-> HashCode.fromBytes(key)}"
                continue
            }

            final record = (List<byte[]>)KryoHelper.deserialize(payload)
            TraceRecord trace = TraceRecord.deserialize(record[0])

            if( closure.maximumNumberOfParameters==2 )
                closure.call(HashCode.fromBytes(key), trace)
            else
                closure.call(trace)
        }

        return this
    }

    /**
     * Close the underlying database
     */
    @Override
    void close() {
        writer.await()
        index.closeQuietly()
        db.closeQuietly()
    }
}
