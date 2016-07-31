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
import nextflow.exception.AbortOperationException
import nextflow.processor.TaskContext
import nextflow.processor.TaskEntry
import nextflow.processor.TaskHandler
import nextflow.processor.TaskProcessor
import nextflow.trace.TraceRecord
import nextflow.util.CacheHelper
import nextflow.util.HistoryFile.Entry
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

    /** The underlying Level DB instance */
    private DB db

    /** The session UUID */
    private UUID uniqueId

    /** The unique run name associated to this cache instance */
    private String runName

    /** The base folder against which the cache is located. Default: current working directory  */
    private Path baseDir

    /** The actual path where DB and index file are located */
    private Path dataDir

    /** An agent used to apply asynchronously DB write operations */
    private Agent writer

    /** The path to the index file */
    private Path indexFile

    /** Index file read/write handle */
    private RandomAccessFile indexHandle

    private final int KEY_SIZE

    Cache(Entry entry, Path home=null) {
        this(entry.sessionId, entry.runName, home)
    }

    Cache(UUID uniqueId, String runName, Path home=null) {
        if( !uniqueId ) throw new AbortOperationException("Missing cache `uuid`")
        if( !runName ) throw new AbortOperationException("Missing cache `runName`")

        this.KEY_SIZE = CacheHelper.hasher('x').hash().asBytes().size()
        this.uniqueId = uniqueId
        this.runName = runName
        this.baseDir = home ?: Paths.get('.').toAbsolutePath()
        this.dataDir = baseDir.resolve(".cache/$uniqueId")
        this.indexFile = dataDir.resolve("index.$runName")
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
        indexFile.delete()
        indexHandle = new RandomAccessFile(indexFile.toFile(), 'rw')
        return this
    }

    /**
     * Open the database in read mode
     *
     * @return The {@link Cache} instance itself
     */
    Cache openForRead() {
        openDb()
        if( !indexFile.exists() )
            throw new AbortOperationException("Missing cache index file: $indexFile")
        indexHandle = new RandomAccessFile(indexFile.toFile(), 'r')
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

    void incTaskEntry( HashCode hash ) {
        final key = hash.asBytes()
        def payload = db.get(key)
        if( !payload ) {
            log.debug "Can't increment reference for cached task with key: $hash"
            return
        }

        final record = (List)KryoHelper.deserialize(payload)
        // third record contains the reference count for this record
        record[2] = ((Integer)record[2]) +1
        // save it again
        db.put(key, KryoHelper.serialize(record))

    }

    boolean removeTaskEntry( HashCode hash ) {
        final key = hash.asBytes()
        def payload = db.get(key)
        if( !payload ) {
            log.debug "Can't increment reference for cached task with key: $hash"
            return
        }

        final record = (List)KryoHelper.deserialize(payload)
        // third record contains the reference count for this record
        def count = record[2] = ((Integer)record[2]) -1
        // save or delete
        if( count > 0 ) {
            db.put(key, KryoHelper.serialize(record))
            return false
        }
        else {
            db.delete(key)
            return true
        }
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

        def record = new ArrayList(3)
        record[0] = trace.serialize()
        record[1] = ctx != null ? ctx.serialize() : null
        record[2] = 1

        // -- save in the db
        db.put( key, KryoHelper.serialize(record) )

    }

    void putTaskAsync( TaskHandler handler ) {
        writer.send { putTaskEntry(handler) }
    }

    void cacheTaskAsync( TaskHandler handler ) {
        writer.send {
            writeTaskIndex0(handler,true)
            incTaskEntry(handler.task.hash)
        }
    }

    void putIndexAsync(TaskHandler handler ) {
        writer.send { writeTaskIndex0(handler) }
    }

    private void writeTaskIndex0( TaskHandler handler, boolean cached = false ) {
        indexHandle.write(handler.task.hash.asBytes())
        indexHandle.writeBoolean(cached)
    }

    void dropIndex( ) {
        indexFile.delete()
    }

    void drop() {
        dataDir.deleteDir()
    }

    /**
     * Iterate the tasks cache using the index file
     * @param closure The operation to applied
     * @return The {@link Cache} instance itself
     */
    Cache eachRecord( Closure closure ) {
        assert closure

        def key = new byte[KEY_SIZE]
        while( indexHandle.read(key) != -1) {
            final cached = indexHandle.readBoolean()

            final payload = (byte[])db.get(key)
            if( !payload ) {
                log.trace "Unable to retrieve cache record for key: ${-> HashCode.fromBytes(key)}"
                continue
            }

            final record = (List<byte[]>)KryoHelper.deserialize(payload)
            TraceRecord trace = TraceRecord.deserialize(record[0])
            trace.setCached(cached)

            final refCount = record[2] as Integer

            final len=closure.maximumNumberOfParameters
            if( len==1 )
                closure.call(trace)

            else if( len==2 )
                closure.call(HashCode.fromBytes(key), trace)

            else if( len==3 )
                closure.call(HashCode.fromBytes(key), trace, refCount)

            else
                throw new IllegalArgumentException("Invalid closure signature -- Too many parameters")

        }

        return this
    }

    boolean isEmpty() {
        !db.iterator().hasNext()
    }

    /**
     * Close the underlying database and index file
     */
    @Override
    void close() {
        writer.await()
        indexHandle.closeQuietly()
        db.closeQuietly()
    }
}
