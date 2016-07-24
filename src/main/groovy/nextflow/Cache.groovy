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
import groovy.transform.PackageScope
import groovy.util.logging.Slf4j
import nextflow.processor.TaskContext
import nextflow.processor.TaskEntry
import nextflow.processor.TaskHandler
import nextflow.processor.TaskProcessor
import nextflow.trace.TraceRecord
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

    Cache(UUID uniqueId) {
        this.uniqueId = uniqueId
        this.baseDir = Paths.get('.')
    }

    /** Only for test purpose */
    @PackageScope
    Cache(UUID uniqueId, Path home) {
        this.uniqueId = uniqueId
        this.baseDir = home
    }

    /**
     * Initialise the database structure on the underlying file system
     *
     * @return The {@link Cache} instance itself
     */
    Cache open() {
        // create an unique DB path
        def path = baseDir.resolve(".nextflow.cache/${uniqueId.toString()}")
        path.mkdirs()
        // open a LevelDB instance
        db = Iq80DBFactory.factory.open(path.resolve('db').toFile(), new Options().createIfMissing(true))
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

    Cache eachRecord( Closure closure ) {

        def itr = db.iterator()
        while( itr.hasNext() ) {
            final entry = itr.next()

            final payload = (byte[])entry.value
            final record = (List<byte[]>)KryoHelper.deserialize(payload)

            TraceRecord trace = TraceRecord.deserialize(record[0])
            closure.call(trace)
        }

        return this
    }

    /**
     * Close the underlying database
     */
    @Override
    void close() {
        try {
            db.close()
        }
        catch( IOException e ) {
            log.debug "Failed to close cache DB -- Cause: ${e.message ?: e}"
        }
    }
}
