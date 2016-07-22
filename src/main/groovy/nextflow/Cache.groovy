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

import com.google.common.hash.HashCode
import groovy.transform.CompileStatic
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
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
@Slf4j
@CompileStatic
class Cache implements Closeable {

    private DB db

    private Session session

    Cache(Session session) {
        this.session=session
    }


    Cache open() {
        // create an unique DB path
        def path = session.workDir.resolve('.cache').resolve( session.uniqueId.toString() )
        path.mkdirs()
        // open a LevelDB instance
        db = Iq80DBFactory.factory.open(path.resolve('db').toFile(), new Options().createIfMissing(true))
        return this
    }


    def TaskEntry getTaskEntry(HashCode taskHash, TaskProcessor processor) {

        def payload = db.get(taskHash.asBytes())
        if( !payload )
            return null

        final entry = (List)KryoHelper.deserialize(payload)
        TraceRecord trace = TraceRecord.deserialize( (byte[])entry[0] )
        TaskContext ctx = entry[1]!=null ? TaskContext.deserialize(processor, (byte[])entry[1]) : null

        return new TaskEntry(trace,ctx)
    }

    def putTaskEntry( TaskHandler handler ) {

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
