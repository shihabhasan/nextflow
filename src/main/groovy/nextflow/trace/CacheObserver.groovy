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

package nextflow.trace

import nextflow.Session
import nextflow.processor.TaskHandler
import nextflow.processor.TaskProcessor
import org.iq80.leveldb.Options
import org.iq80.leveldb.impl.Iq80DBFactory

/**
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
class CacheObserver implements TraceObserver {

    private Session session

    private org.iq80.leveldb.DB db

    @Override
    void onFlowStart(Session session) {
        this.session = session
        // create an unique DB path
        def path = session.workDir.resolve('.db').resolve( session.uniqueId.toString() )
        path.mkdirs()
        // open a LevelDB instance
        db = Iq80DBFactory.factory.open(path.resolve('db').toFile(), new Options().createIfMissing(true))

    }

    @Override
    void onFlowComplete() {
        db.close()
    }

    @Override
    void onFlowError(Throwable error) {

    }

    @Override
    void onProcessCreate(TaskProcessor process) {

    }

    @Override
    void onProcessDestroy(TaskProcessor process) {

    }

    @Override
    void onProcessSubmit(TaskHandler handler) {

    }

    @Override
    void onProcessStart(TaskHandler handler) {

    }

    @Override
    void onProcessComplete(TaskHandler handler) {

    }

    @Override
    void onProcessError(TaskHandler handler, Throwable error) {

    }
}
