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

package nextflow.cli

import com.beust.jcommander.Parameter
import com.beust.jcommander.Parameters
import com.google.common.hash.HashCode
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import nextflow.Cache
import nextflow.exception.AbortOperationException
import nextflow.file.FileHelper
import nextflow.trace.TraceRecord
import nextflow.util.HistoryFile.Entry
import org.iq80.leveldb.DBException
/**
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
@Slf4j
@CompileStatic
@Parameters(commandDescription = "Clean up project cache and work directory")
class CmdClean extends CmdBase implements CacheBase {

    static final NAME = 'clean'

    @Parameter(names=['-q', '-quiet'], description = 'Do not print names of files removed', arity = 0)
    boolean quiet

    @Parameter(names=['-f', '-force'], description = 'Force clean command', arity = 0)
    boolean force

    @Parameter(names=['-n', '-dry-run'], description = 'Print names of file to be removed without deleting them' , arity = 0)
    boolean dryRun

    @Parameter(names='-after', description = 'Clean up runs executed after the specified one')
    String after

    @Parameter(names='-before', description = 'Clean up runs executed before the specified one')
    String before

    @Parameter(names='-but', description = 'Clean up all runs except the specified one')
    String but

    @Parameter
    List<String> args


    private Cache currentCache

    @Override
    String getName() {
        return NAME
    }

    /**
     * Command entry method
     */
    @Override
    void run() {
        init()
        validateOptions()

        listIds().each { entry -> cleanupCache(entry)}

    }

    private void validateOptions() {

        if( !dryRun && !force )
            throw new AbortOperationException("Neither -f or -n specified -- refused to clean")
    }


    private void cleanupCache(Entry entry) {
        currentCache = cacheFor(entry).openForRead()
        // -- remove each entry and work dir
        currentCache.eachRecord(this.&removeRecord)
        // -- close the cache
        currentCache.close()

        // -- STOP HERE !
        if( dryRun ) return

        // -- remove the index file
        currentCache.dropIndex()
        // -- remove the session from the history file
        history.deleteEntry(entry)
        // -- check if exists another history entry for the same session
        if( !history.checkExistsById(entry.sessionId)) {
            currentCache.drop()
        }

    }

   private void removeRecord(HashCode hash, TraceRecord record) {
        if( dryRun ) {
            println "World remove ${record.workDir}"
            return
        }

        try {
            def folder = FileHelper.asPath(record.workDir)

            // delete folder
            folder.deleteDir()
            if( !quiet )
                println "Removed ${record.workDir}"

            // remove the entry in the db
            currentCache.removeTaskEntry(hash)
        }
        catch(IOException e) {
            log.warn "Unable to delete: ${record.workDir} -- Cause: ${e.message ?: e}"
        }
        catch( DBException e ) {
            log.debug "Failed to remove entry from cache db: $hash -- Cause: ${e.message ?: e}"
        }


    }

}
