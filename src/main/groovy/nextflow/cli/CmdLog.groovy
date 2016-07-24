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
import groovy.text.SimpleTemplateEngine
import groovy.text.TemplateEngine
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import nextflow.Cache
import nextflow.exception.AbortOperationException
import nextflow.trace.TraceRecord
import nextflow.util.HistoryFile
/**
 * Implements the `log` command to print tasks runtime information of an execute pipeline
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
@Slf4j
@CompileStatic
class CmdLog extends CmdBase {

    static final NAME = 'log'

    @Parameter(names = ['-s'], description='Character used to separate column values')
    String sep = '\t'

    @Parameter(names=['-f','-fields'], description = 'Comma separated list of fields to include in the printed log -- Use the `-l` option to show the list of available fields')
    String fields = 'folder'

    @Parameter(names = ['-t','-template'], description = 'Text template used to each record in the log ')
    String template

    @Parameter(names=['-l','-list-fields'], description = 'Show all available fields', arity = 0)
    boolean listFields

    @Parameter
    List<String> args


    private TemplateEngine engine

    @Override
    final String getName() { NAME }

    /**
     * Implements the `log` command
     */
    @Override
    void run() {
        // cannot be specified both `fields` and `template` options
        if( fields && template )
            throw new AbortOperationException("Options `fields` and `template` cannot be used in the same command")

        // -- show the list of expected fields and exit
        if( listFields ) {
            TraceRecord.FIELDS.keySet().sort().each { println " $it" }
            return
        }

        // -- get the session ID from the command line if specified or retrieve from
        def sessionId = args ? args[0] : HistoryFile.history.retrieveLastUniqueId()
        if( !sessionId ) {
            log.info "It looks no pipeline was executed in this folder (or execution history has been deleted)"
            return
        }

        // -- initialize the template engine
        engine = new SimpleTemplateEngine()
        if( !template ) {
            template = fields.tokenize(',  \n').collect { '$'+it } .join(sep)
        }
        else if( new File(template).exists() ) {
            template = new File(template).text
        }

        // -- convert the session ID string to a UUID
        def uuid
        try {
            uuid = UUID.fromString(sessionId)
        }
        catch( IllegalArgumentException e ) {
            throw new AbortOperationException("Not a valid nextflow session ID: $sessionId -- It must be 128-bit UUID formatted string", e)
        }

        // -- go
        new Cache(uuid)
                .open()
                .eachRecord(this.&printRecord)
                .close()

    }

    /**
     * Print a log {@link TraceRecord} the the standard output by using the specified {@link #template}
     *
     * @param record A {@link TraceRecord} instance representing a task runtime information
     */
    protected void printRecord(TraceRecord record) {
        println engine
                .createTemplate(template)
                .make(new TraceAdaptor(record))
                .toString()
    }

    /**
     * Wrap a {@link TraceRecord} instance as a {@link Map}
     */
    private static class TraceAdaptor {

        private TraceRecord record

        @Delegate
        private Map<String,Object> delegate = [:]

        TraceAdaptor(TraceRecord record) {
            this.record = record
        }

        @Override
        boolean containsKey(Object key) {
            delegate.containsKey(key.toString()) || record.containsKey(key.toString())
        }

        @Override
        Object get(Object key) {
            if( delegate.containsKey(key) ) {
                return delegate.get(key)
            }
            return record.getFmtStr(key.toString())
        }

    }
}
