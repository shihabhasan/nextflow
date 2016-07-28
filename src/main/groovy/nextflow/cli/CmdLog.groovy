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
import java.nio.file.Path

import ch.grengine.Grengine
import com.beust.jcommander.Parameter
import groovy.text.Template
import groovy.transform.CompileStatic
import groovy.transform.PackageScope
import groovy.util.logging.Slf4j
import nextflow.Cache
import nextflow.exception.AbortOperationException
import nextflow.file.FileHelper
import nextflow.processor.TaskRun
import nextflow.processor.TaskTemplateEngine
import nextflow.trace.TraceRecord
import nextflow.ui.TableBuilder
import nextflow.util.HistoryFile
/**
 * Implements the `log` command to print tasks runtime information of an execute pipeline
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
@Slf4j
@CompileStatic
class CmdLog extends CmdBase {

    static int MAX_LINES = 100

    /**
     * Only for testing purpose
     */
    @PackageScope
    Path basePath

    static final NAME = 'log'

    @Parameter(names = ['-s'], description='Character used to separate column values')
    String sep = '\t'

    @Parameter(names=['-f','-fields'], description = 'Comma separated list of fields to include in the printed log -- Use the `-l` option to show the list of available fields')
    String fields = 'folder'

    @Parameter(names = ['-t','-template'], description = 'Text template used to each record in the log ')
    String templateStr

    @Parameter(names=['-l','-list-fields'], description = 'Show all available fields', arity = 0)
    boolean listFields

    @Parameter(names=['-F','-filter'], description = "Filter log entries by a custom expression e.g. process =~ /foo.*/ && status == 'COMPLETED'")
    String filterStr

    @Parameter(names='-after', description = 'Show log entries for runs executed after the specified one')
    String after

    @Parameter(names='-before', description = 'Show log entries for runs executed before the specified one')
    String before

    @Parameter(names='-but', description = 'Show log entries of all runs except the specified one')
    String but

    @Parameter
    List<String> args

    private Script filterScript

    private HistoryFile history

    private boolean showHistory

    private Template templateScript

    @Override
    final String getName() { NAME }


    private void validateOptions() {

        if( !history ) {
            history = !basePath ? HistoryFile.DEFAULT : new HistoryFile(basePath.resolve(HistoryFile.FILE_NAME))
        }

        if( !history.exists() || history.empty() )
            throw new AbortOperationException("It looks no pipeline was executed in this folder (or execution history has been deleted)")

        if( fields && templateStr )
            throw new AbortOperationException("Options `fields` and `template` cannot be used in the same command")

        if( after && before )
            throw new AbortOperationException("Options `after` and `before` cannot be used in the same command")

        if( after && but )
            throw new AbortOperationException("Options `after` and `but` cannot be used in the same command")

        if( before && but )
            throw new AbortOperationException("Options `before` and `but` cannot be used in the same command")

        showHistory = !args && !before && !after && !but
    }

    private void initialize() {

        // -- initialize the filter engine
        if( filterStr ) {
            filterScript = new Grengine().create("{ it -> $filterStr }")
        }

        // -- initialize the template engine
        if( !templateStr ) {
            templateStr = fields.tokenize(',  \n').collect { '$'+it } .join(sep)
        }
        else if( new File(templateStr).exists() ) {
            templateStr = new File(templateStr).text
        }

        templateScript = new TaskTemplateEngine().createTemplate(templateStr)
    }

    private List<String> listIds() {

        if( but ) {
            return history.findBut(but)
        }

        if( before ) {
            return history.findBefore(before)
        }

        else if( after ) {
            return history.findBefore(after)
        }

        // -- get the session ID from the command line if specified or retrieve from
        def sessionId = history.findBy(args ? args[0] : 'last')
        sessionId ? [sessionId] : Collections.<String>emptyList()
    }

    /**
     * Implements the `log` command
     */
    @Override
    void run() {
        validateOptions()
        initialize()

        // -- show the list of expected fields and exit
        if( listFields ) {
            TraceRecord.FIELDS.keySet().sort().each { println "  $it" }
            return
        }

        // -- show the current history and exit
        if( showHistory ) {
            printHistory()
            return
        }

        // -- main
        listIds().each { uuid ->

            // -- go
            cacheFor(uuid)
                        .openForRead()
                        .eachRecord(this.&printRecord)
                        .close()

        }

    }

    private Cache cacheFor(String sessionId) {

        // -- convert the session ID string to a UUID
        def uuid
        try {
            uuid = UUID.fromString(sessionId)
        }
        catch( IllegalArgumentException e ) {
            throw new AbortOperationException("Not a valid nextflow session ID: $sessionId -- It must be 128-bit UUID formatted string", e)
        }

        !basePath ? new Cache(uuid) : new Cache(uuid,basePath)
    }

    /**
     * Print a log {@link TraceRecord} the the standard output by using the specified {@link #templateStr}
     *
     * @param record A {@link TraceRecord} instance representing a task runtime information
     */
    protected void printRecord(TraceRecord record) {

        final adaptor = new TraceAdaptor(record)

        if( filterScript ) {
            filterScript.setBinding(adaptor)
            // dynamic execution of the filter statement
            // the `run` method interprets the statement groovy closure
            // then the `call` method invokes the closure which returns a bool value
            // if `false` skip this record
            if( !((Closure)filterScript.run()).call() ) {
                return
            }
        }

        println templateScript.make(adaptor).toString()
    }

    private void printHistory() {
        def table = new TableBuilder(cellSeparator: '\t')
                    .head('TIMESTAMP')
                    .head('RUN NAME')
                    .head('SESSION ID')
                    .head('COMMAND')

        history.eachRow { List row ->
            table.append(row)
        }

        println table.toString()
    }

    /**
     * Wrap a {@link TraceRecord} instance as a {@link Map} or a {@link Binding} object
     */
    private static class TraceAdaptor extends Binding {

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

            if( key == 'stdout' ) {
                return fetch(getWorkDir().resolve(TaskRun.CMD_OUTFILE))
            }

            if( key == 'stderr' ) {
                return fetch(getWorkDir().resolve(TaskRun.CMD_ERRFILE))
            }

            if( key == 'log' ) {
                return fetch(getWorkDir().resolve(TaskRun.CMD_LOG))
            }

            if( key == 'env' ) {
                return fetch(getWorkDir().resolve(TaskRun.CMD_ENV))
            }

            return record.getFmtStr(key.toString())
        }


        Object getVariable(String name) {
            if( record.containsKey(name) )
                return record.store.get(name)

            throw new MissingPropertyException(name)
        }

        Map getVariables() {
            new HashMap(record.store)
        }

        private Path getWorkDir() {
            def folder = (String)record.get('folder')
            folder ? FileHelper.asPath(folder) : null
        }

        private String fetch(Path path) {
            try {
                int c=0
                def result = new StringBuilder()
                path.withReader { reader ->
                    String line
                    while( line=reader.readLine() && c++<MAX_LINES ) {
                        result << line
                    }
                }

                result.toString() ?: TraceRecord.NA

            }
            catch( IOError e ) {
                log.debug "Failed to fetch content for file: $path -- Cause: ${e.message ?: e}"
                return TraceRecord.NA
            }
        }
    }
}
