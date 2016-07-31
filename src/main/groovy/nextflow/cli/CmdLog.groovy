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
import com.beust.jcommander.Parameters
import com.google.common.hash.HashCode
import groovy.text.Template
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import nextflow.exception.AbortOperationException
import nextflow.file.FileHelper
import nextflow.processor.TaskRun
import nextflow.processor.TaskTemplateEngine
import nextflow.trace.TraceRecord
import nextflow.ui.TableBuilder
/**
 * Implements the `log` command to print tasks runtime information of an execute pipeline
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
@Slf4j
@CompileStatic
@Parameters(commandDescription = "Print executions log and runtime info")
class CmdLog extends CmdBase implements CacheBase {

    static private int MAX_LINES = 100

    static private List<String> ALL_FIELDS

    static {
        ALL_FIELDS = []
        ALL_FIELDS.addAll( TraceRecord.FIELDS.keySet() )
        ALL_FIELDS << 'stdour'
        ALL_FIELDS << 'stderr'
        ALL_FIELDS << 'log'
        ALL_FIELDS << 'env'
        ALL_FIELDS.sort(true)
    }

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

    private boolean showHistory

    private Template templateScript

    @Override
    final String getName() { NAME }


    void init() {
        CacheBase.super.init()

        //
        // validate input options
        //
        if( fields && templateStr )
            throw new AbortOperationException("Options `fields` and `template` cannot be used in the same command")

//        if( (fields || templateStr) && !args )
//            throw new AbortOperationException("You need to specify a run name or session id")

        showHistory = !args && !before && !after && !but

        //
        // initialise template engine and filters
        //
        if( filterStr ) {
            filterScript = new Grengine().create("{ it -> $filterStr }")
        }

        if( !templateStr ) {
            templateStr = fields.tokenize(',  \n').collect { '$'+it } .join(sep)
        }
        else if( new File(templateStr).exists() ) {
            templateStr = new File(templateStr).text
        }

        templateScript = new TaskTemplateEngine().createTemplate(templateStr)
    }

    /**
     * Implements the `log` command
     */
    @Override
    void run() {
        init()

        // -- show the list of expected fields and exit
        if( listFields ) {
            ALL_FIELDS.each { println "  $it" }
            return
        }

        // -- show the current history and exit
        if( showHistory ) {
            printHistory()
            return
        }

        // -- main
        listIds().each { entry ->

            // -- go
            cacheFor(entry)
                        .openForRead()
                        .eachRecord(this.&printRecord)
                        .close()

        }

    }

    private Map<HashCode,Boolean> printed = new HashMap<>()

    /**
     * Print a log {@link TraceRecord} the the standard output by using the specified {@link #templateStr}
     *
     * @param record A {@link TraceRecord} instance representing a task runtime information
     */
    protected void printRecord(HashCode hash, TraceRecord record) {

        if( printed.containsKey(hash) )
            return
        else
            printed.put(hash,Boolean.TRUE)


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
            def folder = (String)record.get(TraceRecord.FOLDER)
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
