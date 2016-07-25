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
import ch.grengine.Grengine
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

    private TemplateEngine engine

    private Script filterScript

    private HistoryFile history

    @Override
    final String getName() { NAME }


    private void validateOptions() {

        if( !history ) {
            history = HistoryFile.DEFAULT
        }

        if( !history.exists() || history.empty() )
            throw new AbortOperationException("It looks no pipeline was executed in this folder (or execution history has been deleted)")

        if( fields && template )
            throw new AbortOperationException("Options `fields` and `template` cannot be used in the same command")

        if( after && before )
            throw new AbortOperationException("Options `after` and `before` cannot be used in the same command")

        if( after && but )
            throw new AbortOperationException("Options `after` and `but` cannot be used in the same command")

        if( before && but )
            throw new AbortOperationException("Options `before` and `but` cannot be used in the same command")
    }

    private void initialize() {

        // -- initialize the filter engine
        if( filterStr ) {
            filterScript = new Grengine().create("{ it -> $filterStr }")
        }

        // -- initialize the template engine
        engine = new SimpleTemplateEngine()
        if( !template ) {
            template = fields.tokenize(',  \n').collect { '$'+it } .join(sep)
        }
        else if( new File(template).exists() ) {
            template = new File(template).text
        }
    }

    private List<String> getIds() {

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
        [history.findBy(args ? args[0] : 'last')]
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

        List<String> allIds = getIds()

        // -- main
        allIds.each { sessionId ->

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

    }

    /**
     * Print a log {@link TraceRecord} the the standard output by using the specified {@link #template}
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

        println engine
                .createTemplate(template)
                .make(adaptor)
                .toString()
    }

    /**
     * Wrap a {@link TraceRecord} instance as a {@link Map}
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
            return record.getFmtStr(key.toString())
        }


        Object getVariable(String name) {
            if( record.containsKey(name) )
                return record.getFmtStr(name)

            throw new MissingPropertyException(name)
        }

        Map getVariables() {
            def result = [:]
            record.store.keySet().each { k ->
                result.put(k, record.getFmtStr(k))
            }
            return result
        }

    }
}
