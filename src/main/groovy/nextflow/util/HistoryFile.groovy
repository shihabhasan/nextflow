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

package nextflow.util
import java.nio.file.Path
import java.text.DateFormat
import java.text.SimpleDateFormat

import groovy.transform.EqualsAndHashCode
import groovy.transform.PackageScope
import groovy.util.logging.Slf4j
import nextflow.exception.AbortOperationException
/**
 * Manages the history file containing the last 1000 executed commands
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
@Slf4j
class HistoryFile extends File {

    public static final String FILE_NAME = '.nextflow.history'

    public static final HistoryFile DEFAULT = new HistoryFile()

    private static final DateFormat TIMESTAMP_FMT = new SimpleDateFormat('yyyy-MM-dd HH:mm:ss')

    private static final VAL_A = (int)('a' as char)
    private static final VAL_F = (int)('f' as char)
    private static final VAL_0 = (int)('0' as char)
    private static final VAL_9 = (int)('9' as char)

    private HistoryFile() {
        super(FILE_NAME)
    }

    HistoryFile(File file) {
        super(file.toString())
    }

    HistoryFile(Path file) {
        super(file.toString())
    }

    void write( UUID key, String name, args ) {
        assert key
        assert args != null

        def now = TIMESTAMP_FMT.format(new Date())
        def value = args instanceof Collection ? args.join(' ') : args
        this << "${now}\t${name}\t${key.toString()}\t${value}\n"
    }

    Entry getLast() {
        if( !exists() || empty() ) {
            return null
        }

        def line = readLines()[-1]
        try {
            line ? Entry.parse(line) : null
        }
        catch( IllegalArgumentException e ) {
            log.debug e.message
            return null
        }
    }

    void print() {

        if( empty() ) {
            System.err.println '(no history available)'
        }
        else {
            println this.text
        }

    }

    /**
     * Check if a session ID exists in the history file
     *
     * @param uuid A complete UUID string or a prefix of it
     * @return {@code true} if the UUID is found in the history file or {@code false} otherwise
     */
    boolean checkExistsById( String uuid ) {
        findById(uuid)
    }

    boolean checkExistsById( UUID uuid ) {
        findById(uuid.toString())
    }

    boolean checkExistsByName( String name ) {
        try {
            return getByName(name) != null
        }
        catch( AbortOperationException e ) {
            return false
        }
    }

    private Entry checkUnique(List<Entry> results) {
        if( !results )
            return null

        results = results.unique()
        if( results.size()==1 ) {
            return results[0]
        }

        String message = 'Which session ID do you mean?\n'
        results.each { message += "    $it\n" }
        throw new AbortOperationException(message)
    }

    /**
     * Lookup a session ID given a `run` name string
     *
     * @param name A name of a pipeline run
     * @return The session ID string associated to the `run` or {@code null} if it's not found
     */
    Entry getByName(String name) {
        if( !exists() || empty() ) {
            return null
        }

        def results = (List<Entry>)readLines().findResults {  String line ->
            try {
                def current = line ? Entry.parse(line) : null
                if( current?.runName == name )
                    return current
            }
            catch( IllegalArgumentException e ) {
                log.debug e.message
                return null
            }
        }

        checkUnique(results)
    }

    /**
     * Lookup a session ID given a part of it
     *
     * @param id A session ID prefix
     * @return A complete session ID or {@code null} if the specified fragment is not found in the history
     */
    Entry getById(String id) {
        def results = findById(id)
        checkUnique(results)
    }

    List<Entry> findById(String id) {
        if( !exists() || empty() ) {
            return null
        }

        def results = (List<Entry>)this.readLines().findResults { String line ->
            try {
                def current = line ? Entry.parse(line) : null
                if( current && current.sessionId.toString().startsWith(id) ) {
                    return current
                }
            }
            catch( IllegalArgumentException e ) {
                log.debug e.message
                return null
            }
        }

        return results
    }

    /**
     * Lookup a session ID given a run name or a
     * @param str
     * @return
     */
    Entry getByIdOrName( String str ) {
        if( str == 'last' )
            return getLast()

        if( isUuidString(str) )
            return getById(str)

        getByName(str)
    }

    List<Entry> findByIdOrName( String str ) {
        if( str == 'last' )
            return [getLast()]

        if( isUuidString(str) )
            return findById(str)

        return [getByName(str)]
    }

    @PackageScope
    static boolean isUuidChar(char ch) {
        if( ch == '-' as char )
            return true

        final x = (ch as int)

        if(  x >= VAL_0 && x <= VAL_9 )
            return true

        if( x >= VAL_A && x <= VAL_F )
            return true

        return false
    }

    static boolean isUuidString(String str) {
        for( int i=0; i<str.size(); i++ )
            if( !isUuidChar(str.charAt(i)))
                return false
        return true
    }

    List<Entry> findAll() {
        if( !exists() || empty() ) {
            return Collections.emptyList()
        }

        def results = this.readLines().findResults {  String line ->
            try {
                line ? Entry.parse(line) : null
            }
            catch( IllegalArgumentException e ) {
                log.debug e.message
                return null
            }
        }

        return results.unique()
    }

    List<Entry> findBefore(String idOrName) {
        def matching = findByIdOrName(idOrName)
        if( !matching )
            return Collections.emptyList()

        def firstMatch = false

        return findAll().findResults {
            if( it==matching[0] ) {
                firstMatch = true
                return null
            }

            !firstMatch ? it : null
        }
    }

    List<Entry> findAfter(String idOrName) {
        def matching = findByIdOrName(idOrName)
        if( !matching )
            return Collections.emptyList()

        def firstMatch = false
        return findAll().findResults {
            if( it==matching[-1] ) {
                firstMatch = true
                return null
            }

            firstMatch ? it : null
        }
    }

    List<Entry> findBut(String idOrName) {
        final matching = findByIdOrName(idOrName)
        final result = findAll()
        result.removeAll(matching)
        return result
    }

    void eachRow(Closure action) {
        this.eachLine { String line ->
            def cols = line.tokenize('\t')
            if( cols.size() == 2 )
                cols = ['-', '-', cols[0], cols[1]]
            action.call(cols)
        }
    }

    def deleteEntry(Entry entry) {
        def newHistory = new StringBuilder()

        this.readLines().each { line ->
            try {
                def current = line ? Entry.parse(line) : null
                if( current != entry ) {
                    newHistory << line << '\n'
                }
            }
            catch( IllegalArgumentException e ) {
                log.debug e.message
            }
        }

        // rewrite the history content
        this.setText(newHistory.toString())
    }

    @EqualsAndHashCode
    static class Entry {
        UUID sessionId
        String runName

        Entry(String sessionId, String name=null) {
            this.runName = name
            this.sessionId = UUID.fromString(sessionId)
        }

        Entry(UUID sessionId, String name=null) {
            this.runName = name
            this.sessionId = sessionId
        }

        String toString() {
            runName? "$sessionId -- $runName" : sessionId.toString()
        }

        static Entry parse(String line) {
            def cols = line.tokenize('\t')
            if( cols.size() == 2 )
                return new Entry(cols[0])

            if( cols.size()>2 )
                return new Entry(cols[2], cols[1])

            throw new IllegalArgumentException("Not a valid history entry: `$line`")
        }
    }
}
