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

    public static final HistoryFile DEFAULT = new HistoryFile()

    private static final DateFormat TIMESTAMP_FMT = new SimpleDateFormat('yyyy-MM-dd HH:mm:ss')

    private static final VAL_A = (int)('a' as char)
    private static final VAL_F = (int)('f' as char)
    private static final VAL_0 = (int)('0' as char)
    private static final VAL_9 = (int)('9' as char)

    private HistoryFile() {
        super('.nextflow.history')
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
        this << "${now}\t${key.toString()}\t${name}\t${value}\n"
    }

    String findLast() {
        if( !exists() || empty() ) {
            return null
        }

        def lines = readLines()
        def lastLine = lines.get(lines.size()-1)
        def cols = lastLine.tokenize('\t')
        if( !cols )
            return null

        (
                cols.size() == 2
                ? cols[0]       // legacy format: the first was the session ID
                : cols[1]       // new format: the second is the session ID
        )
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
    boolean checkById( String uuid ) {
        findById(uuid) != null
    }

    private String checkUnique(List<String> results) {
        if( !results )
            return null

        results = results.unique()
        if( results.size()==1 ) {
            return results[0]
        }

        String message = 'Which session ID do you mean?\n'
        results.each { message += '    ' + it + '\n' }
        throw new AbortOperationException(message)
    }

    /**
     * Lookup a session ID given a `run` name string
     *
     * @param name A name of a pipeline run
     * @return The session ID string associated to the `run` or {@code null} if it's not found
     */
    String findByName(String name) {
        if( !exists() || empty() ) {
            return Collections.emptyList()
        }

        def results = readLines().findResults {  String line ->
            def cols = line.tokenize('\t')
            cols.size()>2 && cols[2] == name ? cols[1] : null
        }

        checkUnique(results)
    }

    /**
     * Lookup a session ID given a part of it
     *
     * @param id A session ID prefix
     * @return A complete session ID or {@code null} if the specified fragment is not found in the history
     */
    String findById(String id) {
        if( !exists() || empty() ) {
            return null
        }

        def results = this.readLines().findResults { String line ->
            def cols = line.tokenize('\t')
            if( cols.size() == 2 )
                cols[0].startsWith(id) ? cols[0] : null
            else
                cols.size()>2 && cols[1].startsWith(id)? cols[1] : null
        }

        checkUnique(results)
    }

    /**
     * Lookup a session ID given a run name or a
     * @param str
     * @return
     */
    String findBy( String str ) {
        if( str == 'last' )
            return findLast()

        if( isUuidString(str) )
            return findById(str)

        findByName(str)
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

    @PackageScope
    static boolean isUuidString(String str) {
        for( int i=0; i<str.size(); i++ )
            if( !isUuidChar(str.charAt(i)))
                return false
        return true
    }

    List<String> findAll() {
        if( !exists() || empty() ) {
            return Collections.emptyList()
        }

        def results = []
        this.eachLine {  String line ->
            def cols = line.tokenize('\t')
            if( cols.size() == 2 && !results.contains(cols[0] ))
                results << cols[0]

            else if( cols.size()>2 && !results.contains(cols[1]))
                results << cols[1]
        }

        return results
    }

    List<String> findBefore(String nameOrId) {
        def sessionId = findBy(nameOrId)
        if( !sessionId )
            return Collections.emptyList()

        def firstMatch = false

        return findAll().findResults {
            if( it==sessionId ) {
                firstMatch = true
                return null
            }

            !firstMatch ? it : null
        }
    }

    List<String> findAfter(String nameOrId) {
        def sessionId = findBy(nameOrId)
        def firstMatch = false

        return findAll().findResults {
            if( it==sessionId ) {
                firstMatch = true
                return null
            }

            firstMatch ? it : null
        }
    }

    List<String> findBut(String nameOrId) {
        def sessionId = findBy(nameOrId)
        def result = findAll()
        result?.remove(sessionId)
        return result
    }

}
