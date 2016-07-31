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

import groovy.util.logging.Slf4j
/**
 * Manages the history file containing the last 1000 executed commands
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
@Slf4j
class HistoryFile extends File {

    final static HistoryFile history = new HistoryFile()

    private HistoryFile() {
        super('.nextflow.history')
    }

    void write( UUID key, args ) {
        assert key
        assert args != null

        def value = args instanceof Collection ? args.join(' ') : args
        this << "${key.toString()}\t${value}\n"
    }

    String retrieveLastUniqueId() {
        if( !exists() || empty() ) {
            return null
        }

        def lines = readLines()
        def lastLine = lines.get(lines.size()-1)
        def splits = lastLine.split(/\t/)
        return splits.size()>0 ? splits[0] : null
    }

    void print() {

        if( empty() ) {
            log.info '(no history available)'
        }
        else {
            println this.text
        }

    }

    boolean findUniqueId( String uuid ) {
        try {
            readLines().find { String line -> line.startsWith(uuid) }
        }
        catch( FileNotFoundException e ) {
            log.debug "File not found: $this"
            return false
        }
    }


}
