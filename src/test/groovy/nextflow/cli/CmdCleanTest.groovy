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

import spock.lang.Specification

/**
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
class CmdCleanTest extends Specification {

    static final UUID = java.util.UUID.randomUUID().toString()

    def 'should return the run name' () {

        given:
        def cmd = new CmdClean()

        when:
        cmd.but = but
        cmd.before = before
        cmd.after = after
        then:
        cmd.runName == expected

        where:
        but             | before        | after         | expected
        null            | null          | null          | null
        'hello_world'   | null          | null          | 'hello_world'
        UUID            | null          | null          | null
        null            | 'ciao_mondo'  | null          | 'ciao_mondo'
        null            | UUID          | null          | null
        null            | null          | 'some_str'    | 'some_str'
        null            | null          | UUID          | null

    }

}
