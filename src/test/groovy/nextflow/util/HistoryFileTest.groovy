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

import java.nio.file.Files

import nextflow.exception.AbortOperationException
import spock.lang.Specification
/**
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
class HistoryFileTest extends Specification {

    static String FILE_TEXT = '''
b8a3c4cf-17e4-49c6-a4cf-4fd8ddbeef98\tnextflow run examples/ampa.nf --in data/sample.fa
b8a3c4cf-17e4-49c6-a4cf-4fd8ddbeef98\tnextflow run examples/ampa.nf --in data/sample.fa -resume
58d8dd16-ce77-4507-ba1a-ec1ccc9bd2e8\tnextflow run examples/basic.nf --in data/sample.fa
2016-07-24 16:43:16\tevil_pike\te710da1b-ce06-482f-bbcf-987a507f85d1\t./launch.sh run hello
2016-07-24 16:43:34\tgigantic_keller\t5a6d3877-8823-4ed6-b7fe-2b6748ed4ff9\t./launch.sh run hello
2016-07-25 09:58:01\tmodest_bartik\t5910a50f-8656-4765-aa79-f07cef912062\t./launch.sh run hello
'''

    def 'test add and get and find' () {

        given:
        def file = Files.createTempFile('test',null)
        file.deleteOnExit()

        when:
        def history = new HistoryFile(file)
        then:
        history.findLast() == null

        when:
        def id1 = UUID.randomUUID()
        def id2 = UUID.randomUUID()
        def id3 = UUID.randomUUID()
        history.write( id1, 'hello_world', [1,2,3] )
        history.write( id2, 'super_star', [1,2,3] )
        history.write( id3, 'slow_food', [1,2,3] )

        then:
        history.findLast() == id3.toString()
        history.checkById( id1.toString() )
        history.checkById( id2.toString() )
        history.checkById( id3.toString() )
        !history.checkById( UUID.randomUUID().toString() )

    }


    def 'should return a session ID given a short version of it' () {

        given:
        def file = Files.createTempFile('test',null)
        file.text = FILE_TEXT

        when:
        def history = new HistoryFile(file)
        then:
        history.findById('b8a3c4cf') == 'b8a3c4cf-17e4-49c6-a4cf-4fd8ddbeef98'
        history.findById('b8a3c4cf-17e4-49c6-a4cf-4fd8ddbeef98') == 'b8a3c4cf-17e4-49c6-a4cf-4fd8ddbeef98'
        history.findById('58d8dd16-ce77-4507-ba1a-ec1ccc9bd2e8') == '58d8dd16-ce77-4507-ba1a-ec1ccc9bd2e8'
        history.findById('5a6d3877-8823-4ed6-b7fe-2b6748ed4ff9') == '5a6d3877-8823-4ed6-b7fe-2b6748ed4ff9'
        history.findById('5910a50f') == '5910a50f-8656-4765-aa79-f07cef912062'
        history.findById('5910a50x') == null

        history.checkById('5910a50f')
        !history.checkById('5910a50x')

        when:
        history.findById('5')
        then:
        thrown(AbortOperationException)

        cleanup:
        file?.delete()
    }


    def 'should return a session ID given a run name' () {

        given:
        def file = Files.createTempFile('test',null)
        file.text = FILE_TEXT

        when:
        def history = new HistoryFile(file)
        then:
        history.findByName('lazy_pike') == null
        history.findByName('evil_pike') == 'e710da1b-ce06-482f-bbcf-987a507f85d1'
        history.findByName('gigantic_keller') == '5a6d3877-8823-4ed6-b7fe-2b6748ed4ff9'

        cleanup:
        file?.delete()
    }

    def 'should verify uuid char' () {
        expect:
        HistoryFile.isUuidChar('-' as char)
        HistoryFile.isUuidChar('0' as char)
        HistoryFile.isUuidChar('3' as char)
        HistoryFile.isUuidChar('9' as char)
        HistoryFile.isUuidChar('a' as char)
        HistoryFile.isUuidChar('b' as char)
        HistoryFile.isUuidChar('f' as char)
        !HistoryFile.isUuidChar('q' as char)
        !HistoryFile.isUuidChar('!' as char)
    }

    def 'should verify uuid string' () {

        expect:
        HistoryFile.isUuidString('b')
        HistoryFile.isUuidString('b8a3c4cf')
        HistoryFile.isUuidString('b8a3c4cf-17e4-49c6-a4cf-4fd8ddbeef98')

        !HistoryFile.isUuidString('hello_world')
    }

    def 'should find entries before the specified one' () {

        given:
        def file = Files.createTempFile('test',null)
        file.deleteOnExit()
        file.text = FILE_TEXT
        def history = new HistoryFile(file)

        expect:
        history.findBefore('5a6d3877') == [
                'b8a3c4cf-17e4-49c6-a4cf-4fd8ddbeef98',
                '58d8dd16-ce77-4507-ba1a-ec1ccc9bd2e8',
                'e710da1b-ce06-482f-bbcf-987a507f85d1'
        ]

        history.findBefore('unknown') == []

    }

    def 'should find entries after the specified one' () {

        given:
        def file = Files.createTempFile('test',null)
        file.deleteOnExit()
        file.text = FILE_TEXT
        def history = new HistoryFile(file)

        expect:
        history.findAfter('evil_pike') == [
                '5a6d3877-8823-4ed6-b7fe-2b6748ed4ff9',
                '5910a50f-8656-4765-aa79-f07cef912062',
        ]


        history.findAfter('unknown') == []
    }


    def 'should return all IDs' () {

        given:
        def file = Files.createTempFile('test',null)
        file.deleteOnExit()
        file.text = FILE_TEXT
        def history = new HistoryFile(file)

        expect:
        history.findAll() == [
                'b8a3c4cf-17e4-49c6-a4cf-4fd8ddbeef98',
                '58d8dd16-ce77-4507-ba1a-ec1ccc9bd2e8',
                'e710da1b-ce06-482f-bbcf-987a507f85d1',
                '5a6d3877-8823-4ed6-b7fe-2b6748ed4ff9',
                '5910a50f-8656-4765-aa79-f07cef912062'
        ]
    }

    def 'should find by a session by a name of ID' () {

        given:
        def file = Files.createTempFile('test',null)
        file.deleteOnExit()
        file.text = FILE_TEXT
        def history = new HistoryFile(file)

        expect:
        history.findBy('last') == '5910a50f-8656-4765-aa79-f07cef912062'
        history.findBy('evil_pike') == 'e710da1b-ce06-482f-bbcf-987a507f85d1'
        history.findBy('b8a3c4cf') == 'b8a3c4cf-17e4-49c6-a4cf-4fd8ddbeef98'
        history.findBy('unknown') == null
    }


}
