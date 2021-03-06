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

package nextflow.executor
import java.nio.file.Paths

import nextflow.processor.TaskBean
import spock.lang.Specification
import spock.lang.Unroll

/**
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
class SimpleFileCopyStrategyTest extends Specification {


    def 'should return stage input file script'() {
        setup:
        def files = [:]
        files['file.txt'] = Paths.get('/home/data/sequences')
        files['seq_1.fa'] = Paths.get('/home/data/file1')
        files['seq_2.fa'] = Paths.get('/home/data/file2')
        files['seq 3.fa'] = Paths.get('/home/data/file 3')
        files['sub/dir/seq_4.fa']  = Paths.get("/home'data/file4")

        when:
        def strategy = new SimpleFileCopyStrategy(inputFiles: files)
        def script = strategy.getStageInputFilesScript()
        def lines = script.readLines()

        then:
        lines[0] == "rm -f file.txt"
        lines[1] == "rm -f seq_1.fa"
        lines[2] == "rm -f seq_2.fa"
        lines[3] == "rm -f seq\\ 3.fa"
        lines[4] == "rm -f sub/dir/seq_4.fa"
        lines[5] == "ln -s /home/data/sequences file.txt"
        lines[6] == "ln -s /home/data/file1 seq_1.fa"
        lines[7] == "ln -s /home/data/file2 seq_2.fa"
        lines[8] == "ln -s /home/data/file\\ 3 seq\\ 3.fa"
        lines[9] == "mkdir -p sub/dir && ln -s /home\\'data/file4 sub/dir/seq_4.fa"
        lines.size() == 10


        when:
        files = [:]
        files['file.txt'] = Paths.get('/data/file')
        files['seq_1.fa'] = Paths.get('/data/seq')
        strategy = new SimpleFileCopyStrategy(inputFiles: files, separatorChar: '; ')
        script = strategy.getStageInputFilesScript()
        lines = script.readLines()

        then:
        lines[0] == "rm -f file.txt; rm -f seq_1.fa; ln -s /data/file file.txt; ln -s /data/seq seq_1.fa; "
        lines.size() == 1

    }


    def 'should remove star glob pattern'() {

        given:
        def strategy = [:] as SimpleFileCopyStrategy

        expect:
        strategy.removeGlobStar('a/b/c') == 'a/b/c'
        strategy.removeGlobStar('/a/b/c') == '/a/b/c'
        strategy.removeGlobStar('some/*/path') == 'some/*/path'
        strategy.removeGlobStar('some/**/path') == 'some'
        strategy.removeGlobStar('some/**/path') == 'some'
        strategy.removeGlobStar('some*') == 'some*'
        strategy.removeGlobStar('some**') == '*'

    }

    def 'should normalize path'() {

        given:
        def strategy = [:] as SimpleFileCopyStrategy

        expect:
        strategy.normalizeGlobStarPaths(['file1.txt','path/file2.txt','path/**/file3.txt', 'path/**/file4.txt','**/fa']) == ['file1.txt','path/file2.txt','path','*']
    }

    @Unroll
    def 'should return a valid `cp` command' () {

        given:
        def strategy = [:] as SimpleFileCopyStrategy
        expect:
        strategy.copyCommand(source, target) == result

        where:
        source              | target    | result
        'file.txt'          | '/to/dir' | "cp -fR file.txt /to/dir"
        "file'3.txt"        | '/to dir' | "cp -fR file\\'3.txt /to\\ dir"
        'path_name'         | '/to/dir' | "cp -fR path_name /to/dir"
        'input/file.txt'    | '/to/dir' | "mkdir -p /to/dir/input && cp -fR input/file.txt /to/dir/input"
        'long/path/name'    | '/to/dir' | "mkdir -p /to/dir/long/path && cp -fR long/path/name /to/dir/long/path"
        'path_name/*'       | '/to/dir' | "mkdir -p /to/dir/path_name && cp -fR path_name/* /to/dir/path_name"
        'path_name/'        | '/to/dir' | "mkdir -p /to/dir/path_name && cp -fR path_name/ /to/dir/path_name"

    }

    def 'should return a valid `mv` command' () {

        given:
        def strategy = [:] as SimpleFileCopyStrategy
        expect:
        strategy.copyCommand(source, target, 'move') == result

        where:
        source              | target    | result
        'file.txt'          | '/to/dir' | "mv -f file.txt /to/dir"
        "file'3.txt"        | '/to dir' | "mv -f file\\'3.txt /to\\ dir"
        'path_name'         | '/to/dir' | "mv -f path_name /to/dir"
        'input/file.txt'    | '/to/dir' | "mkdir -p /to/dir/input && mv -f input/file.txt /to/dir/input"
        'long/path/name'    | '/to/dir' | "mkdir -p /to/dir/long/path && mv -f long/path/name /to/dir/long/path"
        'path_name/*'       | '/to/dir' | "mkdir -p /to/dir/path_name && mv -f path_name/* /to/dir/path_name"
        'path_name/'        | '/to/dir' | "mkdir -p /to/dir/path_name && mv -f path_name/ /to/dir/path_name"

    }

    def 'should return a valid `rsync` command' () {

        given:
        def strategy = [:] as SimpleFileCopyStrategy
        expect:
        strategy.copyCommand(source, target, 'rsync') == result

        where:
        source              | target    | result
        'file.txt'          | '/to/dir' | "rsync -rRl file.txt /to/dir"
        "file'3.txt"        | '/to dir' | "rsync -rRl file\\'3.txt /to\\ dir"
        'path_name'         | '/to/dir' | "rsync -rRl path_name /to/dir"
        'input/file.txt'    | '/to/dir' | "rsync -rRl input/file.txt /to/dir"
        'long/path/name'    | '/to/dir' | "rsync -rRl long/path/name /to/dir"
        'path_name/*'       | '/to/dir' | "rsync -rRl path_name/* /to/dir"
        'path_name/'        | '/to/dir' | "rsync -rRl path_name/ /to/dir"

    }

    def 'should return cp script to unstage output files' () {

        given:
        def task = new TaskBean( outputFiles: [ 'simple.txt', 'my/path/file.bam' ], targetDir: Paths.get('/target/work dir'))

        when:
        def strategy = new SimpleFileCopyStrategy(task)
        def script = strategy.getUnstageOutputFilesScript()
        then:
        script == '''
                mkdir -p /target/work\\ dir
                cp -fR simple.txt /target/work\\ dir || true
                mkdir -p /target/work\\ dir/my/path && cp -fR my/path/file.bam /target/work\\ dir/my/path || true
                '''
                .stripIndent().rightTrim()

    }

    def 'should return rsync script to unstage output files' () {

        given:
        def task = new TaskBean( outputFiles: [ 'simple.txt', 'my/path/file.bam' ], targetDir: Paths.get("/target/work's"), unstageStrategy: 'rsync')

        when:
        def strategy = new SimpleFileCopyStrategy(task)
        def script = strategy.getUnstageOutputFilesScript()
        then:
        script == '''
                mkdir -p /target/work\\'s
                rsync -rRl simple.txt /target/work\\'s || true
                rsync -rRl my/path/file.bam /target/work\\'s || true
                '''
                .stripIndent().rightTrim()

    }

}
