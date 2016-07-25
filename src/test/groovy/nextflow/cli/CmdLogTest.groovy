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
import java.nio.file.Files

import nextflow.Cache
import nextflow.executor.CachedTaskHandler
import nextflow.processor.ProcessConfig
import nextflow.processor.TaskContext
import nextflow.processor.TaskProcessor
import nextflow.processor.TaskRun
import nextflow.script.TaskBody
import nextflow.trace.TraceRecord
import nextflow.util.CacheHelper
import nextflow.util.HistoryFile
import nextflow.util.LoggerHelper
import org.junit.Rule
import spock.lang.Specification
import test.OutputCapture

/**
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
class CmdLogTest extends Specification {

    /*
     * Read more http://mrhaki.blogspot.com.es/2015/02/spocklight-capture-and-assert-system.html
     */
    @Rule
    OutputCapture capture = new OutputCapture()

    def setupSpec () {
        LoggerHelper.configureLogger(new CliOptions())
    }

    def 'should print the folders for executed tasks' () {

        setup:
        def folder = Files.createTempDirectory('test')
        def uuid = UUID.randomUUID()

        // -- the session object
        def cache = new Cache(uuid, folder)

        // -- the processor mock
        def proc = Mock(TaskProcessor)
        proc.getTaskBody() >> new TaskBody(null,'source')
        proc.getConfig() >> new ProcessConfig([:])

        // -- the task context
        def ctx = new TaskContext()
        ctx.setHolder( [X: 10, Y: 'Hello'] )

        // -- the task mock
        def task1 = Mock(TaskRun)
        task1.getProcessor() >> proc
        task1.getHash() >> { CacheHelper.hasher('x1').hash() }

        def task2 = Mock(TaskRun)
        task2.getProcessor() >> proc
        task2.getHash() >> { CacheHelper.hasher('x2').hash() }

        def task3 = Mock(TaskRun)
        task3.getProcessor() >> proc
        task3.getHash() >> { CacheHelper.hasher('x3').hash() }

        cache.open()
        cache.putTaskEntry(new CachedTaskHandler(task1, new TraceRecord([task_id: 1, process: 'foo', exit: 0, folder:"$folder/aaa"])) )
        cache.putTaskEntry(new CachedTaskHandler(task2, new TraceRecord([task_id: 2, process: 'foo', exit: 1, folder:"$folder/bbb"])) )
        cache.putTaskEntry(new CachedTaskHandler(task3, new TraceRecord([task_id: 3, process: 'bar', exit: 0, folder:"$folder/ccc"])) )
        cache.close()

        def history = new HistoryFile(folder.resolve(HistoryFile.FILE_NAME))
        history.write(uuid,'test_1','run')

        when:
        def log = new CmdLog(basePath: folder)
        log.run()
        def stdout = capture.toString()
        then:
        stdout.readLines().size() == 3
        stdout.readLines().contains( "$folder/aaa" .toString())
        stdout.readLines().contains( "$folder/bbb" .toString())
        stdout.readLines().contains( "$folder/ccc" .toString())

    }

    def 'should filter printed tasks' () {

        setup:
        def folder = Files.createTempDirectory('test')
        def uuid = UUID.randomUUID()

        // -- the session object
        def cache = new Cache(uuid, folder)

        // -- the processor mock
        def proc = Mock(TaskProcessor)
        proc.getTaskBody() >> new TaskBody(null,'source')
        proc.getConfig() >> new ProcessConfig([:])

        // -- the task context
        def ctx = new TaskContext()
        ctx.setHolder( [X: 10, Y: 'Hello'] )

        // -- the task mock
        def task1 = Mock(TaskRun)
        task1.getProcessor() >> proc
        task1.getHash() >> { CacheHelper.hasher('x1').hash() }

        def task2 = Mock(TaskRun)
        task2.getProcessor() >> proc
        task2.getHash() >> { CacheHelper.hasher('x2').hash() }

        def task3 = Mock(TaskRun)
        task3.getProcessor() >> proc
        task3.getHash() >> { CacheHelper.hasher('x3').hash() }

        cache.open()
        cache.putTaskEntry(new CachedTaskHandler(task1, new TraceRecord([task_id: 1, process: 'foo', exit: 0, folder:"$folder/aaa"])) )
        cache.putTaskEntry(new CachedTaskHandler(task2, new TraceRecord([task_id: 2, process: 'foo', exit: 1, folder:"$folder/bbb"])) )
        cache.putTaskEntry(new CachedTaskHandler(task3, new TraceRecord([task_id: 3, process: 'bar', exit: 0, folder:"$folder/ccc"])) )
        cache.close()

        def history = new HistoryFile(folder.resolve(HistoryFile.FILE_NAME))
        history.write(uuid,'test_1','run')


        when:
        def log = new CmdLog(basePath: folder, filterStr: 'exit == 0')
        log.run()

        def stdout = capture.toString()
        then:
        stdout.readLines().size() == 2
        stdout.readLines().contains( "$folder/aaa" .toString())
        stdout.readLines().contains( "$folder/ccc" .toString())

    }

}
