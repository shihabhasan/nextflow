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

package nextflow.script

import java.nio.file.Files

import nextflow.Const
import nextflow.Session
import nextflow.scm.AssetManager
import nextflow.util.Duration
import org.eclipse.jgit.api.Git
import spock.lang.Specification

/**
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
class WorkflowMetadataTest extends Specification {

    def 'should populate workflow object' () {

        given:
        final begin = new Date()
        def dir = Files.createTempDirectory('test')
        /*
         * create the github repository
         */
        dir.resolve('main.nf').text = "println 'Hello world'"
        dir.resolve('nextflow.config').text = 'manifest {  }'

        def init = Git.init()
        def repo = init.setDirectory( dir.toFile() ).call()
        repo.add().addFilepattern('.').call()
        def commit = repo.commit().setAll(true).setMessage('First commit').call()
        repo.close()

        // append fake remote data
        dir.resolve('.git/config') << '''
            [remote "origin"]
                url = https://github.com/nextflow-io/nextflow.git
                fetch = +refs/heads/*:refs/remotes/origin/*
            [branch "master"]
                remote = origin
                merge = refs/heads/master
            '''
                .stripIndent()

        /*
         * create ScriptFile object
         */
        def manager = new AssetManager().setLocalPath(dir.toFile())
        def script = manager.getScriptFile()

        /*
         * config file onComplete handler
         */
        def handlerInvoked
        def session = new Session([workflow: [onComplete: { -> handlerInvoked=workflow.commandLine } ]  ])

        /*
         * script runner
         */
        def runner = Mock(ScriptRunner)
        runner.getScriptFile() >> script
        runner.fetchContainers() >> 'busybox/latest'
        runner.commandLine >> 'nextflow run -this -that'
        runner.session >> session

        when:
        def metadata = new WorkflowMetadata(runner)
        session.binding.setVariable('workflow',metadata)
        then:
        metadata.repository == 'https://github.com/nextflow-io/nextflow.git'
        metadata.commitId == commit.name().substring(0,10)
        metadata.revision == 'master'
        metadata.container == 'busybox/latest'
        metadata.projectDir == dir
        metadata.start >= begin
        metadata.start <= new Date()
        metadata.complete == null
        metadata.commandLine == 'nextflow run -this -that'
        metadata.nextflow.version == Const.APP_VER
        metadata.nextflow.build == Const.APP_BUILDNUM
        metadata.nextflow.timestamp == Const.APP_TIMESTAMP_UTC
        metadata.profile == 'standard'
        metadata.sessionId == session.uniqueId
        metadata.runName == session.runName
        !metadata.resume

        when:
        metadata.invokeOnComplete()
        then:
        metadata.complete > metadata.start
        metadata.complete <= new Date()
        metadata.duration == new Duration( metadata.complete.time - metadata.start.time )
        handlerInvoked == metadata.commandLine


        when:
        runner.profile >> 'foo_profile'
        metadata = new WorkflowMetadata(runner)
        then:
        metadata.profile == 'foo_profile'

        cleanup:
        dir?.deleteDir()
    }


    def 'should access workflow script variables onComplete' () {

        given:
        def dir = Files.createTempDirectory('test')
        dir.resolve('main.nf').text = "println 'Hello world'"
        def script = new ScriptFile(dir.resolve('main.nf').toFile())

        def session = new Session()

        def runner = Mock(ScriptRunner)
        runner.getScriptFile() >> script
        runner.fetchContainers() >> 'busybox/latest'
        runner.commandLine >> 'nextflow run -this -that'
        runner.session >> session

        when:
        def metadata = new WorkflowMetadata(runner)
        session.binding.setVariable('value_a', 1)
        session.binding.setVariable('value_b', 2)
        session.binding.setVariable('workflow', metadata)

        def result1
        def result2
        def result3
        def result4
        def result5

        def handler = {
            result1 = workflow.commandLine   // workflow property
            result2 = workflow      // workflow object itself
            result3 = value_a       // variable in the session binding
            result4 = events        // workflow private field, should not be accessed
            result5 = xyz           // unknown field, should return null
        }
        metadata.onComplete(handler)
        metadata.invokeOnComplete()

        then:
        result1 == metadata.commandLine
        result2 == metadata
        result3 == 1
        result4 == null
        result5 == null

        cleanup:
        dir?.deleteDir()

    }

    def 'should access workflow script variables onError' () {

        given:
        def dir = Files.createTempDirectory('test')
        dir.resolve('main.nf').text = "println 'Hello world'"
        def script = new ScriptFile(dir.resolve('main.nf').toFile())

        def session = new Session()

        def runner = Mock(ScriptRunner)
        runner.getScriptFile() >> script
        runner.fetchContainers() >> 'busybox/latest'
        runner.commandLine >> 'nextflow run -this -that'
        runner.session >> session

        when:
        def metadata = new WorkflowMetadata(runner)
        session.binding.setVariable('value_a', 1)
        session.binding.setVariable('value_b', 2)
        session.binding.setVariable('workflow', metadata)

        def result1
        def result2
        def result3
        def result4
        def result5
        def result6

        def handler = {
            result1 = workflow.commandLine   // workflow property
            result2 = workflow      // workflow object itself
            result3 = value_a       // variable in the session binding
            result4 = events        // workflow private field, should not be accessed
            result5 = xyz           // unknown field, should return null
            result6 = workflow.success
        }
        metadata.onError(handler)
        metadata.invokeOnError()

        then:
        result1 == metadata.commandLine
        result2 == metadata
        result3 == 1
        result4 == null
        result5 == null
        result6 == false

        cleanup:
        dir?.deleteDir()

    }


}
