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
import java.nio.file.Path
import java.util.concurrent.atomic.AtomicInteger

import nextflow.container.DockerBuilder
import nextflow.processor.TaskRun
import nextflow.util.MemoryUnit
import nextflow.util.PathTrie
/**
 * Kubernetes executor
 *
 * See https://research.cs.wisc.edu/htcondor/
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
class KubernetesExecutor extends AbstractGridExecutor {

    static private volumes = new AtomicInteger()

    static final public String CMD_KUBE = '.command.yaml'

    final protected BashWrapperBuilder createBashWrapperBuilder(TaskRun task) {
        // creates the wrapper script
        final builder = new KubernetesWrapperBuilder(task)
        return builder
    }


    @Override
    protected String getHeaderToken() {
        throw new UnsupportedOperationException()
    }

    @Override
    protected List<String> getDirectives(TaskRun task, List<String> result) {
        throw new UnsupportedOperationException()
    }

    @Override
    List<String> getSubmitCommandLine(TaskRun task, Path scriptFile) {
        return ['kubectl', 'create', '-f', CMD_KUBE, '-o', 'name']
    }

    @Override
    def parseJobId(String text) {
        if( text.startsWith('job/') ) {
            return text.substring(4)
        }
        throw new IllegalStateException("Not a valid Kubernates job id: `$text`")
    }

    @Override
    protected String getKillCommand() {
        'kubectl'
    }

    @Override
    protected List<String> queueStatusCommand(Object queue) {
        ['kubectl', 'get', 'pods', '-a']
    }

    static protected Map DECODE_STATUS = [
            'U': QueueStatus.PENDING,   // Unexpanded
            'I': QueueStatus.PENDING,   // Idle
            'R': QueueStatus.RUNNING,   // Running
            'X': QueueStatus.ERROR,     // Removed
            'C': QueueStatus.DONE,      // Completed
            'H': QueueStatus.HOLD,      // Held
            'E': QueueStatus.ERROR      // Error
    ]


    @Override
    protected Map<?, QueueStatus> parseQueueStatus(String text) {
        def result = [:]
        if( !text ) return result

        boolean started = false
        def itr = text.readLines().iterator()
        while( itr.hasNext() ) {
            String line = itr.next()
            if( !started ) {
                started = line.startsWith(' ID ')
                continue
            }

            if( !line.trim() ) {
                break
            }

            def cols = line.tokenize(' ')
            def id = cols[0]
            def st = cols[5]
            result[id] = DECODE_STATUS[st]
        }

        return result
    }


    static class KubernetesWrapperBuilder extends BashWrapperBuilder {

        private String taskHash
        private cpu
        private mem

        KubernetesWrapperBuilder(TaskRun task) {
            super(task)
            taskHash = task.hash.toString()
            cpu = task.config.getCpus()
            mem = task.config.getMemory()
        }

        Path build() {
            final wrapper = super.build()
            // save the condor manifest
            workDir.resolve(CMD_KUBE).text = makeYaml()
            // returns the launcher wrapper file
            return wrapper
        }

        String makeYaml() {

            // get input files paths
            def paths = DockerBuilder.inputFilesToPaths(inputFiles)
            // add standard paths
            if( binDir ) paths << binDir
            if( workDir ) paths << workDir

            def trie = new PathTrie()
            paths.each { trie.add(it) }

            // defines the mounts
            def mounts = [:]
            trie.longest().each {
                mounts.put( "vol-${volumes.incrementAndGet()}", it )
            }

            new YamlBuilder(
                    name: "nxf-${taskHash}",
                    image: containerImage,
                    cmd: ['bash', TaskRun.CMD_RUN],
                    workDir: workDir.toString(),
                    mounts: mounts,
                    cpu: cpu,
                    mem: mem  )
                    .create()
        }

    }


    static class YamlBuilder {

        String name
        String image
        List<String> cmd
        String workDir
        Map<String,String> mounts
        int cpu
        MemoryUnit mem

        String create() {

"""\
apiVersion: batch/v1
kind: Job
metadata:
  name: $name
  labels:
    app: nextflow
spec:
  template:
    spec:
      restartPolicy: Never
      containers:
      - name: $name
        image: $image
        command: ${cmd.collect { "\"$it\"" }}
        workingDir: $workDir
${getResources0()}${getMounts0(mounts)}${getVolumes0(mounts)}\
"""

        }

        /**
         * Creates the kubernetes submit YAML mounts fragment
         * @param vols A map holding the volumes to be mount
         * @return A YAML fragment representing the container mounts
         */
        private String getMounts0(Map<String,String> vols) {

            if( !vols ) return ''

            def result = new StringBuilder('        volumeMounts:\n')

            vols.each { String name, String path ->

                result.append(
"""\
        - mountPath: $path
          name: $name
"""
                )
            }

            return result.toString()
        }

        /**
         * Creates the kubernetes submit YAML volumes fragment
         * @param vols A map holding the volumes to be mount
         * @return A YAML fragment representing the container volumes
         */
        private String getVolumes0(Map<String,String> vols) {

            if( !vols ) return ''

            def result = new StringBuilder('      volumes:\n')

            vols.each { String name, String path ->

                result.append(
"""\
      - name: $name
        hostPath:
          path: $path
"""
                )

            }

            result.toString()
        }


        private String getResources0() {

            if( cpu>1 || mem ) {
                def res = ''
                if( cpu>1 ) res += "            cpu: ${cpu}\n"
                if( mem ) res += "            memory: ${mem.toMega()}Mi\n"

                def result = new StringBuilder('        resources:\n')
                result.append('          limits:\n')
                result.append(res)
                result.append('          requests:\n')
                result.append(res)
                return result.toString()
            }
            else {
                return ''
            }
        }

    }
}
