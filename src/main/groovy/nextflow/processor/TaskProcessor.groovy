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
package nextflow.processor
import java.nio.file.Files
import java.nio.file.NoSuchFileException
import java.nio.file.Path
import java.nio.file.Paths
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.Lock
import java.util.concurrent.locks.ReentrantLock

import ch.grengine.Grengine
import com.google.common.hash.HashCode
import groovy.transform.CompileStatic
import groovy.transform.Memoized
import groovy.transform.PackageScope
import groovy.util.logging.Slf4j
import groovyx.gpars.agent.Agent
import groovyx.gpars.dataflow.Dataflow
import groovyx.gpars.dataflow.DataflowChannel
import groovyx.gpars.dataflow.DataflowQueue
import groovyx.gpars.dataflow.DataflowReadChannel
import groovyx.gpars.dataflow.operator.DataflowEventAdapter
import groovyx.gpars.dataflow.operator.DataflowOperator
import groovyx.gpars.dataflow.operator.DataflowProcessor
import groovyx.gpars.dataflow.operator.PoisonPill
import groovyx.gpars.dataflow.stream.DataflowStreamWriteAdapter
import groovyx.gpars.group.PGroup
import nextflow.Nextflow
import nextflow.Session
import nextflow.exception.FailedGuardException
import nextflow.exception.MissingFileException
import nextflow.exception.MissingValueException
import nextflow.exception.ProcessException
import nextflow.exception.ProcessFailedException
import nextflow.exception.ProcessNotRecoverableException
import nextflow.executor.CachedTaskHandler
import nextflow.executor.Executor
import nextflow.file.FileHelper
import nextflow.file.FileHolder
import nextflow.script.BaseScript
import nextflow.script.BasicMode
import nextflow.script.EachInParam
import nextflow.script.EnvInParam
import nextflow.script.FileInParam
import nextflow.script.FileOutParam
import nextflow.script.InParam
import nextflow.script.OutParam
import nextflow.script.ScriptType
import nextflow.script.SetInParam
import nextflow.script.SetOutParam
import nextflow.script.StdInParam
import nextflow.script.StdOutParam
import nextflow.script.TaskBody
import nextflow.script.ValueInParam
import nextflow.script.ValueOutParam
import nextflow.util.ArrayBag
import nextflow.util.BlankSeparatedList
import nextflow.util.CacheHelper
import nextflow.util.CollectionHelper
/**
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
@Slf4j
class TaskProcessor {

    static enum RunType {
        SUBMIT('Submitted process'),
        RETRY('Re-submitted process')

        String message;

        RunType(String str) { message=str };
    }

    static final public String TASK_CONTEXT_PROPERTY_NAME = 'task'

    /**
     * Keeps track of the task instance executed by the current thread
     */
    protected final ThreadLocal<TaskRun> currentTask = new ThreadLocal<>()

    /**
     * Global count of all task instances
     */
    static final protected allCount = new AtomicInteger()

    /**
     * Unique task index number (run)
     */
    final protected indexCount = new AtomicInteger()

    /**
     * The current workflow execution session
     */
    protected Session session

    /**
     * The script object which defines this task
     */
    protected BaseScript ownerScript

    /**
     * Gpars thread pool
     */
    protected PGroup group = Dataflow.retrieveCurrentDFPGroup()

    /**
     * The processor descriptive name
     */
    protected String name

    /**
     * The piece of code to be execute provided by the user
     */
    protected TaskBody taskBody

    /**
     * The corresponding {@code DataflowProcessor} which will receive and
     * manage accordingly the task inputs
     *
     * note: it must be declared volatile -- issue #41
     */
    protected volatile DataflowProcessor processor

    /**
     * The underlying executor which will run the task
     */
    protected final Executor executor

    /**
     * The corresponding task configuration properties, it holds the inputs/outputs
     * definition as well as other execution meta-declaration
     */
    protected final ProcessConfig config

    /**
     * Count the number of time an error occurred
     */
    private volatile int errorCount

    /**
     * Lock the work dir creation process
     */
    private static Lock lockWorkDirCreation = new ReentrantLock()

    /**
     * Set to true the very first time the error is shown.
     *
     * Note: it is declared static because the error must be shown only the
     * very first time  for all processes
     */
    private static final AtomicBoolean errorShown = new AtomicBoolean()

    /**
     * Flag set {@code true} when the processor termination has been invoked
     *
     * See {@code #checkProcessTermination}
     */
    protected volatile boolean terminated

    protected boolean allScalarValues

    protected boolean hasEachParams

    /**
     * Whenever the process execution is required to be blocking in order to handle
     * shared object in a thread safe manner
     */
    protected boolean blocking

    /**
     * The state is maintained by using an agent
     */
    protected Agent<StateObj> state

    /**
     * Groovy engine used to evaluate dynamic code
     */
    protected Grengine grengine

    /**
     * Process ID number. The first is 1, the second 2 and so on ..
     */
    private final int id

    private static int processCount

    /*
     * Initialise the process ID
     *
     * Note: processes are create in a sequential manner (by the main thread that parse the script)
     * so it does not require a synchronized block
     */
    {
        id = ++processCount
        grengine = session && session.classLoader ? new Grengine(session.classLoader) : new Grengine()
    }

    /* for testing purpose - do not remove */
    protected TaskProcessor() { }

    /**
     * Create and initialize the processor object
     *
     * @param name
     * @param executor
     * @param session
     * @param script
     * @param config
     * @param taskBody
     */
    TaskProcessor( String name, Executor executor, Session session, BaseScript script, ProcessConfig config, TaskBody taskBody ) {
        assert executor
        assert session
        assert script
        assert taskBody

        this.executor = executor
        this.session = session
        this.ownerScript = script
        this.config = config
        this.taskBody = taskBody
        this.name = name

    }

    /**
     * @return The processor unique id
     */
    int getId() { id }
  
    /**
     * @return The {@code TaskConfig} object holding the task configuration properties
     */
    ProcessConfig getConfig() { config }

    /**
     * @return The current {@code Session} instance
     */
    Session getSession() { session }

    /**
     * @return The processor name
     */
    String getName() { name }

    /**
     * @return The {@code BaseScript} object which represents pipeline script
     */
    BaseScript getOwnerScript() { ownerScript }

    /**
     *  Define the type of script hold by the {@code #code} property
     */
    protected ScriptType getScriptType() { taskBody.type }

    /**
     * @return The user provided script block
     */
    public TaskBody getTaskBody() { taskBody }

    /**
     * Launch the 'script' define by the code closure as a local bash script
     *
     * @param code A {@code Closure} retuning a bash script e.g.
     *          <pre>
     *              {
     *                 """
     *                 #!/bin/bash
     *                 do this ${x}
     *                 do that ${y}
     *                 :
     *                 """
     *              }
     *
     * @return {@code this} instance
     */
    def run() {

        if ( !taskBody )
            throw new IllegalStateException("Missing task body for process `$name`")

        /**
         * Verify if this process run only one time
         */
        allScalarValues = config.getInputs().allScalarInputs()
        hasEachParams = config.getInputs().any { it instanceof EachInParam }

        /*
         * Normalize input channels
         */
        config.fakeInput()

        /*
         * Normalize the output
         * - even though the output may be empty, let return the stdout as output by default
         */
        if ( config.getOutputs().size() == 0 ) {
            config.fakeOutput()
        }

        // the state agent
        state = new Agent<>(new StateObj(name))
        state.addListener { StateObj old, StateObj obj ->
            try {
                if( log.isTraceEnabled() )
                    log.trace "<$name> Process state changed to: $obj -- finished: ${obj.isFinished()}"
                if( !terminated && obj.isFinished() ) {
                    terminateProcess()
                    terminated = true
                }
            }
            catch( Throwable e ) {
                session.abort(e)
            }
        }

        // register the processor
        // note: register the task *before* creating (and starting the dataflow operator) in order
        // a race condition on the processes barrier - this fix issue #43
        session.processRegister(this)

        // create the underlying dataflow operator
        createOperator()

        session.notifyProcessCreate(this)

        /*
         * When there is a single output channel, return let returns that item
         * otherwise return the list
         */
        def result = config.getOutputs().channels
        return result.size() == 1 ? result[0] : result
    }

    /**
     * Template method which extending classes have to override in order to
     * create the underlying *dataflow* operator associated with this processor
     *
     * See {@code DataflowProcessor}
     */

    protected void createOperator() {

        def opInputs = new ArrayList(config.getInputs().getChannels())

        /*
         * check if there are some iterators declaration
         * the list holds the index in the list of all *inputs* for the {@code each} declaration
         */
        def iteratorIndexes = []
        config.getInputs().eachWithIndex { param, index ->
            if( param instanceof EachInParam ) {
                log.trace "Process ${name} > got each param: ${param.name} at index: ${index} -- ${param.dump()}"
                iteratorIndexes << index
            }
        }

        /*
         * When one (or more) {@code each} are declared as input, it is created an extra
         * operator which will receive the inputs from the channel (excepts the values over iterate)
         *
         * The operator will *expand* the received inputs, iterating over the user provided value and
         * forwarding the final values the the second *parallel* processor executing the user specified task
         */
        if( iteratorIndexes ) {
            log.debug "Creating *combiner* operator for each param(s) at index(es): ${iteratorIndexes}"

            // don't care about the last channel, being the control channel it doesn't bring real values
            final size = opInputs.size()-1

            // the iterator operator needs to executed just one time
            // thus add a dataflow queue binding a single value and then a stop signal
            def termination = new DataflowQueue<>()
            termination << Boolean.TRUE
            opInputs[size] = termination

            // the channel forwarding the data from the *iterator* process to the target task
            final linkingChannels = new ArrayList(size)
            size.times { linkingChannels[it] = new DataflowQueue() }

            // the script implementing the iterating process
            final forwarder = createForwardWrapper(size, iteratorIndexes)

            // instantiate the iteration process
            def stopAfterFirstRun = allScalarValues
            def interceptor = new BaseProcessInterceptor(opInputs, stopAfterFirstRun)
            def params = [inputs: opInputs, outputs: linkingChannels, maxForks: 1, listeners: [interceptor]]
            session.allProcessors << (processor = new DataflowOperator(group, params, forwarder))
            // fix issue #41
            processor.start()

            // set as next inputs the result channels of the iteration process
            // adding the 'control' channel removed previously
            opInputs = new ArrayList(size+1)
            opInputs.addAll( linkingChannels )
            opInputs.add( config.getInputs().getChannels().last() )
        }


        /*
         * create a mock closure to trigger the operator
         */
        final wrapper = createCallbackWrapper( opInputs.size(), this.&invokeTask )

        /*
         * define the max forks attribute:
         * - by default the process execution is parallel using the poolSize value
         * - otherwise use the value defined by the user via 'taskConfig'
         */
        def maxForks = session.poolSize
        if( config.maxForks ) {
            maxForks = config.maxForks
            blocking = true
        }
        log.debug "Creating operator > $name -- maxForks: $maxForks"

        /*
         * finally create the operator
         */
        // note: do not specify the output channels in the operator declaration
        // this allows us to manage them independently from the operator life-cycle
        def stopAfterFirstRun = allScalarValues && !hasEachParams
        def interceptor = new TaskProcessorInterceptor(opInputs, stopAfterFirstRun)
        def params = [inputs: opInputs, maxForks: maxForks, listeners: [interceptor] ]
        session.allProcessors << (processor = new DataflowOperator(group, params, wrapper))

        // notify the creation of a new vertex the execution DAG
        session.dag.addProcessNode(name, config.getInputs(), config.getOutputs())

        // fix issue #41
        processor.start()

    }

    /**
     * Implements the closure which *combines* all the iteration
     *
     * @param numOfInputs Number of in/out channel
     * @param indexes The list of indexes which identify the position of iterators in the input channels
     * @return The closure implementing the iteration/forwarding logic
     */
    protected createForwardWrapper( int len, List indexes ) {

        final args = []
        (len+1).times { args << "x$it" }

        final outs = []
        len.times { outs << "x$it" }

        /*
         * Explaining the following closure:
         *
         * - it has to be evaluated as a string since the number of input must much the number input channel
         *   that is known only at runtime
         *
         * - 'out' holds the list of all input values which need to be forwarded (bound) to output as many
         *   times are the items in the iteration list
         *
         * - the iteration list(s) is (are) passed like in the closure inputs like the other values,
         *   the *indexes* argument defines the which of them are the iteration lists
         *
         * - 'itr' holds the list of all iteration lists
         *
         * - using the groovy method a combination of all values is create (cartesian product)
         *   see http://groovy.codehaus.org/groovy-jdk/java/util/Collection.html#combinations()
         *
         * - the resulting values are replaced in the 'out' array of values and forwarded out
         *
         */

        final str =
                """
            { ${args.join(',')} ->
                def out = [ ${outs.join(',')} ]
                def itr = [ ${indexes.collect { 'x'+it }.join(',')} ]
                def cmb = itr.combinations()
                for( entries in cmb ) {
                    def count = 0
                    ${len}.times { i->
                        if( i in indexes ) { out[i] = entries[count++] }
                    }
                    bindAllOutputValues( *out )
                }
            }
            """


        final Binding binding = new Binding( indexes: indexes )
        final result = (Closure)grengine.run(str, binding)
        return result

    }


    /**
     * The processor execution body
     *
     * @param processor
     * @param values
     */
    final protected void invokeTask( def args ) {

        // create and initialize the task instance to be executed
        final List values = args instanceof List ? args : [args]
        if( log.isTraceEnabled() )
            log.trace "Setup new process > $name"

        // -- create the task run instance
        final task = createTaskRun()
        // -- set the task instance as the current in this thread
        currentTask.set(task)

        // -- map the inputs to a map and use to delegate closure values interpolation
        final secondPass = [:]
        int count = makeTaskContextStage1(task, secondPass, values)
        makeTaskContextStage2(task, secondPass, count)

        // verify that `when` guard, when specified, is satisfied
        if( !checkWhenGuard(task) )
            return

        // -- verify if exists a stored result for this case,
        //    if true skip the execution and return the stored data
        if( checkStoredOutput(task) )
            return

        def hash = createTaskHashKey(task)
        checkCachedOrLaunchTask(task, hash, resumable)
    }


    /**
     * @return A string 'she-bang' formatted to the added on top script to be executed.
     * The interpreter to be used define bu the *taskConfig* property {@code shell}
     */
    static shebangLine(shell) {
        assert shell, "Missing 'shell' property in process configuration"

        String result = shell instanceof List ? shell.join(' ') : shell
        if( result.startsWith('/') ) {
            result = '#!' + result
        }
        else {
            result= '#!/usr/bin/env ' + result
        }

        return result

    }

    /**
     * Remove extra leading, trailing whitespace and newlines chars,
     * also if the script does not start with a {@code shebang} line,
     * add the default by using the current {@code #shell} attribute
     */
    static String normalizeScript(String script, shell) {
        assert script != null

        def result = new StringBuilder()
        result << script.stripIndent().trim()
        result << '\n'

        if( result[0] != '#' || result[1] != '!') {
            result.insert(0, shebangLine(shell) +'\n')
        }

        return result.toString()
    }

    /**
     * Given the task script extract the top *she-bang* interpreter declaration removing the {@code #!} characters.
     * @param script The script to be executed
     *
     * @return The interpreter as defined in the she-bang declaration, for example {@code /usr/bin/env perl}
     */
    static String fetchInterpreter( String script ) {
        assert script != null

        if( script[0] == '#' && script[1] == '!') {
            return script.readLines()[0].substring(2)
        }

        return null
    }

    /**
     * Wraps the target method by a closure declaring as many arguments as many are the user declared inputs
     * object.
     *
     * @param n The number of inputs
     * @param method The method which will execute the task
     * @return The operator closure adapter
     */
    final protected Closure createCallbackWrapper( int n, Closure method ) {

        final args = []
        for( int i=0; i<n; i++ )
            args << "__p$i"

        final str = " { ${args.join(',')} -> callback([ ${args.join(',')} ]) }"
        final binding = new Binding( ['callback': method] )
        final result = (Closure)grengine.run(str,binding)

        return result
    }

    /**
     * Create a new {@code TaskRun} instance, initializing the following properties :
     * <li>{@code TaskRun#id}
     * <li>{@code TaskRun#status}
     * <li>{@code TaskRun#index}
     * <li>{@code TaskRun#name}
     * <li>{@code TaskRun#process}
     *
     * @return The new newly created {@code TaskRun}
     */
    final protected TaskRun createTaskRun() {
        log.trace "Creating a new process > $name"

        def id = allCount.incrementAndGet()
        def index = indexCount.incrementAndGet()
        def task = new TaskRun(
                id: id,
                index: index,
                processor: this,
                type: scriptType,
                config: config.createTaskConfig(),
                context: new TaskContext(this)
        )

        // setup config
        task.config.index = task.index
        task.config.process = task.processor.name
        task.config.executor = task.processor.executor.name

        /*
         * initialize the inputs/outputs for this task instance
         */
        config.getInputs().each { InParam param ->
            if( param instanceof SetInParam )
                param.inner.each { task.setInput(it)  }
            else
                task.setInput(param)
        }

        config.getOutputs().each { OutParam param ->
            if( param instanceof SetOutParam ) {
                param.inner.each { task.setOutput(it) }
            }
            else
                task.setOutput(param)
        }

        return task
    }

    /**
     * Try to check if exists a previously executed process result in the a cached folder. If it exists
     * use the that result and skip the process execution, otherwise the task is sumitted for execution.
     *
     * @param task
     *      The {@code TaskRun} instance to be executed
     * @param hash
     *      The unique {@code HashCode} for the given task inputs
     * @param script
     *      The script to be run (only when it's a merge task)
     * @return
     *      {@code false} when a cached result has been found and the execution has skipped,
     *      or {@code true} if the task has been submitted for execution
     *
     */

    final protected boolean checkCachedOrLaunchTask( TaskRun task, HashCode hash, boolean shouldTryCache ) {

        int tries = 0
        while( true ) {
            if( tries++ ) {
                hash = CacheHelper.defaultHasher().newHasher().putBytes(hash.asBytes()).putInt(tries).hash()
            }

            boolean exists=false
            final folder = FileHelper.getWorkFolder(session.workDir, hash)
            lockWorkDirCreation.lock()
            try {
                exists = folder.exists()
                if( !exists && !folder.mkdirs() )
                    throw new IOException("Unable to create folder: $folder -- check file system permission")
            }
            finally {
                lockWorkDirCreation.unlock()
            }

            log.trace "[${task.name}] Cacheable folder: $folder -- exists: $exists; try: $tries"
            def cached = shouldTryCache && exists && checkCachedOutput(task, folder, hash)
            if( cached )
                return false

            if( exists )
                continue

            // submit task for execution
            submitTask( task, hash, folder )
            return true
        }

    }

    /**
     * Check if exists a *storeDir* for the specified task. When if exists
     * and contains the expected result files, the process execution is skipped.
     *
     * @param task The task for which check the stored output
     * @return {@code true} when the folder exists and it contains the expected outputs,
     *      {@code false} otherwise
     */
    final boolean checkStoredOutput( TaskRun task ) {
        if( !task.config.storeDir ) {
            log.trace "[$task.name] Store dir not set -- return false"
            return false
        }

        // -- when store path is set, only output params of type 'file' can be specified
        final ctx = task.context
        def invalid = task.getOutputs().keySet().any {
            if( it instanceof ValueOutParam ) {
                return !ctx.containsKey(it.name)
            }
            if( it instanceof FileOutParam ) {
                return false
            }
            return true
        }
        if( invalid ) {
            log.warn "[$task.name] Store dir can be used when using 'file' outputs"
            return false
        }

        if( !task.config.getStoreDir().exists() ) {
            log.trace "[$task.name] Store dir does not exists > ${task.config.storeDir} -- return false"
            // no folder -> no cached result
            return false
        }


        try {
            // -- check if all output resources are available
            collectOutputs(task)
            log.info "[skipping] Stored process > ${task.name}"

            // set the exit code in to the task object
            task.exitStatus = task.config.getValidExitStatus()[0]
            task.cached = true

            // -- now bind the results
            finalizeTask0(task)
            return true
        }
        catch( MissingFileException | MissingValueException e ) {
            log.trace "[$task.name] Missed store > ${e.getMessage()} -- folder: ${task.config.storeDir}"
            task.exitStatus = Integer.MAX_VALUE
            task.workDir = null
            return false
        }
    }

    /**
     * Check whenever the outputs for the specified task already exist
     *
     * @param task The task instance
     * @param folder The folder where the outputs are stored (eventually)
     * @return {@code true} when all outputs are available, {@code false} otherwise
     */
    final boolean checkCachedOutput(TaskRun task, Path folder, HashCode hash) {

        // check if exists the task exit code file
        def exitCode = null
        def exitFile = folder.resolve(TaskRun.CMD_EXIT)
        if( task.type == ScriptType.SCRIPTLET ) {
            def str
            try {
                str = exitFile.text?.trim()
            }
            catch( IOException e ) {
                log.trace "[$task.name] Exit file can't be read > $exitFile -- return false -- Cause: ${e.message}"
                return false
            }

            exitCode = str.isInteger() ? str.toInteger() : null
            if( !task.isSuccess(exitCode) ) {
                log.trace "[$task.name] Exit code is not valid > $str -- return false"
                return false
            }
        }

        /*
         * load the task record in the cache DB
         */
        final entry = session.getCachedTask(task.processor, hash)

        /*
         * verify cached context map
         */
        TaskContext ctx = null
        def ctxFile = folder.resolve(TaskRun.CMD_CONTEXT)
        if( !entry && task.hasCacheableValues() ) {
            try {
                ctx = TaskContext.read(this, ctxFile)
            }
            catch( Throwable e ) {
                log.warn1("[$task.name] Unable to resume cached task -- See log file for details", causedBy: e)
                return false
            }
        }

        /*
         * verify stdout file
         */
        def stdoutFile = folder.resolve( TaskRun.CMD_OUTFILE )

        try {
            // -- check if all output resources are available
            collectOutputs(task, folder, stdoutFile, ctx)

            // set the exit code in to the task object
            task.cached = true
            task.hash = hash
            task.workDir = folder
            task.stdout = stdoutFile
            if( exitCode != null ) {
                task.exitStatus = exitCode
            }
            if( ctx != null ) {
                task.context = ctx
                task.config.context = ctx
                task.code?.delegate = ctx
            }

            log.info "[${task.hashLog}] Cached process > ${task.name}"
            // -- notify cached event
            if( entry )
                session.dispatcher.notifyCached(new CachedTaskHandler(task,entry.trace))

            // -- now bind the results
            finalizeTask0(task)
            return true
        }
        catch( MissingFileException | MissingValueException e ) {
            log.trace "[$task.name] Missed cache > ${e.getMessage()} -- folder: $folder"
            task.exitStatus = Integer.MAX_VALUE
            task.workDir = null
            return false
        }

    }

    /**
     * Handles an error raised during the processor execution
     *
     * @param error The exception raised during the task execution
     * @param task The {@code TaskDef} instance which raised the exception
     * @return {@code true} to terminate the processor execution,
     *         {@code false} ignore the error and continue to process other pending tasks
     */
    final protected boolean handleException( Throwable error, TaskRun task = null ) {
        log.trace "Handling error: $error -- task: $task"
        def fault = resumeOrDie(task, error)

        if (fault instanceof TaskFault) {
            session.fault(fault)
            // when a `TaskFault` is returned a `TERMINATE` is implicit, thus return `true`
            return true
        }

        return fault == ErrorStrategy.TERMINATE || fault == ErrorStrategy.FINISH
    }

    /**
     * @param task The {@code TaskRun} instance that raised an error
     * @param error The error object
     * @return
     *      Either a value of value of {@link ErrorStrategy} representing the error strategy chosen
     *      or an instance of {@TaskFault} representing the cause of the error (that implicitly means
     *      a {@link ErrorStrategy#TERMINATE})
     */
    final synchronized protected resumeOrDie( TaskRun task, Throwable error ) {
        if( log.isTraceEnabled() )
        log.trace "Handling unexpected condition for\n  task: $task\n  error [${error?.class?.name}]: ${error?.getMessage()?:error}"

        ErrorStrategy errorStrategy = ErrorStrategy.TERMINATE
        final message = []
        try {
            // -- do not recoverable error, just re-throw it
            if( error instanceof Error ) throw error

            final int taskErrCount = task ? ++task.failCount : 0
            final int procErrCount = ++errorCount

            // -- when is a task level error and the user has chosen to ignore error,
            //    just report and error message and DO NOT stop the execution
            if( task && error instanceof ProcessException ) {
                // expose current task exist status
                task.config.exitStatus = task.exitStatus
                task.config.errorCount = procErrCount
                task.config.retryCount = taskErrCount

                errorStrategy = checkErrorStrategy(task, error, taskErrCount, procErrCount)
                if( errorStrategy.soft ) {
                    task.failed = true
                    return errorStrategy
                }
            }

            // -- mark the task as failed
            if( task )
                task.failed = true

            // -- make sure the error is showed only the very first time across all processes
            if( errorShown.getAndSet(true) || session.aborted ) {
                return errorStrategy
            }

            message << "Error executing process > '${task?.name ?: name}'"
            switch( error ) {
                case ProcessException:
                    formatTaskError( message, error, task )
                    break

                case FailedGuardException:
                    formatGuardError( message, error as FailedGuardException, task )
                    break;

                default:
                    message << formatErrorCause(error)
            }
            log.error message.join('\n'), error

        }
        catch( Throwable e ) {
            // no recoverable error
            log.error("Execution aborted due to an unexpected error", e )
        }

        return new TaskFault(error: error, task: task, report: message.join('\n'), strategy: errorStrategy)
    }

    protected ErrorStrategy checkErrorStrategy( TaskRun task, ProcessException error, final int taskErrCount, final int procErrCount ) {

        final errorStrategy = task.config.getErrorStrategy()

        // retry is not allowed when the script cannot be compiled or similar errors
        if( error instanceof ProcessNotRecoverableException ) {
            return !errorStrategy.soft ? errorStrategy : ErrorStrategy.TERMINATE
        }

        // IGNORE strategy -- just continue
        if( errorStrategy == ErrorStrategy.IGNORE ) {
            log.warn "Error running process > ${error.getMessage()} -- error is ignored"
            return ErrorStrategy.IGNORE
        }

        // RETRY strategy -- check that process do not exceed 'maxError' and the task do not exceed 'maxRetries'
        if( errorStrategy == ErrorStrategy.RETRY ) {
            final int maxErrors = task.config.getMaxErrors()
            final int maxRetries = task.config.getMaxRetries()

            if( (procErrCount < maxErrors || maxErrors == -1) && taskErrCount <= maxRetries ) {
                final taskCopy = task.clone()
                session.getExecService().submit({
                    try {
                        taskCopy.config.attempt = taskErrCount+1
                        taskCopy.runType = RunType.RETRY
                        checkCachedOrLaunchTask( taskCopy, taskCopy.hash, false )
                    }
                    catch( Throwable e ) {
                        log.error "Unable to re-submit task `${taskCopy.name}`"
                        session.abort(e)
                    }
                } as Runnable)
                return ErrorStrategy.RETRY
            }

            return ErrorStrategy.TERMINATE
        }

        return errorStrategy
    }

    final protected formatGuardError( List message, FailedGuardException error, TaskRun task ) {
        // compose a readable error message
        message << formatErrorCause( error )

        if( error.source )  {
            message << "\nWhen block:"
            error.source.stripIndent().eachLine {
                message << "  $it"
            }
        }

        if( task?.workDir )
            message << "\nWork dir:\n  ${task.workDir.toString()}"

        return message
    }

    final protected formatTaskError( List message, Throwable error, TaskRun task ) {

        // compose a readable error message
        message << formatErrorCause( error )

        /*
         * task executing scriptlets
         */
        if( task?.script ) {
            // - print the executed command
            message << "Command executed${task.template ? " [$task.template]": ''}:\n"
            task.script?.stripIndent()?.trim()?.eachLine {
                message << "  ${it}"
            }

            // - the exit status
            message << "\nCommand exit status:\n  ${task.exitStatus != Integer.MAX_VALUE ? task.exitStatus : '-'}"

            // - the tail of the process stdout
            message << "\nCommand output:"
            final max = 50
            def lines = task.dumpStdout(max)
            if( lines.size() == 0 ) {
                message << "  (empty)"
            }
            lines.each {
                message << "  ${task.workDir ? it.replace(task.workDir.toString()+'/','') : it }"
            }

            // - the tail of the process stderr
            lines = task.dumpStderr(max)
            if( lines ) {
                message << "\nCommand error:"
                lines.each {
                    message << "  ${task.workDir ? it.replace(task.workDir.toString()+'/','') : it }"
                }
            }

        }
        else {
            if( task?.source )  {
                message << "\nSource block:"
                task.source.stripIndent().eachLine {
                    message << "  $it"
                }
            }

        }

        if( task?.workDir )
            message << "\nWork dir:\n  ${task.workDir.toString()}"

        message << "\nTip: ${getRndTip()}"

        return message
    }

    static List tips = [
            'when you have fixed the problem you can continue the execution appending to the nextflow command line the option \'-resume\'',
            "you can try to figure out what's wrong by changing to the process work dir and showing the script file named: '${TaskRun.CMD_SCRIPT}'",
            "view the complete command output by changing to the process work dir and entering the command: 'cat ${TaskRun.CMD_OUTFILE}'",
            "you can replicate the issue by changing to the process work dir and entering the command: 'bash ${TaskRun.CMD_RUN}'"
    ]

    static Random RND = Random.newInstance()

    /**
     * Display a random tip at the bottom of the error report
     *
     * @return The tip string to display
     */
    protected String getRndTip() {
        tips[ RND.nextInt( tips.size() ) ]
    }


    /**
     * Send a poison pill over all the outputs channel
     */
    final protected synchronized void sendPoisonPill() {
        log.trace "<$name> Sending a poison pill(s)"

        config.getOutputs().getChannels().each { channel ->

            if( channel instanceof DataflowQueue ) {
                channel.bind( PoisonPill.instance )
            }
            else if( channel instanceof DataflowStreamWriteAdapter ) {
                channel.bind( PoisonPill.instance )
            }
        }
    }


    private String formatErrorCause( Throwable error ) {

        def result = new StringBuilder()
        result << '\nCaused by:\n'

        def message = error.cause?.getMessage() ?: ( error.getMessage() ?: error.toString() )
        result.append('  ').append(message).append('\n')

        result.toString()
    }

    /**
     * Publish output files to a specified target folder
     *
     * @param task The task whose outputs need to be published
     * @param overwrite When {@code true} any existing file will be overwritten, otherwise the publishing is ignored
     */
    @CompileStatic
    protected void publishOutputs( TaskRun task ) {
        def publish = task.config.getPublishDir()
        if( !publish ) {
            return
        }

        if( publish.overwrite == null ) {
            publish.overwrite = !task.cached
        }

        List<Path> files = []
        task.getOutputsByType(FileOutParam).each { param, value ->
            if( value instanceof Path ) {
                files << ((Path)value)
            }
            else if( value instanceof Collection<Path> ) {
                value.each { Path it -> files << it }
            }
            else if( value != null ) {
                throw new IllegalArgumentException("Unknown output file object [${value.class.name}]: ${value}")
            }
        }


        publish.apply(files, task)
    }


    /**
     * Bind the expected output files to the corresponding output channels
     * @param processor
     */
    synchronized protected void bindOutputs( TaskRun task ) {

        // -- creates the map of all tuple values to bind
        Map<Short,List> tuples = [:]
        config.getOutputs().each { OutParam p -> tuples.put(p.index,[]) }

        // -- collects the values to bind
        task.outputs.each { OutParam param, value ->

            switch( param ) {
            case StdOutParam:
                log.trace "Process $name > normalize stdout param: $param"
                value = value instanceof Path ? value.text : value?.toString()

            case FileOutParam:
            case ValueOutParam:
                log.trace "Process $name > collecting out param: ${param} = $value"
                tuples[param.index].add(value)
                break

            default:
                throw new IllegalArgumentException("Illegal output parameter type: $param")
            }
        }

        // -- bind out the collected values
        config.getOutputs().each { param ->
            def list = tuples[param.index]
            if( list == null ) throw new IllegalStateException()

            if( param.mode == BasicMode.standard ) {
                log.trace "Process $name > Binding out param: ${param} = ${list}"
                bindOutParam(param, list)
            }

            else if( param.mode == BasicMode.flatten ) {
                log.trace "Process $name > Flatting out param: ${param} = ${list}"
                CollectionHelper.flatten( list ) {
                    bindOutParam( param, it )
                }
            }

            else if( param.mode == SetOutParam.CombineMode.combine ) {
                log.trace "Process $name > Combining out param: ${param} = ${list}"
                def combs = list.combinations()
                combs.each { bindOutParam(param, it) }
            }

            else
                throw new IllegalStateException("Unknown bind output parameter type: ${param}")
        }


        // -- finally prints out the task output when 'echo' is true
        if( task.config.echo ) {
            task.echoStdout()
        }
    }

    protected void bindOutParam( OutParam param, List values ) {
        if( log.isTraceEnabled() )
            log.trace "<$name> Binding param $param with $values"

        def x = values.size() == 1 ? values[0] : values
        param.getOutChannels().each { it.bind(x) }
    }

    protected void collectOutputs( TaskRun task ) {
        collectOutputs( task, task.getTargetDir(), task.@stdout, task.context )
    }

    /**
     * Once the task has completed this method is invoked to collected all the task results
     *
     * @param task
     */
    final protected void collectOutputs( TaskRun task, Path workDir, def stdout, Map context ) {
        if( log.isTraceEnabled() )
            log.trace "<$name> collecting output: ${task.outputs}"

        task.outputs.keySet().each { OutParam param ->

            switch( param ) {
                case StdOutParam:
                    collectStdOut(task, (StdOutParam)param, stdout)
                    break

                case FileOutParam:
                    collectOutFiles(task, (FileOutParam)param, workDir, context)
                    break

                case ValueOutParam:
                    collectOutValues(task, (ValueOutParam)param, context)
                    break

                default:
                    throw new IllegalArgumentException("Illegal output parameter: ${param.class.simpleName}")

            }
        }

        // mark ready for output binding
        task.canBind = true
    }

    /**
     * Collects the process 'std output'
     *
     * @param task The executed process instance
     * @param param The declared {@link StdOutParam} object
     * @param stdout The object holding the task produced std out object
     */
    protected void collectStdOut( TaskRun task, StdOutParam param, def stdout ) {

        if( stdout == null && task.type == ScriptType.SCRIPTLET ) {
            throw new IllegalArgumentException("Missing 'stdout' for process > ${task.name}")
        }

        if( stdout instanceof Path && !stdout.exists() ) {
            throw new MissingFileException("Missing 'stdout' file: ${stdout} for process > ${task.name}")
        }

        task.setOutput(param, stdout)
    }


    protected void collectOutFiles( TaskRun task, FileOutParam param, Path workDir, Map context ) {

        final List<Path> allFiles = []
        // type file parameter can contain a multiple files pattern separating them with a special character
        def entries = param.getFilePatterns(context, task.workDir)

        // for each of them collect the produced files
        entries.each { String pattern ->
            List<Path> result = null
            if( FileHelper.isGlobPattern(pattern) ) {
                result = fetchResultFiles(param, pattern, workDir)
                // filter the inputs
                if( !param.includeInputs ) {
                    result = filterByRemovingStagedInputs(task, result)
                    if( log.isTraceEnabled() )
                    log.trace "Process ${task.name} > after removing staged inputs: ${result}"
                }
            }
            else {
                def file = workDir.resolve(pattern)
                if( file.exists() )
                    result = [file]
                else
                    log.debug "Process `${task.name}` is unable to find [${file.class.simpleName}]: `$file` (pattern: `$pattern`)"
            }

            if( !result )
                throw new MissingFileException("Missing output file(s): '$pattern' expected by process: ${task.name}")

            allFiles.addAll(result)
        }

        task.setOutput( param, allFiles.size()==1 ? allFiles[0] : allFiles )

    }


    protected void collectOutValues( TaskRun task, ValueOutParam param, Map ctx ) {

        try {
            // fetch the output value
            final val = param.resolve(ctx)
            // set into the output set
            task.setOutput(param,val)
            // trace the result
            if( log.isTraceEnabled() )
                log.trace "Collecting param: ${param.name}; value: ${val}"
        }
        catch( MissingPropertyException e ) {
            throw new MissingValueException("Missing value declared as output parameter: ${e.property}")
        }

    }

    /**
     * Collect the file(s) with the name specified, produced by the execution
     *
     * @param workDir The job working path
     * @param namePattern The file name, it may include file name wildcards
     * @return The list of files matching the specified name in lexicographical order
     * @throws MissingFileException when no matching file is found
     */
    @PackageScope
    List<Path> fetchResultFiles( FileOutParam param, String namePattern, Path workDir ) {
        assert namePattern
        assert workDir

        List files = []
        def opts = visitOptions(param, namePattern)
        // scan to find the file with that name
        try {
            FileHelper.visitFiles(opts, workDir, namePattern) { Path it -> files.add(it) }
        }
        catch( NoSuchFileException e ) {
            throw new MissingFileException("Cannot access folder: '$workDir'", e)
        }

        return files.sort()
    }

    /**
     * Given a {@link FileOutParam} object create the option map for the
     * {@link FileHelper#visitFiles(java.util.Map, java.nio.file.Path, java.lang.String, groovy.lang.Closure)} method
     *
     * @param param A task {@link FileOutParam}
     * @param namePattern A file glob pattern
     * @return A {@link Map} object holding the traverse options for the {@link FileHelper#visitFiles(java.util.Map, java.nio.file.Path, java.lang.String, groovy.lang.Closure)} method
     */
    @PackageScope
    Map visitOptions( FileOutParam param, String namePattern ) {
        final opts = [:]
        opts.relative = false
        opts.hidden = param.hidden ?: namePattern.startsWith('.')
        opts.followLinks = param.followLinks
        opts.maxDepth = param.maxDepth
        opts.type = param.type ? param.type : ( namePattern.contains('**') ? 'file' : 'any' )
        return opts
    }

    /**
     * Given a list of {@code Path} removes all the hidden file i.e. the ones which names starts with a dot char
     * @param files A list of {@code Path}
     * @return The result list not containing hidden file entries
     */
    @PackageScope
    List<Path> filterByRemovingHiddenFiles( List<Path> files ) {
        files.findAll { !it.getName().startsWith('.') }
    }

    /**
     * Given a list of {@code Path} removes all the entries which name match the name of
     * file used as input for the specified {@code TaskRun}
     *
     * See TaskRun#getStagedInputs
     *
     * @param task
     * @param files
     * @return
     */
    @PackageScope
    List<Path> filterByRemovingStagedInputs( TaskRun task, List<Path> files ) {

        // get the list of input files
        def List<String> allStaged = task.getStagedInputs()
        files.findAll { !allStaged.contains(it.getName()) }

    }


    /**
     * @return The map holding the shell environment variables for the task to be executed
     */
    @Memoized
    def Map<String,String> getProcessEnvironment() {

        def result = [:]

        // add the taskConfig environment entries
        if( session.config.env instanceof Map ) {
            session.config.env.each { name, value ->
                result.put( name, value?.toString() )
            }
        }
        else {
            log.debug "Invalid 'session.config.env' object: ${session.config.env?.class?.name}"
        }


        // pre-pend the 'bin' folder to the task environment
        if( session.binDir ) {
            if( result.containsKey('PATH') ) {
                result['PATH'] =  "${session.binDir}:${result['PATH']}".toString()
            }
            else {
                result['PATH'] = "${session.binDir}:\$PATH".toString()
            }
        }

        return Collections.unmodifiableMap(result)
    }

    /**
     * An input file parameter can be provided with any value other than a file.
     * This function normalize a generic value to a {@code Path} create a temporary file
     * in the for it.
     *
     * @param input The input value
     * @param altName The name to be used when a temporary file is created.
     * @return The {@code Path} that will be staged in the task working folder
     */
    protected FileHolder normalizeInputToFile( Object input, String altName ) {

        /*
         * when it is a local file, just return a reference holder to it
         */
        if( input instanceof Path && input.fileSystem == FileHelper.workDirFileSystem ) {
            return new FileHolder(input)
        }

        /*
         * when it is a file stored in a "foreign" storage, copy
         * to a local file and return a reference holder to the local file
         */

        if( input instanceof Path ) {
            log.debug "Copying to process workdir foreign file: ${input.toUri().toString()}"
            def result = Nextflow.tempFile(input.getFileName().toString())
            Files.copy(Files.newInputStream(input), result)
            return new FileHolder(input, result)
        }

        /*
         * default case, convert the input object to a string and save
         * to a local file
         */
        def source = input?.toString() ?: ''
        def result = Nextflow.tempFile(altName)
        result.text = source
        return new FileHolder(source, result)
    }


    protected List<FileHolder> normalizeInputToFiles( Object obj, int count ) {

        Collection allItems = obj instanceof Collection ? obj : [obj]
        def len = allItems.size()

        // use a bag so that cache hash key is not affected by file entries order
        def files = new ArrayBag(len)
        for( def item : allItems ) {
            files << normalizeInputToFile(item, "input.${++count}")
        }

        return files
    }

    protected singleItemOrList( List<FileHolder> items ) {
        assert items != null

        if( items.size() == 1 ) {
            return Paths.get(items[0].stageName)
        }

        def result = new ArrayList(items.size())
        for( int i=0; i<items.size(); i++ ) {
            result.add( Paths.get(items[i].stageName) )
        }
        return new BlankSeparatedList(result)
    }


    /**
     * An input file name may contain wildcards characters which have to be handled coherently
     * given the number of files specified.
     *
     * @param name A file name with may contain a wildcard character star {@code *} or question mark {@code ?}.
     *  Only one occurrence can be specified for star or question mark wildcards.
     *
     * @param value Any value that have to be managed as an input files. Values other than {@code Path} are converted
     * to a string value, using the {@code #toString} method and saved in the local file-system. Value of type {@code Collection}
     * are expanded to multiple values accordingly.
     *
     * @return
     */
    protected List<FileHolder> expandWildcards( String name, List<FileHolder> files ) {
        assert files != null

        // use an unordered so that cache hash key is not affected by file entries order
        final result = new ArrayBag()
        if( files.size()==0 ) { return result }

        if( !name || name == '*' ) {
            return files
        }

        if( name.endsWith('/*') ) {
            final p = name.lastIndexOf('/')
            final dir = name.substring(0,p)
            return files.collect { it.withName("${dir}/${it.storePath.name}") }
        }

        // no wildcards in the file name
        else if( !name.contains('*') && !name.contains('?') ) {
            /*
             * The name do not contain any wildcards *BUT* when multiple files are provide
             * it is managed like having a 'star' at the end of the file name
             */
            if( files.size()>1 ) {
                name += '*'
            }
            else {
                // just return that name
                result << files[0].withName(name)
                return result
            }
        }

        /*
         * The star wildcard: when a single item is provided, it is simply ignored
         * When a collection of files is provided, the name is expanded to the index number
         */
        if( name.contains('*') ) {
            if( files.size()>1 ) {
                def count = 1
                files.each { FileHolder holder ->
                    def stageName = name.replace('*', (count++).toString())
                    result << holder.withName(stageName)
                }
            }
            else {
                // there's just one value, remove the 'star' wildcards
                def stageName = name.replace('*','')
                result << files[0].withName(stageName)
            }
        }

        /*
         * The question mark wildcards *always* expand to an index number
         * as long as are the number of question mark characters
         */
        else if( name.contains('?') ) {
            def count = 1
            files.each { FileHolder holder ->
                String match = (name =~ /\?+/)[0]
                def replace = (count++).toString().padLeft(match.size(), '0')
                def stageName = name.replace(match, replace)
                result << holder.withName(stageName)
            }

        }

        // not a valid condition
        else {
            throw new IllegalStateException("Invalid file expansion for name: '$name'")
        }

        return result
    }


    /**
     * Given a map holding variables key-value pairs, create a script fragment
     * exporting the required environment variables
     */
    static String bashEnvironmentScript( Map<String,String> environment ) {

        final script = []
        environment.each { name, value ->
            if( name ==~ /[a-zA-Z_]+[a-zA-Z0-9_]*/ ) {
                script << "export $name=\"$value\""
            }
            else {
                log.trace "Illegal environment variable name: '${name}'"
            }
        }
        script << ''

        return script.join('\n')
    }


    protected decodeInputValue( InParam param, List values ) {

        def val = values[ param.index ]
        if( param.mapIndex != -1 ) {
            def list
            if( val instanceof Map )
                list = val.values()

            else if( val instanceof Collection )
                list = val
            else
                list = [val]

            try {
                return list[param.mapIndex]
            }
            catch( IndexOutOfBoundsException e ) {
                throw new ProcessException(e)
            }
        }

        return val
    }


    final protected int makeTaskContextStage1( TaskRun task, Map secondPass, List values ) {

        final contextMap = task.context
        int count = 0

        task.inputs.keySet().each { InParam param ->

            // add the value to the task instance
            def val = decodeInputValue(param,values)

            switch(param) {
                case EachInParam:
                case ValueInParam:
                    contextMap.put( param.name, val )
                    break

                case FileInParam:
                    secondPass[param] = val
                    return // <-- leave it, because we do not want to add this 'val' at this stage

                case StdInParam:
                case EnvInParam:
                    // nothing to do
                    break

                default:
                    log.debug "Unsupported input param type: ${param?.class?.simpleName}"
            }

            // add the value to the task instance context
            task.setInput(param, val)
        }

        return count
    }

    final protected void makeTaskContextStage2( TaskRun task, Map secondPass, int count ) {

        final ctx = task.context

        // -- all file parameters are processed in a second pass
        //    so that we can use resolve the variables that eventually are in the file name
        secondPass.each { FileInParam param, val ->

            def fileParam = param as FileInParam
            def normalized = normalizeInputToFiles(val,count)
            def resolved = expandWildcards( fileParam.getFilePattern(ctx), normalized )
            ctx.put( param.name, singleItemOrList(resolved) )
            count += resolved.size()

            // add the value to the task instance context
            task.setInput(param, resolved)
        }

        // -- set the delegate map as context ih the task config
        //    so that lazy directives will be resolved against it
        task.config.context = ctx

        // -- resolve the task command script
        task.resolve(taskBody)
    }

    final protected void makeTaskContextStage3( TaskRun task, HashCode hash, Path folder ) {

        // set hash-code & working directory
        task.hash = hash
        task.workDir = folder
        task.config.workDir = folder
        task.config.hash = hash.toString()
        task.config.name = task.getName()

    }

    final protected createTaskHashKey(TaskRun task) {

        List keys = [ session.uniqueId, name, task.source ]
        // add all the input name-value pairs to the key generator
        task.inputs.each {
            keys.add( it.key.name )
            keys.add( it.value )
        }

        // add all variable references in the task script but not declared as input/output
        def vars = getTaskGlobalVars(task)
        if( vars ) {
            log.trace "Task: $name > Adding script vars hash code: ${vars}"
            vars.each { k, v -> keys.add( k ); keys.add( v ) }
        }

        final mode = config.getHashMode()
        final hash = CacheHelper.hasher(keys, mode).hash()
        if( log.isTraceEnabled() ) {
            traceInputsHashes(task, keys, mode, hash)
        }
        return hash
    }

    private void traceInputsHashes( TaskRun task, List entries, CacheHelper.HashMode mode, hash ) {

        def buffer = new StringBuilder()
        buffer.append("[${task.name}] cache hash: ${hash}; mode: $mode; entries: \n")
        for( Object item : entries ) {
            buffer.append( "  ${CacheHelper.hasher(item, mode).hash()} [${item?.class?.name}] $item \n")
        }

        if( log.isTraceEnabled() )
            log.trace(buffer.toString())
    }

    final protected Map<String,Object> getTaskGlobalVars(TaskRun task) {
        getTaskGlobalVars( task.context.getVariableNames(), ownerScript.binding, task.context.getHolder() )
    }

    /**
     * @param variableNames The collection of variables referenced in the task script
     * @param binding The script global binding
     * @param context The task variable context
     * @return The set of task variables accessed in global script context and not declared as input/output
     */
    final protected Map<String,Object> getTaskGlobalVars(Set<String> variableNames, Binding binding, Map context) {
        final result = new HashMap(variableNames.size())
        final processName = name

        def itr = variableNames.iterator()
        while( itr.hasNext() ) {
            final varName = itr.next()

            final p = varName.indexOf('.')
            final baseName = p !=- 1 ? varName.substring(0,p) : varName

            if( context.containsKey(baseName) ) {
                // when the variable belong to the task local context just ignore it,
                // because it must has been provided as an input parameter
                continue
            }

            if( binding.hasVariable(baseName) ) {
                def value
                try {
                    if( p == -1 ) {
                        value = binding.getVariable(varName)
                    }
                    else {
                        value = grengine.run(varName, binding)
                    }
                }
                catch( MissingPropertyException | NullPointerException e ) {
                    value = null
                    if( log.isTraceEnabled() )
                    log.trace "Process `${processName}` cannot access global variable `$varName` -- Cause: ${e.message}"
                }

                // value for 'workDir' and 'baseDir' folders are added always as string
                // in order to avoid to invalid the cache key when resuming the execution
                if( varName=='workDir' || varName=='baseDir' )
                    value = value.toString()

                result.put( varName, value )
            }

        }
        return result
    }


    /**
     * Execute the specified task shell script
     *
     * @param script The script string to be execute, e.g. a BASH script
     * @return {@code TaskDef}
     */
    final protected void submitTask( TaskRun task, HashCode hash, Path folder ) {
        if( log.isTraceEnabled() )
            log.trace "[${task.name}] actual run folder: ${task.workDir}"

        makeTaskContextStage3(task, hash, folder)

        // add the task to the collection of running tasks
        session.dispatcher.submit(task, blocking)

    }

    protected boolean checkWhenGuard(TaskRun task) {

        try {
            def pass = task.config.getGuard('when')
            if( pass ) {
                return true
            }

            log.trace "Task ${task.name} is not executed becase `when` condition is not verified"
            finalizeTask0(task)
            return false
        }
        catch ( FailedGuardException error ) {
            handleException(error, task)
            return false
        }
    }

    /**
     * Finalize the task execution, checking the exit status
     * and binding output values accordingly
     *
     * @param task The {@code TaskRun} instance to finalize
     */
    @PackageScope
    final finalizeTask( TaskRun task ) {
        if( log.isTraceEnabled() )
            log.trace "finalizing process > ${task.name} -- $task"

        def fault = null
        try {
            if( task.type == ScriptType.SCRIPTLET ) {
                if( task.exitStatus == Integer.MAX_VALUE )
                    throw new ProcessFailedException("Process '${task.name}' terminated for an unknown reason -- Likely it has been terminated by the external system")

                if ( !task.isSuccess() )
                    throw new ProcessFailedException("Process '${task.name}' terminated with an error exit status")
            }

            // -- verify task exist status
            if( task.error )
                throw new ProcessFailedException("Process '${task.name}' failed", task.error)

            // -- if it's OK collect results and finalize
            collectOutputs(task)
        }
        catch ( Throwable error ) {
            fault = resumeOrDie(task, error)
        }

        // -- finalize the task
        if( fault != ErrorStrategy.RETRY )
            finalizeTask0(task)

        return fault
    }

    /**
     * Whenever the process can be cached
     */
    boolean isCacheable() {
        session.cacheable && config.cacheable
    }

    @PackageScope boolean isResumable() {
        isCacheable() && session.resumeMode
    }

    /**
     * Finalize the task execution, checking the exit status
     * and binding output values accordingly
     *
     * @param task The {@code TaskRun} instance to finalize
     * @param producedFiles The map of files to be bind the outputs
     */
    private void finalizeTask0( TaskRun task ) {
        if( log.isTraceEnabled() )
            log.trace "Finalize process > ${task.name}"

        // -- bind output (files)
        if( task.canBind ) {
            bindOutputs(task)
            publishOutputs(task)
        }

        // increment the number of processes executed
        state.update { StateObj it -> it.incCompleted() }
    }

    protected void terminateProcess() {
        log.debug "<${name}> Sending poison pills and terminating process"
        sendPoisonPill()
        session.processDeregister(this)
    }

    /**
     * Prints a warning message. This method uses a {@link Memoized} annotation
     * to avoid to show multiple times the same message when multiple process instances
     * invoke it.
     *
     * @param message The warning message to print
     */
    @Memoized
    def warn( String message ) {
        log.warn "Process `$name` $message"
        return null
    }


    /*
     * logger class for the *iterator* processor
     */
    class BaseProcessInterceptor extends DataflowEventAdapter {

        final List<DataflowChannel> inputs

        final boolean stopAfterFirstRun

        final int len

        final DataflowQueue control

        final int first

        BaseProcessInterceptor( List<DataflowChannel> inputs, boolean stop ) {
            this.inputs = new ArrayList<>(inputs)
            this.stopAfterFirstRun = stop
            this.len = inputs.size()
            this.control = (DataflowQueue)inputs.get(len-1)
            this.first = inputs.findIndexOf { it instanceof DataflowQueue }
        }

        @Override
        public Object messageArrived(final DataflowProcessor processor, final DataflowReadChannel<Object> channel, final int index, final Object message) {
            if( len == 1 || stopAfterFirstRun ) {
                // -- kill itself
                control.bind(PoisonPill.instance)
            }
            else if( index == first ) {
                // -- keep things rolling
                control.bind(Boolean.TRUE)
            }

            return message;
        }

    }

    /**
     *  Intercept dataflow process events
     */
    class TaskProcessorInterceptor extends BaseProcessInterceptor {

        TaskProcessorInterceptor(List<DataflowChannel> inputs, boolean stop) {
            super(inputs, stop)
        }

        @Override
        public List<Object> beforeRun(final DataflowProcessor processor, final List<Object> messages) {
            log.trace "<${name}> Before run -- messages: ${messages}"
            // the counter must be incremented here, otherwise it won't be consistent
            state.update { StateObj it -> it.incSubmitted() }
            return messages;
        }


        @Override
        void afterRun(DataflowProcessor processor, List<Object> messages) {
            log.trace "<${name}> After run"
            currentTask.remove()
        }

        @Override
        public Object messageArrived(final DataflowProcessor processor, final DataflowReadChannel<Object> channel, final int index, final Object message) {
            if( log.isTraceEnabled() ) {
                def channelName = config.getInputs()?.names?.get(index)
                def taskName = currentTask.get()?.name ?: name
                log.trace "<${taskName}> Message arrived -- ${channelName} => ${message}"
            }

            super.messageArrived(processor, channel, index, message)
        }

        @Override
        public Object controlMessageArrived(final DataflowProcessor processor, final DataflowReadChannel<Object> channel, final int index, final Object message) {
            if( log.isTraceEnabled() ) {
                def channelName = config.getInputs()?.names?.get(index)
                def taskName = currentTask.get()?.name ?: name
                log.trace "<${taskName}> Control message arrived ${channelName} => ${message}"
            }

            super.controlMessageArrived(processor, channel, index, message)

            if( message == PoisonPill.instance ) {
                log.debug "<${name}> Poison pill arrived"
                state.update { StateObj it -> it.poison() }
            }

            return message
        }

        @Override
        public void afterStop(final DataflowProcessor processor) {
            log.debug "<${name}> After stop"
        }

        /**
         * Invoked if an exception occurs. Unless overridden by subclasses this implementation returns true to terminate the operator.
         * If any of the listeners returns true, the operator will terminate.
         * Exceptions outside of the operator's body or listeners' messageSentOut() handlers will terminate the operator irrespective of the listeners' votes.
         * When using maxForks, the method may be invoked from threads running the forks.
         * @param processor
         * @param error
         * @return
         */
        public boolean onException(final DataflowProcessor processor, final Throwable error) {
            // return `true` to terminate the dataflow processor
            handleException( error, currentTask.get() )
        }
    }
}
