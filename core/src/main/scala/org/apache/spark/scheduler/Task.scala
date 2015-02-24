/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.scheduler

import java.io._
import java.nio.ByteBuffer
import java.util.Scanner

import org.jocl.CL._
import org.jocl._

import scala.collection.mutable.HashMap

import org.apache.spark.{TaskContextHelper, TaskContextImpl, TaskContext, Logging}
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.serializer.SerializerInstance
import org.apache.spark.util.ByteBufferInputStream
import org.apache.spark.util.Utils


/**
 * A unit of execution. We have two kinds of Task's in Spark:
 * - [[org.apache.spark.scheduler.ShuffleMapTask]]
 * - [[org.apache.spark.scheduler.ResultTask]]
 *
 * A Spark job consists of one or more stages. The very last stage in a job consists of multiple
 * ResultTasks, while earlier stages consist of ShuffleMapTasks. A ResultTask executes the task
 * and sends the task output back to the driver application. A ShuffleMapTask executes the task
 * and divides the task output to multiple buckets (based on the task's partitioner).
 *
 * @param stageId id of the stage this task belongs to
 * @param partitionId index of the number in the RDD
 */
private[spark] abstract class Task[T](val stageId: Int, var partitionId: Int) extends Serializable {

  final def run(attemptId: Long): T = {
    context = new TaskContextImpl(stageId, partitionId, attemptId, false)
    TaskContextHelper.setTaskContext(context)
    context.taskMetrics.hostname = Utils.localHostName()
    taskThread = Thread.currentThread()
    if (_killed) {
      kill(interruptThread = false)
    }
    try {
      runTask(context)
    } finally {
      context.markTaskCompleted()
      TaskContextHelper.unset()
    }
  }

  def runTask(context: TaskContext): T

  def preferredLocations: Seq[TaskLocation] = Nil

  // Map output tracker epoch. Will be set by TaskScheduler.
  var epoch: Long = -1

  var metrics: Option[TaskMetrics] = None

  // Task context, to be initialized in run().
  @transient protected var context: TaskContextImpl = _

  // The actual Thread on which the task is running, if any. Initialized in run().
  @volatile @transient private var taskThread: Thread = _

  // A flag to indicate whether the task is killed. This is used in case context is not yet
  // initialized when kill() is invoked.
  @volatile @transient private var _killed = false

  /**
   * Whether the task has been killed.
   */
  def killed: Boolean = _killed

  /**
   * Kills a task by setting the interrupted flag to true. This relies on the upper level Spark
   * code and user code to properly handle the flag. This function should be idempotent so it can
   * be called multiple times.
   * If interruptThread is true, we will also call Thread.interrupt() on the Task's executor thread.
   */
  def kill(interruptThread: Boolean) {
    _killed = true
    if (context != null) {
      context.markInterrupted()
    }
    if (interruptThread && taskThread != null) {
      taskThread.interrupt()
    }
  }

}

/**
 * Handles transmission of tasks and their dependencies, because this can be slightly tricky. We
 * need to send the list of JARs and files added to the SparkContext with each task to ensure that
 * worker nodes find out about it, but we can't make it part of the Task because the user's code in
 * the task might depend on one of the JARs. Thus we serialize each task as multiple objects, by
 * first writing out its dependencies.
 */
private[spark] object Task {
  /**
   * Serialize a task and the current app dependencies (files and JARs added to the SparkContext)
   */
  def serializeWithDependencies(
      task: Task[_],
      currentFiles: HashMap[String, Long],
      currentJars: HashMap[String, Long],
      serializer: SerializerInstance)
    : ByteBuffer = {

    val out = new ByteArrayOutputStream(4096)
    val dataOut = new DataOutputStream(out)

    // Write currentFiles
    dataOut.writeInt(currentFiles.size)
    for ((name, timestamp) <- currentFiles) {
      dataOut.writeUTF(name)
      dataOut.writeLong(timestamp)
    }

    // Write currentJars
    dataOut.writeInt(currentJars.size)
    for ((name, timestamp) <- currentJars) {
      dataOut.writeUTF(name)
      dataOut.writeLong(timestamp)
    }

    // Write the task itself and finish
    dataOut.flush()
    val taskBytes = serializer.serialize(task).array()
    out.write(taskBytes)
    ByteBuffer.wrap(out.toByteArray)
  }

  /**
   * Deserialize the list of dependencies in a task serialized with serializeWithDependencies,
   * and return the task itself as a serialized ByteBuffer. The caller can then update its
   * ClassLoaders and deserialize the task.
   *
   * @return (taskFiles, taskJars, taskBytes)
   */
  def deserializeWithDependencies(serializedTask: ByteBuffer)
    : (HashMap[String, Long], HashMap[String, Long], ByteBuffer) = {

    val in = new ByteBufferInputStream(serializedTask)
    val dataIn = new DataInputStream(in)

    // Read task's files
    val taskFiles = new HashMap[String, Long]()
    val numFiles = dataIn.readInt()
    for (i <- 0 until numFiles) {
      taskFiles(dataIn.readUTF()) = dataIn.readLong()
    }

    // Read task's JARs
    val taskJars = new HashMap[String, Long]()
    val numJars = dataIn.readInt()
    for (i <- 0 until numJars) {
      taskJars(dataIn.readUTF()) = dataIn.readLong()
    }

    // Create a sub-buffer for the rest of the data, which is the serialized Task object
    val subBuffer = serializedTask.slice()  // ByteBufferInputStream will have read just up to task
    (taskFiles, taskJars, subBuffer)
  }
}

class OpenCLContext extends Serializable with Logging{

  /**
   * This method should be called after the task is sent to workers because all the fields in this
   * class are @transient and will be lost when serialized.
   * @param path
   */
  def initOpenCL(path: String): OpenCLContext = {
    if (context == null) {
      val platformIndex: Int = 0
      val deviceType: Long = CL_DEVICE_TYPE_ALL
      val deviceIndex: Int = 0
      CL.setExceptionsEnabled(true)
      val numPlatformsArray = new Array[Int](1) // TODO we only need one device at this time
      clGetPlatformIDs(0, null, numPlatformsArray)
      val numPlatforms: Int = numPlatformsArray(0)
      val platforms = new Array[cl_platform_id](numPlatforms)
      clGetPlatformIDs(platforms.length, platforms, null)
      val platform: cl_platform_id = platforms(platformIndex)
      val contextProperties: cl_context_properties = new cl_context_properties
      contextProperties.addProperty(CL_CONTEXT_PLATFORM, platform)
      val numDevicesArray = new Array[Int](1)
      clGetDeviceIDs(platform, deviceType, 0, null, numDevicesArray)
      val numDevices: Int = numDevicesArray(0)
      val devices = new Array[cl_device_id](numDevices)
      clGetDeviceIDs(platform, deviceType, numDevices, devices, null)
      val device: cl_device_id = devices(deviceIndex)
      context = clCreateContext(contextProperties, 1, Array[cl_device_id](device), null, null, null)
      queue = clCreateCommandQueue(context, device, 0, null)
      try {
        val kernelFile: InputStream = getClass.getResourceAsStream(path)
        programSource = new Scanner(kernelFile).useDelimiter("\\Z").next()
      }
      catch {
        case e: FileNotFoundException => {
          System.out.println("Kernel file not found.\n")
        }
      }
      program = clCreateProgramWithSource(context, 1, Array[String](programSource), null, null)
      clBuildProgram(program, 0, null, null, null, null)
    }
    this
  }
  
  def close(): Unit = {
    clReleaseProgram(program)
    clReleaseCommandQueue(queue)
    clReleaseContext(context)
  }


  @transient var context: cl_context = null
  @transient var queue: cl_command_queue = null
  @transient var program: cl_program = null
  @transient var programSource: String = null
  
  // For profiling purposes only
  var pciTransferTime = 0L
  var pciTransferBytes = 0L
  var diskReadTime = 0L
}

object OpenCLContextSingletone extends Serializable with Logging {
  lazy val openClContext = {
    logInfo("Going to initialize OpenCLContextSingletone ")
    val _context = new OpenCLContext
  _context
  }

}
