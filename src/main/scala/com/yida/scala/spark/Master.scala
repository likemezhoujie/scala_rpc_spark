package com.yida.scala.spark


import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import com.typesafe.config.ConfigFactory

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration._
//利用akka的actor模型，实现简易版的spark通信框架-----------Master 端
class Master extends Actor {
  println("Master constructor invoked")
  //会在构造代码块之后调用，并且只会被执行一次
  //定义一个map，用于存放对应的worker信息，key：workerId，value：WorkerInfo
  private val workersMap: mutable.HashMap[String, WorkerInfo] = new mutable.HashMap[String,WorkerInfo]()
  //定义一个list用于 存放所有的worker信息，方便后期按照worker的资源进行排序
  private val workersList: ListBuffer[WorkerInfo] = new ListBuffer[WorkerInfo]
  //定义一个常量，表示每隔多久定时检查
  val CHECK_INTERVAL=15000 //15秒

  override def preStart(): Unit = {
    println("preStaer Method invoked")
    //master定时的清除超时的worker
    //手动导入隐式转换
    import context.dispatcher
    context.system.scheduler.schedule(0 millis,CHECK_INTERVAL millis,self,CheckTimeOutWorker)
  }
  //他会在prestart方法执行后被调用，并且是一直循环被执行
  override def receive: Receive = {
    //接收worker的注册信息
    case RegisterMessage(workerId,memory,cores) =>{
      //判断当前worker是否注册，没有注册就添加到map中
        if(!workersMap.contains(workerId)){
          val workerInfo = new WorkerInfo(workerId,memory,cores)
          //添加workerInfo信息到map中
          workersMap(workerId) = workerInfo
          //添加workerInfo信息到list
          workersList.append(workerInfo)
        }
      //反馈注册成功信息给worker
      sender ! RegisteredMessage("success")
    }
      //接收worker的心跳信息
    case SendHeart(workerId) =>{
      //判断是否已经注册
      if(workersMap.contains(workerId)){
        //获取当前系统时间
        val lastTime: Long = System.currentTimeMillis()
        //记录当前worker发送心跳的时间
        workersMap(workerId).lastHeartBeat=lastTime
      }
    }
      //判断超时的worker
    case CheckTimeOutWorker =>{
      //获取当前系统时间
      val nowTime: Long = System.currentTimeMillis()
      //nowTime -   worker上一次注册时间 > 检查时间间隔
      val outTimeWorkers: ListBuffer[WorkerInfo] = workersList.filter(x=>nowTime-x.lastHeartBeat>CHECK_INTERVAL)
      //清楚超时worker
      for(t <- outTimeWorkers){
        //在map中移除超时的worker信息
        workersMap -= t.workerId
        //在list里移除超时的worker信息
        workersList -= t
        println("超时的workerId"+t.workerId)
      }
      println("存活的worker数量："+workersList.size)
      //按照worker中内存大小降序排序
      workersList.sortBy(x=>x.memory).reverse.toString()
    }
  }
}

object Master{

  def main(args: Array[String]): Unit = {
    //定义master的ip地址
    val host = args(0)
    //定义master的port端口
    val port = args(1)
    val configStr =
      s"""
         |akka.actor.provider = "akka.remote.RemoteActorRefProvider"
         |akka.remote.netty.tcp.hostname = "$host"
         |akka.remote.netty.tcp.port = "$port"
      """.stripMargin
    //利用ConfigFactory对象的parseString方法创建config对象，然后构建配置信息
    val config = ConfigFactory.parseString(configStr)
    //创建ActorSystem，它是整个进程中的老大，负责创建和监督actor，它是单利对象
    val masterActorSystem: ActorSystem = ActorSystem("masterActorSystem",config)
    //通过masterActorSystem对象创建actor
    val masterActor: ActorRef = masterActorSystem.actorOf(Props(new Master),"masterActor")
    //向master actor发送消息
    //masterActor ! "content"
  }
}