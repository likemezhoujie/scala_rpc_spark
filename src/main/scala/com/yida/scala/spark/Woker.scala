package com.yida.scala.spark


import java.util.UUID

import akka.actor.{Actor, ActorRef, ActorSelection, ActorSystem, Props}
import com.typesafe.config.ConfigFactory
import scala.concurrent.duration._

//利用akka的actor模型，实现简易版的spark通信框架-----------Worker 端
class Worker(memory:Int,cores:Int,masterHost:String,masterPort:String) extends Actor{
  println("Worker constructor invoked")
  //定义workerId
  private val workerId: String = UUID.randomUUID().toString
  //定义多久发送一次心跳
  val HEART_INTERVAL = 10000 //10秒
  var master: ActorSelection = _
  override def preStart(): Unit = {
    println("preStart Method invoked")
    //todo:通过actorContext上下文对象调用actorSelection，需要：通信协议，master的ip地址、端口、master对应的actorSystem，actor层级，最终拿到master actor的引用
    master = context.actorSelection(s"akka.tcp://masterActorSystem@$masterHost:$masterPort/user/masterActor")
    //向master发送注册信息，包括worker的id，memory、cores封装在一个样例类中
    master ! RegisterMessage(workerId,memory,cores)
  }
  override def receive: Receive = {
    case RegisteredMessage(message) => {
      println(message+ "我已经注册成功")
      //x向master发送心跳
      import context.dispatcher
      context.system.scheduler.schedule(0 millis,HEART_INTERVAL millis,self,HeartBeat)
    }
      //接收自己的心跳信息
    case HeartBeat =>{
      master ! SendHeart(workerId)
    }
  }
}

object Worker{

  def main(args: Array[String]): Unit = {
    //worker的ip
    val host = args(0)
    //worker的端口
    val port = args(1)
    //worker的内存
    val memory = args(2).toInt
    //worker的核数
    val cores = args(3).toInt
    //master的ip地址
    val masterHost = args(4)
    //master的端口
    val masterPort = args(5)
    val  configStr =
      s"""
         |akka.actor.provider = "akka.remote.RemoteActorRefProvider"
         |akka.remote.netty.tcp.hostname = "$host"
         |akka.remote.netty.tcp.port = "$port"
      """.stripMargin
    var config = ConfigFactory.parseString(configStr)
    //todo:1.创建actorSystem对象，他是整个进程中的老大
    val workerActorSystem: ActorSystem = ActorSystem("workerActorSystem",config)
    //利用workeractorsystem创建worker的actor
    val workerActor: ActorRef = workerActorSystem.actorOf(Props(new Worker(memory,cores,masterHost,masterPort)),"workerActor")
    //发送消息
    workerActor ! "content"
  }
}