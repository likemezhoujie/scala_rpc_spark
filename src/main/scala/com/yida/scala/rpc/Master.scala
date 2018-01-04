package com.yida.scala.rpc

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import com.typesafe.config.ConfigFactory
//利用akka的actor模型，实现2个进程间的通信-----------Master 端
class Master extends Actor {
  println("Master constructor invoked")
  //会在构造代码块之后调用，并且只会被执行一次
  override def preStart(): Unit = {
    println("preStaer Method invoked")
  }
  //他会在prestart方法执行后被调用，并且是一直循环被执行
  override def receive: Receive = {
    case "content" => {
      println("a client connected")
      sender ! "success"
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
    println(host)
    //利用ConfigFactory对象的parseString方法创建config对象，然后构建配置信息
    val config = ConfigFactory.parseString(configStr)
    //创建ActorSystem，它是整个进程中的老大，负责创建和监督actor，它是单利对象
    val masterActorSystem: ActorSystem = ActorSystem("masterActorSystem",config)
    //通过masterActorSystem对象创建actor
    val masterActor: ActorRef = masterActorSystem.actorOf(Props(new Master),"masterActor")
    //向master actor发送消息
    //masterActor ! "content"0
  }
}