package com.yida.scala.rpc

import akka.actor.{Actor, ActorRef, ActorSelection, ActorSystem, Props}
import com.typesafe.config.ConfigFactory

//todo:利用akka的actor模型，实现2个进程之间的通信----worker端
class Worker extends Actor{
  println("Worker constructor invoked")

  override def preStart(): Unit = {
    println("preStart Method invoked")
    //todo:通过actorContext上下文对象调用actorSelection，需要：通信协议，master的ip地址、端口、master对应的actorSystem，actor层级，最终拿到master actor的引用
    val master: ActorSelection = context.actorSelection("akka.tcp://masterActorSystem@192.168.75.79:8888/user/masterActor")
    //向master发送信息
    master ! "content"
  }
  override def receive: Receive = {
    case "content" =>{
      println("where is my bear ?")
    }
    case "success" => {
      println("我已经注册成功")
    }
  }
}

object Worker{

  def main(args: Array[String]): Unit = {
    val host = args(0)
    val port = args(1)
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
    val workerActor: ActorRef = workerActorSystem.actorOf(Props(new Worker),"workerActor")
    //发送消息
    workerActor ! "content"
  }
}