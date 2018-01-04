package com.yida.scala.spark

//将消息封装在样例类中
trait RemoteMessage extends Serializable{

}
//worker---------------master   woker向master 发送注册信息，由于不在同一进程中，需要实现序列化
case class RegisterMessage(val workerId:String,val memory:Int,val cores:Int) extends RemoteMessage
//用于存放worker的资源信息
case class WorkerInfo(workerId:String,memory:Int,cores:Int){
  //定义一个变量，用于存放每一次发送心跳的时间
  var lastHeartBeat:Long=_

  //重写toString方法
  override def toString: String = {
    s"workerId:$workerId,memory:$memory,cores:$cores"
  }
}
//master---------------worker  master向worker发送注册成功信息，由于不在同一进程中，需要实现序列化
case class RegisteredMessage(message:String) extends RemoteMessage
//worker---------------worker  worker给自己发送信息
case object HeartBeat
//worker---------------master   worker向master发送心跳，由于不在同一个进程中，需要序列化
case class SendHeart(workerId:String) extends RemoteMessage
//master---------------master  master定时检查
case object CheckTimeOutWorker