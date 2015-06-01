package kvstore

import akka.actor.Props
import akka.actor.Actor
import akka.actor.ActorRef
import scala.concurrent.duration._

object Replicator {
  case class Replicate(key: String, valueOption: Option[String], id: Long)
  case class Replicated(key: String, id: Long)
  
  case class Snapshot(key: String, valueOption: Option[String], seq: Long)
  case class SnapshotAck(key: String, seq: Long)
  case class SnapshotCheck(seq: Long, stop: Boolean = false)

  def props(replica: ActorRef): Props = Props(new Replicator(replica))
}

class Replicator(val replica: ActorRef) extends Actor {
  import Replicator._
  import context.dispatcher
  
  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */

  // map from sequence number to pair of sender and request
  var acks = Map.empty[Long, (ActorRef, Replicate)]
  // a sequence of not-yet-sent snapshots (you can disregard this if not implementing batching)
  var pending = Vector.empty[Snapshot]
  
  var _seqCounter = 0L
  def nextSeq = {
    val ret = _seqCounter
    _seqCounter += 1
    ret
  }

  
  /* TODO Behavior for the Replicator. */
  def receive: Receive = {
    case rep @ Replicate(key, value, id) =>
      val seq = nextSeq
      acks += (seq -> (sender(), rep))
      sendSnapshot(seq)
      context.system.scheduler.scheduleOnce(1.second, self, SnapshotCheck(seq, stop = true))

    case SnapshotAck(key, seq) if acks.contains(seq) =>
      val (requester, rep) = acks.get(seq).get
      acks -= seq
      requester ! Replicated(rep.key, rep.id)

    case SnapshotCheck(seq, false) => sendSnapshot(seq)

    case SnapshotCheck(seq, true) if acks.contains(seq) =>
      acks -= seq
  }

  def sendSnapshot(seq: Long) = if (acks.contains(seq)) {
    val Replicate(key, valueOption, _) = acks.get(seq).get._2
    replica ! Snapshot(key, valueOption, seq)
    context.system.scheduler.scheduleOnce(100.milliseconds, self, SnapshotCheck(seq))
  }

}
