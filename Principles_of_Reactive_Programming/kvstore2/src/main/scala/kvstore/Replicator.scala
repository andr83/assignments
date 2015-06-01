package kvstore

import akka.actor.Props
import akka.actor.Actor
import akka.actor.ActorRef
import scala.concurrent.duration._

object Replicator {
  case class Replicate(key: String, valueOption: Option[String], id: Long)
  case class Replicated(key: String, id: Long)
  case class ReplicaFailed(id: Long)
  
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
      val revId = nextSeq
      acks += (revId -> (sender(), rep))
      sendSnapshot(rep, revId)
    case SnapshotAck(key, revId) if acks.contains(revId) =>
      val (master, rep) = acks.get(revId).get
      acks -= revId
      master ! Replicated(rep.key, rep.id)
    case SnapshotCheck(revId, false) if acks.contains(revId) =>
      val rep = acks.get(revId).get._2
      sendSnapshot(rep, revId)
    case SnapshotCheck(revId, true) if acks.contains(revId) =>
      val (master, rep) = acks.get(revId).get
      acks -= revId
      replica ! ReplicaFailed(revId)
      master ! ReplicaFailed(rep.id)
  }

  def sendSnapshot(rep: Replicate, revId: Long) {
    replica ! Snapshot(rep.key, rep.valueOption, revId)
    context.system.scheduler.scheduleOnce(100.milliseconds, self, SnapshotCheck(revId))
    context.system.scheduler.scheduleOnce(1.second, self, SnapshotCheck(revId, stop = true))
  }

}
