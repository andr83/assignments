package kvstore

import akka.actor._
import kvstore.Arbiter._
import scala.collection.immutable.Queue
import akka.actor.SupervisorStrategy.Restart
import scala.annotation.tailrec
import akka.pattern.{ ask, pipe }
import scala.concurrent.duration._
import akka.util.Timeout

object Replica {
  sealed trait Operation {
    def key: String
    def id: Long
  }
  case class Insert(key: String, value: String, id: Long) extends Operation
  case class Remove(key: String, id: Long) extends Operation
  case class Get(key: String, id: Long) extends Operation

  sealed trait OperationReply
  case class OperationAck(id: Long) extends OperationReply
  case class OperationFailed(id: Long) extends OperationReply
  case class GetResult(key: String, valueOption: Option[String], id: Long) extends OperationReply

  case class Acknowledge(key: String, valueOption: Option[String], id: Long)
  case class Acknowledged(id: Long)
  case class AcknowledgeFailed(id: Long)
  case class AcknowledgeCheck(id: Long, stop: Boolean = false)

  def props(arbiter: ActorRef, persistenceProps: Props): Props = Props(new Replica(arbiter, persistenceProps))
}

class Replica(val arbiter: ActorRef, persistenceProps: Props) extends Actor {
  import Replica._
  import Replicator._
  import Persistence._
//  import Acknowledger._
  import context.dispatcher

  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */
  
  var kv = Map.empty[String, String]
  // a map from secondary replicas to replicators
  var secondaries = Map.empty[ActorRef, ActorRef]
  // the current set of replicators
  var replicators = Set.empty[ActorRef]

  // id, (client, persist, replicators, persisted)
  var acks = Map.empty[Long, (ActorRef, Acknowledge, Set[ActorRef], Boolean)]

  var expectedSeq = 0L

  var persistence = context.system.actorOf(persistenceProps)

  override val supervisorStrategy = OneForOneStrategy(maxNrOfRetries = 10, withinTimeRange = 1.second) {
    case _: PersistenceException => Restart
  }

  override def preStart() {
    arbiter ! Join
  }

//  lazy val acknowledger = context.system.actorOf(Props(new Acknowledger(self, persistence, receive == leader)))

  def receive = {
    case JoinedPrimary   => context.become(leader)
    case JoinedSecondary => context.become(replica)
  }

  /* TODO Behavior for  the leader role. */
  val leader: Receive = modificationHandler orElse readHandler orElse acknowledgeLeaderHandler orElse {
    case Acknowledged(id) if acks.contains(id) =>
      val requester = acks.get(id).get._1
      acks -= id
      requester ! OperationAck(id)

    case AcknowledgeFailed(id) if acks.contains(id) =>
      val requester = acks.get(id).get._1
      acks -= id
      requester ! OperationFailed(id)

    case Replicas(replicas) =>
      val slaves = replicas - self

      // which one to remove
      (secondaries.keySet -- slaves).foreach(secondary => {
        val replicator = secondaries.get(secondary).get
        val ids = for (
          (id, (requester, a, replicators, isPersist)) <- acks
          if replicators.contains(replicator)
        ) yield {
            acks += (id ->(requester, a, replicators - replicator, isPersist))
            id
          }
        ids.foreach(checkAcknowledge)
        context.stop(secondaries.get(secondary).get)
      })

      // which one are new
      val toReplicate = slaves -- secondaries.keySet

      secondaries = slaves.map(replica => {
        replica -> secondaries.getOrElse(replica, context.system.actorOf(Replicator.props(replica)))
      }).toMap
      replicators = secondaries.values.toSet

      if (toReplicate.nonEmpty) kv.foldLeft[Int](0) ({
        case (i: Int, (key: String, value: String)) =>
          val r = Replicate(key, Some(value), i)
          toReplicate.foreach(secondaries.get(_).get ! r)
          i + 1
      })
  }

  /* TODO Behavior for the replica role. */
  val replica: Receive = readHandler orElse acknowledgeReplicaHandler orElse {
    case Snapshot(key, valueOption, seq) if seq == expectedSeq =>
      valueOption match {
        case Some(value) => kv += (key -> value)
        case None => kv -= key
      }
      acknowledge(sender(), key, valueOption, seq)

    case Snapshot(key, valueOption, seq) =>
      if (seq < expectedSeq)
        sender() ! SnapshotAck(key, seq)

    case Acknowledged(seq) if acks.contains(seq) =>
      val (requester, a, _, _) = acks.get(seq).get
      acks -= seq
      expectedSeq = Math.max(expectedSeq, seq + 1)
      requester ! SnapshotAck(a.key, seq)

    case AcknowledgeFailed(seq) if acks.contains(seq) =>
      val (_, a, _, _) = acks.get(seq).get
      acks -= seq
  }

  def modificationHandler: Receive = {
    case Insert(key, value, id) =>
      kv += (key -> value)
      acknowledge(sender(), key, Some(value), id)

    case Remove(key, id) =>
      kv -= key
      acknowledge(sender(), key, None, id)
  }

  def readHandler: Receive = {
    case Get(key, id) =>
      val v= kv.get(key)
      sender() ! GetResult(key, kv.get(key), id)
  }

  def acknowledgeLeaderHandler: Receive = acknowledgeReplicaHandler orElse {
    case Replicated(key, id) if acks.contains(id) =>
      val (requester, a, replicators, isPersisted) = acks.get(id).get
      acks += (id ->(requester, a, replicators - sender(), isPersisted))
      checkAcknowledge(id)
  }

  def acknowledgeReplicaHandler: Receive = {
    case AcknowledgeCheck(id, false) if acks.contains(id) => acknowledge(id)

    case AcknowledgeCheck(id, true) if acks.contains(id) =>
      self ! AcknowledgeFailed(id)

    case Persisted(key, id) if acks.contains(id) =>
      val (requester, a, replicators, _) = acks.get(id).get
      acks += (id -> (requester, a, replicators, true))
      checkAcknowledge(id)
  }

  def checkAcknowledge(id: Long) = if (acks.isDefinedAt(id)) {
    val (_, _ , replicators, isPersisted) = acks.get(id).get
    if (replicators.isEmpty && isPersisted) {
      self ! Acknowledged(id)
    }
  }

  def acknowledge(requester: ActorRef, key: String, valueOption: Option[String], id: Long): Unit = {
    val a = Acknowledge(key, valueOption, id)
    acks += (id -> (requester, a, replicators, false))
    replicators.foreach(_ ! Replicate(a.key, a.valueOption, id))
    acknowledge(id)
    context.system.scheduler.scheduleOnce(1.seconds, self, AcknowledgeCheck(id, stop = true))
  }

  def acknowledge(id: Long) = if (acks.contains(id)) {
    val (_, a , _, isPersisted) = acks.get(id).get
    if (!isPersisted) {
      persistence ! Persist(a.key, a.valueOption, id)
    }
    context.system.scheduler.scheduleOnce(100.milliseconds, self, AcknowledgeCheck(id))
  }

}

