package kvstore

import akka.actor.{ OneForOneStrategy, Props, ActorRef, Actor }
import kvstore.Arbiter._
import scala.collection.immutable.Queue
import akka.actor.SupervisorStrategy.{Resume, Restart}
import scala.annotation.tailrec
import akka.pattern.{ ask, pipe }
import scala.concurrent.duration._
import akka.actor.PoisonPill
import akka.actor.OneForOneStrategy
import akka.actor.SupervisorStrategy
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

  def props(arbiter: ActorRef, persistenceProps: Props): Props = Props(new Replica(arbiter, persistenceProps))
}

class Replica(val arbiter: ActorRef, persistenceProps: Props) extends Actor {
  import Replica._
  import Replicator._
  import Persistence._
  import context.dispatcher

  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */
  
  var kv = Map.empty[String, String]
  // a map from secondary replicas to replicators
  var secondaries = Map.empty[ActorRef, ActorRef]
  // the current set of replicators
  var replicators = Set.empty[ActorRef]

  var persistence = context.system.actorOf(persistenceProps)

  // a map of persistence operations from request id to sender
  var ps = Map.empty[Long, (ActorRef, Persist)]
  // a map from request id to set of replicators
  var acks = Map.empty[Long, (Option[ActorRef], Set[ActorRef], Replicate)]

  var revId = 0L
  var syncRev = -1L

  override val supervisorStrategy = OneForOneStrategy(maxNrOfRetries = 10, withinTimeRange = 1.second) {
    case _: PersistenceException => Restart
    case _: Exception => Restart
  }

  override def preStart() {
    arbiter ! Join
  }

  def receive = {
    case JoinedPrimary   => context.become(leader)
    case JoinedSecondary => context.become(replica)
  }

  /* TODO Behavior for  the leader role. */
  val leader: Receive = readHandler orElse {
    case Insert(key, value, id) =>
//      kv = kv + (key -> value)
      sendPersist(sender(), Persist(key, Some(value), id))
      sendReplicate(sender(), Replicate(key, Some(value), id))
      //replicators foreach(_ ! Snapshot(key, Some(value), id))
      //sender() ! OperationAck(id)

    case Remove(key, id) =>
//      kv = kv - key
      sendPersist(sender(), Persist(key, None, id))
      sendReplicate(sender(), Replicate(key, None, id))
      //replicators foreach(_ ! Snapshot(key, None, id))
//      sender() ! OperationAck(id)

    case Persisted(key, id) if ps.contains(id) =>
      val (client, p) = ps.get(id).get
      ps -= id
      if (!acks.contains(id)) {
        p.valueOption match {
          case Some(value) => kv += (key -> value)
          case None => kv -= key
        }
        client ! OperationAck(id)
      }

    case PersistCheck(id, false) if ps.contains(id) =>
      val (client, p: Persist) = ps.get(id).get
      sendPersist(client, p)

    case PersistCheck(id, true) if ps.contains(id) =>
      val (client, p) = ps.get(id).get
      ps -= id
      acks -= id
      sendReplicate(None, Replicate(p.key, kv.get(p.key), syncRev), replicators)
      syncRev -= 1
      client ! OperationFailed(id)

    case Replicated(key, id) if acks.contains(id) => onReplicated(key, id, sender())

    case ReplicaFailed(id) if acks.contains(id) =>
      val (clientOption, _, rep) = acks.get(id).get
      acks -= id
      ps -= id
      sendReplicate(None, Replicate(rep.key, kv.get(rep.key), syncRev), replicators)
      syncRev -= 1
      clientOption.foreach(_ ! OperationFailed(id))

    case Replicas(replicas) =>
      val slaves = replicas - self
      val toRemove = secondaries.keys.toSet -- slaves
      val toCreate = slaves -- secondaries.values.toSet
      toRemove.foreach(secondary => {
        val replicator = secondaries.get(secondary).get
        for (
          (id, (client, repls, replicate)) <- acks
          if repls.contains(replicator)
        ) {
          onReplicated(replicate.key, replicate.id, replicator)
        }
        secondaries -= secondary
        context.system.stop(secondary)
        context.system.stop(replicator)
      })
      var newReplicators = Set.empty[ActorRef]
      toCreate.foreach(secondary => {
        val replicator = context.system.actorOf(Replicator.props(secondary))
        newReplicators += replicator
        secondaries += (secondary -> replicator)
      })

      kv.foreach {case (key, value) =>
        sendReplicate(None, Replicate(key, Some(value), syncRev), newReplicators)
        syncRev -= 1
      }
//      secondaries = slaves.map(rep => rep -> context.system.actorOf(Replicator.props(rep))).toMap
      replicators = secondaries.values.toSet
  }

  /* TODO Behavior for the replica role. */
  val replica: Receive = readHandler orElse {
    case Snapshot(key, valueOption, seq) if seq == revId =>
      valueOption match {
        case Some(value) => kv += (key -> value)
        case None => kv -= key
      }
//      revId += 1
      sendPersist(sender(), Persist(key, valueOption, seq))
//      sender ! SnapshotAck(key, seq)
    case Snapshot(key, valueOption, seq) if seq < revId =>
      sender ! SnapshotAck(key, seq)

    case Persisted(key, seq)  =>
      if (ps.contains(seq)) {
        val (client, p) = ps.get(seq).get
        ps -= seq
        if (seq < revId + 1) {
          revId = Math.max(revId - 1, seq + 1)
          client ! SnapshotAck(key, seq)
        }
      }
//      p.valueOption match {
//        case Some(value) => kv += (key -> value)
//        case None => kv -= key
//      }

    case PersistCheck(seq, false) if ps.contains(seq) =>
      val (client, p: Persist) = ps.get(seq).get
      sendPersist(client, p)

    case PersistCheck(seq, true) if ps.contains(seq) =>
      ps -= seq

    case ReplicaFailed(seq) if seq == revId =>
      ps -= seq
      revId += 1
  }

  def readHandler: Receive = {
    case Get(key, id) => sender() ! GetResult(key, kv.get(key), id)
  }

  def onReplicated(key: String, id: Long, replicator: ActorRef) = {
    var (clientOption, waitingReplicators, replicate) = acks.get(id).get
    waitingReplicators -= replicator
    if (waitingReplicators.isEmpty) {
      acks -= id
      if (!ps.contains(id)) {
        replicate.valueOption match {
          case Some(value) => kv += (replicate.key -> value)
          case None => kv -= replicate.key
        }
        clientOption.foreach(_ ! OperationAck(id))
      }
    } else {
      acks += (id -> (clientOption, waitingReplicators, replicate))
    }
  }

  def sendPersist(client: ActorRef, p: Persist) = {
    if (!ps.contains(p.id)) {
      ps += (p.id -> (client, p))
    }
    persistence ! p
    context.system.scheduler.scheduleOnce(100.milliseconds, self, PersistCheck(p.id))
    context.system.scheduler.scheduleOnce(1.seconds, self, PersistCheck(p.id, stop = true))
  }

  def sendReplicate(client: ActorRef, replicate: Replicate): Unit = {
    sendReplicate(Some(client), replicate, replicators)
  }

  def sendReplicate(client: Option[ActorRef], replicate: Replicate, replicators: Set[ActorRef] = replicators): Unit = {
    acks += (replicate.id -> (client, replicators, replicate))
    if (replicators.isEmpty) {
      self ! Replicated(replicate.key, replicate.id)
    } else {
      replicators.foreach(_ ! replicate)
    }
  }
}

