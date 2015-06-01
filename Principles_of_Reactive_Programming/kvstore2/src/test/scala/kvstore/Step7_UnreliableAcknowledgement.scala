package kvstore

import akka.actor.ActorSystem
import akka.testkit.{TestProbe, ImplicitSender, TestKit}
import org.scalactic.ConversionCheckedTripleEquals
import org.scalatest.{Matchers, BeforeAndAfterAll, FunSuiteLike}
import scala.concurrent.duration._
import Arbiter._
import Persistence._
import Replicator._

/**
 * @author Andrei Tupitcyn
 */
class Step7_UnreliableAcknowledgement extends TestKit(ActorSystem("Step7_UnreliableAcknowledgement"))
with FunSuiteLike
with BeforeAndAfterAll
with Matchers
with ConversionCheckedTripleEquals
with ImplicitSender
with Tools {

  override def afterAll(): Unit = {
    system.shutdown()
  }

  private def randomInt(implicit n: Int = 16) = (Math.random * n).toInt

  private def randomQuery(client: Session) {
    val rnd = Math.random
    if (rnd < 0.3) client.setAcked(s"k$randomInt", s"v$randomInt")
    else if (rnd < 0.6) client.removeAcked(s"k$randomInt")
    else client.getAndVerify(s"k$randomInt")
  }

  ignore("case1: Primary and secondaries must work in concert when persistence is unreliable") {
    val arbiter = TestProbe()
    val persistence = TestProbe()
    val persistenceSecondary = TestProbe()
    val primary = system.actorOf(Replica.props(arbiter.ref, probeProps(persistence)), "case1-primary")
    val secondary = system.actorOf(Replica.props(arbiter.ref, probeProps(persistenceSecondary)), "case1-secondary")
    val client = session(primary)
    val clientSecondary = session(secondary)

//    val secondary = TestProbe()

    arbiter.expectMsg(Join)
    arbiter.send(primary, JoinedPrimary)
    arbiter.send(secondary, JoinedSecondary)
    arbiter.send(primary, Replicas(Set(primary, secondary)))

    val setId = client.set("foo", "bar")
    val persistId = persistence.expectMsgPF() {
      case Persist("foo", Some("bar"), id) => id
    }

    persistenceSecondary.expectMsgType[Persist]
    clientSecondary.get("foo") should === (Some("bar"))

    persistence.reply(Persisted("foo", persistId))
    client.waitFailed(setId)
    client.get("foo") should === (None)
    clientSecondary.get("foo") should === (None)
  }

  test("case2") {
    val arbiter = TestProbe()
    val persistence = TestProbe()
    val persistenceSecondary = TestProbe()
    val primary = system.actorOf(Replica.props(arbiter.ref, probeProps(persistence)), "case1-primary")
    val secondary = system.actorOf(Replica.props(arbiter.ref, probeProps(persistenceSecondary)), "case1-secondary")
    val client = session(primary)
    val clientSecondary = session(secondary)

    arbiter.expectMsg(Join)
    arbiter.send(primary, JoinedPrimary)

    var setK1 = client.set("k1", "v1")
    persistence.expectMsgType[Persist]
    persistence.reply(Persisted("k1", 0L))
    client.waitAck(setK1)

    var setK2 = client.set("k2", "v2")
    persistence.expectMsgType[Persist]
    persistence.reply(Persisted("k2", 1L))
    client.waitAck(setK2)

    var removeK1 = client.remove("k1")
    persistence.expectMsgType[Persist]
    persistence.reply(Persisted("k1", 2L))
    client.waitAck(removeK1)

    arbiter.send(secondary, JoinedSecondary)
    arbiter.send(primary, Replicas(Set(primary, secondary)))

    val (key: String, persistId: Long) = persistenceSecondary.expectMsgPF() {
      case Persist(k, _, id) => (k, id)
    }
    persistenceSecondary.reply(Persisted(key, persistId))

    setK1 = client.set("k1", "v1")
    persistence.expectMsgType[Persist]
    persistence.reply(Persisted("k1", 3L))

    val (key2, persistId2) = persistenceSecondary.expectMsgPF() {
      case Persist(k, _, id) => (k, id)
    }
//    persistenceSecondary.reply(Persisted(key2, persistId2))

    client.waitFailed(setK1)

    client.get("k1") should === (None)
    clientSecondary.get("k1") should === (None)

  }

//  ignore("case2: Primary and secondaries must work in concert when persistence is unreliable") {
//    val arbiter = TestProbe()
//
//    val primary = system.actorOf(
//      Replica.props(arbiter.ref, Persistence.props(flaky = false)), "case1-primary")
//    arbiter.expectMsg(Join)
//    arbiter.send(primary, JoinedPrimary)
//
//    val secondary = system.actorOf(
//      Replica.props(arbiter.ref, Persistence.props(flaky = true)), "case1-secondary")
//    arbiter.expectMsg(Join)
//    arbiter.send(secondary, JoinedSecondary)
//
//    arbiter.send(primary, Replicas(Set(primary, secondary)))
//
//    val client = session(primary)
//    for (_ <- 0 until 1000) randomQuery(client)
//  }
//
//  ignore("case2: Random ops with 3 secondaries") {
//    val arbiter = TestProbe()
//
//    val primary = system.actorOf(
//      Replica.props(arbiter.ref, Persistence.props(flaky = false)), "case2-primary")
//    arbiter.expectMsg(Join)
//    arbiter.send(primary, JoinedPrimary)
//
//    val secondaries = (1 to 3).map(id =>
//      system.actorOf(Replica.props(arbiter.ref, Persistence.props(flaky = true)),
//        s"case2-secondary-$id"))
//
//    secondaries foreach { secondary =>
//      arbiter.expectMsg(Join)
//      arbiter.send(secondary, JoinedSecondary)
//    }
//
//    val client = session(primary)
//    for (i <- 0 until 1000) {
//      randomQuery(client)
//      if      (i == 100) arbiter.send(primary, Replicas(Set(secondaries(0))))
//      else if (i == 200) arbiter.send(primary, Replicas(Set(secondaries(0), secondaries(1))))
//      else if (i == 300) arbiter.send(primary, Replicas(Set(secondaries(0), secondaries(1), secondaries(2))))
//      else if (i == 400) arbiter.send(primary, Replicas(Set(secondaries(0), secondaries(1))))
//      else if (i == 500) arbiter.send(primary, Replicas(Set(secondaries(0))))
//      else if (i == 600) arbiter.send(primary, Replicas(Set()))
//    }
//  }
//
//  ignore("case3: Random ops with multiple clusters") {
//    val arbiter = TestProbe()
//
//    val primary = system.actorOf(
//      Replica.props(arbiter.ref, Persistence.props(flaky = false)), "case3-primary")
//    arbiter.expectMsg(Join)
//    arbiter.send(primary, JoinedPrimary)
//
//    val cluster = (1 to 10) map { i =>
//      (1 to 5).toSet map { (j: Int) =>
//        val secondary = system.actorOf(
//          Replica.props(arbiter.ref, Persistence.props(flaky = true)),
//          s"case3-secondary-$i-$j")
//        arbiter.expectMsg(Join)
//        arbiter.send(secondary, JoinedSecondary)
//        secondary
//      }
//    }
//
//    val client = session(primary)
//    for (i <- 0 until 1000) {
//      randomQuery(client)
//      if (randomInt(10) < 3)
//        arbiter.send(primary, Replicas(cluster(randomInt(10))))
//    }
//
//  }
}
