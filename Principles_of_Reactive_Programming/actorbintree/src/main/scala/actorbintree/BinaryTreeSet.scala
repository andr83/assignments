/**
 * Copyright (C) 2009-2013 Typesafe Inc. <http://www.typesafe.com>
 */
package actorbintree

import akka.actor._
import scala.collection.immutable.Queue

object BinaryTreeSet {

  trait Operation {
    def requester: ActorRef
    def id: Int
    def elem: Int
  }

  trait OperationReply {
    def id: Int
  }

  /** Request with identifier `id` to insert an element `elem` into the tree.
    * The actor at reference `requester` should be notified when this operation
    * is completed.
    */
  case class Insert(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request with identifier `id` to check whether an element `elem` is present
    * in the tree. The actor at reference `requester` should be notified when
    * this operation is completed.
    */
  case class Contains(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request with identifier `id` to remove the element `elem` from the tree.
    * The actor at reference `requester` should be notified when this operation
    * is completed.
    */
  case class Remove(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request to perform garbage collection*/
  case object GC

  /** Holds the answer to the Contains request with identifier `id`.
    * `result` is true if and only if the element is present in the tree.
    */
  case class ContainsResult(id: Int, result: Boolean) extends OperationReply
  
  /** Message to signal successful completion of an insert or remove operation. */
  case class OperationFinished(id: Int) extends OperationReply

}


class BinaryTreeSet extends Actor {
  import BinaryTreeSet._
  import BinaryTreeNode._

  def createRoot: ActorRef = context.actorOf(BinaryTreeNode.props(0, initiallyRemoved = true))

  var root = createRoot

  // optional
  var pendingQueue = Queue.empty[Operation]

  // optional
  def receive = normal

  // optional
  /** Accepts `Operation` and `GC` messages. */
  val normal: Receive = {
    case op: Operation => root ! op
    case GC =>
      val newRoot = createRoot
      root ! CopyTo(newRoot)
      context.become(garbageCollecting(newRoot))
  }

  // optional
  /** Handles messages while garbage collection is performed.
    * `newRoot` is the root of the new binary tree where we want to copy
    * all non-removed elements into.
    */
  def garbageCollecting(newRoot: ActorRef): Receive = {
    case op: Operation => pendingQueue = pendingQueue :+ op
    case CopyFinished => {
      root ! PoisonPill
      root = newRoot

      pendingQueue foreach(root ! _)
      pendingQueue = Queue.empty[Operation]
      context.become(normal)
    }
  }

}

object BinaryTreeNode {
  trait Position

  case object Left extends Position
  case object Right extends Position

  case class CopyTo(treeNode: ActorRef)
  case object CopyFinished

  def props(elem: Int, initiallyRemoved: Boolean) = Props(classOf[BinaryTreeNode],  elem, initiallyRemoved)
}

class BinaryTreeNode(val elem: Int, initiallyRemoved: Boolean) extends Actor {
  import BinaryTreeNode._
  import BinaryTreeSet._

  var subtrees = Map[Position, ActorRef]()
  var removed = initiallyRemoved

  // optional
  def receive = normal

  // optional
  /** Handles `Operation` messages and `CopyTo` requests. */
  val normal: Receive = {
    case Insert(req, id, value) =>
      if (value == elem) {
        removed = false
        req ! OperationFinished(id)
      } else {
        val pos = if (value < elem) Left else Right
        subtrees.get(pos) match {
          case Some(tree) => tree ! Insert(req, id, value)
          case None =>
            val node = context.actorOf(BinaryTreeNode.props(value, initiallyRemoved = false))
            subtrees = subtrees.updated(pos, node)
            req ! OperationFinished(id)
        }
      }

    case Contains(req, id, value) =>
      if (value == elem) {
        req ! ContainsResult(id, !removed)
      } else {
        val pos = if (value < elem) Left else Right
        subtrees.get(pos) match {
          case Some(tree) => tree ! Contains(req, id, value)
          case None => req ! ContainsResult(id, result = false)
        }
      }

    case Remove(req, id, value) =>
      if (value == elem) {
        removed = true
        req ! OperationFinished(id)
      } else {
        val pos = if (value < elem) Left else Right
        subtrees.get(pos) match {
          case Some(tree) => tree ! Remove(req, id, value)
          case None => req ! OperationFinished(id)
        }
      }

    case CopyTo(tree) =>
      val expected = subtrees.values.toSet
      if (expected.isEmpty && removed) {
        context.parent ! CopyFinished
      } else {
        context.become(copying(expected, removed))
        expected foreach(_ ! CopyTo(tree))
        if (!removed) {
          tree ! Insert(self, -1, elem)
        }
      }

  }

  // optional
  /** `expected` is the set of ActorRefs whose replies we are waiting for,
    * `insertConfirmed` tracks whether the copy of this node to the new tree has been confirmed.
    */
  def copying(expected: Set[ActorRef], insertConfirmed: Boolean): Receive = {
    case OperationFinished(-1) =>
      if (expected.isEmpty) {
        context.become(normal)
        context.parent ! CopyFinished
      } else {
        context.become(copying(expected, insertConfirmed = true))
      }

    case CopyFinished =>
      if (expected.size <= 1 ) {
        if (insertConfirmed) {
          context.become(normal)
          context.parent ! CopyFinished
        } else if (expected.size == 1){
          context.become(copying(expected - expected.head, insertConfirmed))
        }
      } else {
        context.become(copying(expected - expected.head, insertConfirmed))
      }
  }


}
