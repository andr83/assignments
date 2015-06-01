package quickcheck

import common._

import org.scalacheck._
import Arbitrary._
import Gen._
import Prop._

abstract class QuickCheckHeap extends Properties("Heap") with IntHeap {

  property("min1") = forAll { a: Int =>
    val h = insert(a, empty)
    findMin(h) == a
  }

  property("min2") = forAll { (a: Int, b: Int) =>
    val heap = insert(a, empty)
    val result = insert(b, heap)

    (findMin(result) == a && a < b) || (findMin(result) == b && b <= a)
  }

  property("empty heap") = forAll { a: Int =>
    val heap = insert(a, empty)
    val result = deleteMin(heap)

    isEmpty(result)
  }

  property("check min after melding two heaps") = forAll { (a: H, b: H) =>
    val minA = findMin(a)
    val minB = findMin(b)
    val result = meld(a, b)

    (findMin(result) == minA && minA < minB) || (findMin(result) == minB && minB <= minA)
  }

  property("deleting minimum") = forAll { (a: Int) =>
    val heap = if (a != Int.MinValue)
      insert(a, insert(Int.MinValue, empty))
    else
      insert(a, insert(Int.MaxValue, empty))

    val previousMin = findMin(heap)
    val result = deleteMin(heap)

    findMin(result) != previousMin
  }

  property("melding and inserting new minimum") = forAll { (a: H, b: H) =>
    val globalMinimum = Int.MinValue
    val heap = meld(a, b)
    val result = insert(globalMinimum, heap)

    findMin(result) == globalMinimum
  }

  property("fiddling with heaps") = forAll { (a: H, b: H) =>
    val melded = meld(a, b)

    val minA = findMin(a)
    val aWithoutMin = deleteMin(a)

    val bWithMinFromA = insert(minA, b)
    val identicalWithMelded = meld(aWithoutMin, bWithMinFromA)

    def heapToList(heap: H, result: List[Int]): List[Int] = isEmpty(heap) match {
      case true => result
      case false =>
        val min = findMin(heap)
        val heapAfterMinimumDeletion = deleteMin(heap)

        heapToList(heapAfterMinimumDeletion, result ::: List(min))
    }

    heapToList(melded, List()) == heapToList(identicalWithMelded, List())
  }

  lazy val genHeap: Gen[H] = for {
    value <- arbitrary[Int]
    heap <- oneOf[H](empty, genHeap)
  } yield insert(value, heap)

  implicit lazy val arbHeap: Arbitrary[H] = Arbitrary(genHeap)
}