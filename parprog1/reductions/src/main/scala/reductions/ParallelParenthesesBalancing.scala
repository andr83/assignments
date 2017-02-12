package reductions

import scala.annotation._
import org.scalameter._
import common._

object ParallelParenthesesBalancingRunner {

  @volatile var seqResult = false

  @volatile var parResult = false

  val standardConfig = config(
    Key.exec.minWarmupRuns -> 40,
    Key.exec.maxWarmupRuns -> 80,
    Key.exec.benchRuns -> 120,
    Key.verbose -> true
  ) withWarmer(new Warmer.Default)

  def main(args: Array[String]): Unit = {
    val length = 100000000
    val chars = new Array[Char](length)
    val threshold = 10000
    val seqtime = standardConfig measure {
      seqResult = ParallelParenthesesBalancing.balance(chars)
    }
    println(s"sequential result = $seqResult")
    println(s"sequential balancing time: $seqtime ms")

    val fjtime = standardConfig measure {
      parResult = ParallelParenthesesBalancing.parBalance(chars, threshold)
    }
    println(s"parallel result = $parResult")
    println(s"parallel balancing time: $fjtime ms")
    println(s"speedup: ${seqtime / fjtime}")
  }
}

object ParallelParenthesesBalancing {

  /** Returns `true` iff the parentheses in the input `chars` are balanced.
   */
  def balance(chars: Array[Char]): Boolean = {
    var openingCount = 0
    var closingCount = 0
    var i = 0
    while (i < chars.length) {
      if (chars(i) == '(') {
        openingCount += 1
      } else if (chars(i) == ')') {
        if (openingCount > 0) {
          openingCount -= 1
        } else {
          closingCount += 1
        }
      }
      i += 1
    }
    (openingCount, closingCount) == (0, 0)
  }

  /** Returns `true` iff the parentheses in the input `chars` are balanced.
   */
  def parBalance(chars: Array[Char], threshold: Int): Boolean = {

    def traverse(from: Int, until: Int, unbalancedOpening: Int, unbalancedClosing: Int) : (Int, Int) = {
      var opening = unbalancedOpening
      var closing = unbalancedClosing
      var i = from
      while (i < until) {
        if (chars(i) == '(') {
          opening += 1
        } else if (chars(i) == ')') {
          if (opening > 0) {
            opening -= 1
          } else {
            closing += 1
          }
        }
        i += 1
      }
      (opening, closing)
    }

    def reduce(from: Int, until: Int): (Int, Int) = {
      if (until - from <= threshold) {
        traverse(from, until, 0, 0)
      } else {
        val mid = from + (until - from) / 2
        val (left, right) = parallel(reduce(from, mid), reduce(mid, until))
        val openingCount = left._1 - right._2
        if (openingCount >= 0) {
          (openingCount + right._1, Math.max(left._2 - openingCount, 0))
        } else {
          (right._1, left._2 + right._2 - left._1)
        }
      }
    }

    reduce(0, chars.length) == (0, 0)
  }

  // For those who want more:
  // Prove that your reduction operator is associative!

}
