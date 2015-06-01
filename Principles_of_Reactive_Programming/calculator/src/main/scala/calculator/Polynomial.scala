package calculator

import scala.math._

object Polynomial {
  def computeDelta(a: Signal[Double], b: Signal[Double],
      c: Signal[Double]): Signal[Double] = Signal {
    b() * b() - 4 * a() * c()
  }

  def computeSolutions(a: Signal[Double], b: Signal[Double],
      c: Signal[Double], delta: Signal[Double]): Signal[Set[Double]] = Signal {
    if (delta() < 0 || a() == 0) {
      Set()
    } else {
      val deltaSqrt = sqrt(delta())
      val res1 = (-b() + deltaSqrt) / (2 * a())
      val res2 = (-b() - deltaSqrt) / (2 * a())
      if (res1 == Double.NaN || res2 == Double.NaN) {
        Set()
      } else {
        Set(res1, res2)
      }
    }
  }
}
