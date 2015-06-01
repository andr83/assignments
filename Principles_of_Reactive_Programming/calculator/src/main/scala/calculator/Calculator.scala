package calculator

sealed abstract class Expr
final case class Literal(v: Double) extends Expr
final case class Ref(name: String) extends Expr
final case class Plus(a: Expr, b: Expr) extends Expr
final case class Minus(a: Expr, b: Expr) extends Expr
final case class Times(a: Expr, b: Expr) extends Expr
final case class Divide(a: Expr, b: Expr) extends Expr

object Calculator {
  def computeValues(
      namedExpressions: Map[String, Signal[Expr]]): Map[String, Signal[Double]] = {
    namedExpressions.mapValues(expr => Signal {
      eval(expr(), namedExpressions)
    })
  }

  def eval(expr: Expr, references: Map[String, Signal[Expr]]): Double = {
    eval(expr, references, Set())
  }

  def eval(expr: Expr, references: Map[String, Signal[Expr]], usedExprs: Set[String]): Double = {
    expr match {
      case Literal(v: Double) => v
      case Ref(name: String) =>
        if (usedExprs.contains(name)) {
          Double.NaN
        } else {
          val refExpr = getReferenceExpr(name, references)
          eval(refExpr, references, usedExprs + name)
        }
      case Plus(a: Expr, b: Expr) => eval(a, references, usedExprs) + eval(b, references, usedExprs)
      case Minus(a: Expr, b: Expr) => eval(a, references, usedExprs) - eval(b, references, usedExprs)
      case Times(a: Expr, b: Expr) => eval(a, references, usedExprs) * eval(b, references, usedExprs)
      case Divide(a: Expr, b: Expr) => eval(a, references, usedExprs) / eval(b, references, usedExprs)
    }
  }

  /** Get the Expr for a referenced variables.
   *  If the variable is not known, returns a literal NaN.
   */
  private def getReferenceExpr(name: String,
      references: Map[String, Signal[Expr]]) = {
    references.get(name).fold[Expr] {
      Literal(Double.NaN)
    } { exprSignal =>
      exprSignal()
    }
  }
}
