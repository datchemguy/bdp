package intro

import org.scalatest.FunSuite
import intro.PatternMatching2._

class PatternMatching2Test extends FunSuite{


  test(" twice") {
    assertResult(List("abc", "abc")) {
      twice(List("abc"))
    }
  }

  test("drunkWords") {
    assertResult(List("uoy","yeh")) {
      drunkWords(List("hey", "you"))
    }
  }

  test("myforall") {
    assert(myForAll(List("sa", "sssb", "sf"), (s: String) => s.startsWith("s")))
    assert(!myForAll(List("sa", "bsssb", "sf"), (s: String) => s.startsWith("s")))
    assert(myForAll(List(), (s: String) => s.startsWith("s")))
  }


  test("lastElem") {
    assertResult(Some("yes")) {
      lastElem(List("no", "yes", "no", "no", "yes"))
    }
  }

  test("append") {
    assertResult(List(1,3,5,2,4)) {
      append(List(1,3,5), List(2,4))
    }
  }
}
