package intro

import Practice._
import org.scalatest.FunSuite

class PracticeTest extends FunSuite {

    test("FirstN") {
        assertResult((1 to 10).toList) {
            firstN((1 to 20).toList, 10)
        }
    }

    test("MaxValue") {
        assertResult(16) {
            maxValue(List(10, 4, 14, -4, 15, 14, 16, 7))
        }
    }

    test("intlise") {
        assertResult(List(6,7,8,9,10,11)) {
            intList(6, 11)
        }
        assertResult(List()) {
            intList(67, 11)
        }
    }

    test("myfilter") {
        assertResult(List(0,4,8)) {
            myFilter(List.range(0,11), (i: Int) => i%2 == 0)
        }
    }
}
