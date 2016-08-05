package interface

import org.scalatest.FreeSpec

class ProgramTest extends FreeSpec {

  "When running the full distributed stream processing program" - {
    "should succeed and create views" in {
      val timeout = 10L
      val readers = Seq(CommittableReader(new ConsumerWrapper[]()))

      Program.applyViewLogic()
    }
  }
}
