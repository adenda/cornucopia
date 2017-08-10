package com.adendamedia.cornucopia

import akka.testkit.{ImplicitSender, TestActorRef, TestKit}
import akka.actor.ActorSystem
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{BeforeAndAfterAll, MustMatchers, WordSpecLike}
import com.adendamedia.cornucopia.actors.Gatekeeper
import com.adendamedia.cornucopia.actors.Gatekeeper.{Task, TaskAccepted}

class LibraryTest extends TestKit(ActorSystem("LibraryTest"))
  with WordSpecLike with BeforeAndAfterAll with MustMatchers with MockitoSugar with ImplicitSender {

  override def afterAll(): Unit = {
    system.terminate()
  }

  trait TestConfig {
    val cornucopiaMock: Cornucopia = mock[Cornucopia]
  }

  "Library" must {
    "accept task" in new TestConfig {

      // TestKit has an implicit ActorSystem
      class TestLibrary(implicit val system: ActorSystem) extends CornucopiaLibrary {
        val cornucopia: Cornucopia = cornucopiaMock
        val ref: TestActorRef[Gatekeeper] = TestActorRef[Gatekeeper](Gatekeeper.props)
      }

      val library = new TestLibrary

      library.ref ! Task("+master", "redis://192.168.0.100")

      expectMsg(TaskAccepted)
    }
  }

}
