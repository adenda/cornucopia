package com.adendamedia.cornucopia.http

import spray.json._

trait EventMarshalling extends DefaultJsonProtocol {
  import CornucopiaTaskMaster._

  implicit val taskFormat: RootJsonFormat[CornucopiaTaskMaster.RestTask] = jsonFormat1(RestTask)
  implicit val taskFormat2: RootJsonFormat[CornucopiaTaskMaster.RestTask2] = jsonFormat2(RestTask2)
}
