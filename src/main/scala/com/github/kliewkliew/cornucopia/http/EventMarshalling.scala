package com.github.kliewkliew.cornucopia.http

import spray.json._

trait EventMarshalling extends DefaultJsonProtocol {
  import CornucopiaTaskMaster._

  implicit val taskFormat: RootJsonFormat[CornucopiaTaskMaster.RestTask] = jsonFormat2(RestTask)
}
