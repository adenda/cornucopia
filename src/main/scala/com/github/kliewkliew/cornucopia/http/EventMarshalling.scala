package com.github.kliewkliew.cornucopia.http

import spray.json._

trait EventMarshalling extends DefaultJsonProtocol {
  import CornucopiaTaskMaster._

  implicit val taskFormat = jsonFormat2(RestTask)
}
