package com.twq.jsontest

import spray.json._

import spray.json.{DefaultJsonProtocol, JsArray, JsNumber, JsObject, JsString, JsValue, RootJsonFormat}

case class Feature(typeStr: String, id: Int, name: Option[String])

// feature的集合，这样的话可以访问完整json,也要写一个Format,把整个转成FeatureCollection
case class FeatureCollection(typeStr: String, features: Array[Feature]) extends IndexedSeq[Feature] {
  def apply(index: Int): Feature = features(index)
  def length: Int = features.length
}

object TestJsonProtocal extends DefaultJsonProtocol {

  implicit object FeatureJsonFormat extends RootJsonFormat[Feature] {  // 需要复写 write and read 方法

    def write(f: Feature): JsObject = {
      // 先定义必须有的
      val buf = scala.collection.mutable.ArrayBuffer(
        "typeStr" -> JsString(f.typeStr),
        "id" -> JsNumber(f.id)
      )
      // 再把可有可无的加进去
      f.name.foreach(n => {
        buf += "name" -> JsString(n)
      })
      JsObject(buf.toMap)
    }

    def read(value: JsValue): Feature = {
      val jso = value.asJsObject
      val typeStr = jso.fields.get("typeStr").get.convertTo[String]
      val id = jso.fields.get("id").get.convertTo[Int]
      val name = jso.fields.get("name").map(_.convertTo[String])
      Feature(typeStr, id, name)
    }
  }

  implicit object FeatureCollectionJsonFormat extends RootJsonFormat[FeatureCollection] {
    def write(fc: FeatureCollection): JsObject = {
      JsObject(
        "typeStr" -> JsString(fc.typeStr),
        "features" -> JsArray(fc.features.map(_.toJson): _*)
      )
    }

    def read(value: JsValue): FeatureCollection = {
      val jso = value.asJsObject
      val typeStr = jso.fields.get("typeStr").get.convertTo[String]
      val features = jso.fields.get("features").get.convertTo[Array[Feature]]
      FeatureCollection(typeStr, features)
    }
  }
}
