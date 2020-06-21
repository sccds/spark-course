package com.twq.jsontest

object TestJson {
  def main(args: Array[String]): Unit = {
    val jsonStr = scala.io.Source.fromURL(this.getClass.getResource("/test.json")).mkString

    import spray.json._

    val jsObject = jsonStr.parseJson.asJsObject

    import TestJsonProtocal._  // 对于自定义的类型，没有现成的隐式对象，需要自己实现一个

    //import spray.json.DefaultJsonProtocol._  // 转成 string 需要隐式转换,实现 jsReader里面read方法
    println(jsObject.fields.get("typeStr").get.convertTo[String])

    val features = jsObject.fields.get("features").get.convertTo[Array[JsValue]]
    println(features)

    val featuresNew = jsObject.fields.get("features").get.convertTo[Array[Feature]]
    println(featuresNew)

    val featureCollection = jsonStr.parseJson.convertTo[FeatureCollection]
    println(featureCollection)
  }
}
