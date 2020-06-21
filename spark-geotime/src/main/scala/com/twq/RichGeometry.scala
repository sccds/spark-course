package com.twq

import com.esri.core.geometry.{Geometry, GeometryEngine, SpatialReference}

/**
 * Geometry的包装类，包含指定的坐标系 SpatialReference, 提供横方便的计算空间关系的方法
 */
class RichGeometry(val geometry: Geometry,
                   val spatialReference: SpatialReference = SpatialReference.create(4326)) extends Serializable {

  def area2D(): Double = geometry.calculateArea2D()

  def distance(other: Geometry): Double = GeometryEngine.distance(geometry, other, spatialReference)

  def contains(other: Geometry): Boolean = GeometryEngine.contains(geometry, other, spatialReference)

  def within(other: Geometry): Boolean = GeometryEngine.within(geometry, other, spatialReference)

  def overlaps(other: Geometry): Boolean = GeometryEngine.overlaps(geometry, other, spatialReference)

  def touches(other: Geometry): Boolean = GeometryEngine.touches(geometry, other, spatialReference)

  def crosses(other: Geometry): Boolean = GeometryEngine.crosses(geometry, other, spatialReference)


}
