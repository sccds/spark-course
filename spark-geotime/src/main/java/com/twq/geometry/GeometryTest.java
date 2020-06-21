package com.twq.geometry;

import com.esri.core.geometry.*;

public class GeometryTest {
    public static void main(String[] args) {
        // Geometry的子类：点，多点，边，多边形的构造
        Geometry point = createPoint1();
        Geometry point2 = createPoint2();
        Geometry multiPoint = createMultipoint1();
        Geometry polyline = createPolyline1();
        Geometry polygon = createPolygon1();

        //Geometry之间的关系
        System.out.println(GeometryEngine.contains(polygon, point, SpatialReference.create(4326)));
        System.out.println(GeometryEngine.distance(point, point2, SpatialReference.create(4326)));

        System.out.println(polygon.calculateArea2D());
    }

    // 点
    static Point createPoint1() {
        Point pt = new Point(-106.4453583, 39.11775);
        return pt;
    }

    // 点
    static Point createPoint2() {
        Point pt = new Point(-100.4453583, 39.11775);
        return pt;
    }

    // 多点
    static MultiPoint createMultipoint1() {
        MultiPoint mPoint = new MultiPoint();

        // add points
        mPoint.add(1, 1);
        mPoint.add(0.5, -0.5);
        mPoint.add(1, -1.5);
        mPoint.add(-1, -1);
        mPoint.add(-2, -0.5);
        mPoint.add(-1.5, 1.5);

        return mPoint;
    }

    // 边
    static Polyline createPolyline1() {
        Polyline line = new Polyline();

        // path1
        line.startPath(6.9, 9.1);
        line.lineTo(7, 6.8);

        // path2
        line.startPath(6.8, 8.8);
        line.lineTo(7, 9);
        line.lineTo(7.2, 8.9);
        line.lineTo(7.4, 9);

        // path3
        line.startPath(7.4, 8.9);
        line.lineTo(7.25, 8.6);
        line.lineTo(7.15, 8.8);

        return line;
    }

    // 多边形
    public static Polygon createPolygon1() {
        Polygon poly = new Polygon();

        // clockwise => outer ring
        poly.startPath(0, 0);
        poly.lineTo(-0.5, 0.5);
        poly.lineTo(0.5, 1);
        poly.lineTo(1, 0.5);
        poly.lineTo(0.5, 0);

        // hole
        poly.startPath(0.5, 0.2);
        poly.lineTo(0.6, 0.5);
        poly.lineTo(0.2, 0.9);
        poly.lineTo(-0.2, 0.5);
        poly.lineTo(0.1, 0.2);
        poly.lineTo(0.2, 0.3);

        return poly;
    }
}
