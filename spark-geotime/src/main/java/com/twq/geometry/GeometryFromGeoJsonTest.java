package com.twq.geometry;

import com.esri.core.geometry.*;
import org.json.JSONException;

public class GeometryFromGeoJsonTest {
    public static void main(String[] args) {

    }

    //点
    static Point createPointFromGeoJson() throws JSONException {

        String geoJsonString = "{\"type\":\"Point\",\"coordinates\":[-106.4453583,39.11775],\"crs\":\"EPSG:4326\"}";

        MapGeometry mapGeom = GeometryEngine.geometryFromGeoJson(geoJsonString, GeoJsonImportFlags.geoJsonImportDefaults, Geometry.Type.Point);

        return (Point)mapGeom.getGeometry();
    }

    //多点
    static MultiPoint createMultipointFromGeoJson() throws JSONException {

        String geoJsonString = "{\"type\":\"MultiPoint\","
                + "\"coordinates\":[[1,1],[0.5,-0.5],[1,-1.5],[-1,-1],[-2,-0.5],[-1.5,1.5]],"
                + "\"crs\":\"EPSG:4326\"}";

        MapGeometry mapGeom = GeometryEngine.geometryFromGeoJson(geoJsonString, GeoJsonImportFlags.geoJsonImportDefaults, Geometry.Type.MultiPoint);

        return (MultiPoint)mapGeom.getGeometry();
    }

    //边
    static Polyline createPolylineFromGeoJson() throws JSONException {

        String geoJsonString = "{\"type\":\"MultiLineString\","
                + "\"coordinates\":[[[6.8,8.8],[7,9],[7.2,8.9],[7.4,9]],"
                + "[[7.4,8.9],[7.25,8.6],[7.15,8.8]],[[6.9, 9.1],[7, 8.8]]],"
                + "\"crs\":\"EPSG:4326\"}";

        MapGeometry mapGeom =  GeometryEngine.geometryFromGeoJson(geoJsonString, GeoJsonImportFlags.geoJsonImportDefaults, Geometry.Type.Polyline);

        return (Polyline)mapGeom.getGeometry();
    }

    //多边形
    static Polygon createPolygonFromGeoJson() throws JSONException
    {
        String geoJsonString = "{\"type\":\"Polygon\","
                + "\"coordinates\":[[[[0.0,0.0],[-0.5,0.5],[0.0,1.0],[0.5,1.0],[1.0,0.5],[0.5,0.0],[0.0,0.0]],"
                + "[[0.5,0.2],[0.6,0.5],[0.2,0.9],[-0.2,0.5],[0.1,0.2],[0.2,0.3],[0.5,0.2]]],"
                + "[[[0.1,0.7],[0.3,0.7],[0.3,0.4],[0.1,0.4],[0.1,0.7]]]],"
                + "\"crs\":\"EPSG:4326\"}";

        MapGeometry mapGeom = GeometryEngine.geometryFromGeoJson(geoJsonString, GeoJsonImportFlags.geoJsonImportDefaults, Geometry.Type.Polygon);

        return (Polygon)mapGeom.getGeometry();
    }
}
