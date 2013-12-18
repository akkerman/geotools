package org.geotools.data.hana;

import com.vividsolutions.jts.geom.*;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;
import org.geotools.data.jdbc.FilterToSQL;
import org.geotools.factory.Hints;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.geotools.jdbc.BasicSQLDialect;
import org.geotools.jdbc.ColumnMetadata;
import org.geotools.jdbc.JDBCDataStore;
import org.geotools.referencing.CRS;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;
import org.opengis.feature.type.GeometryDescriptor;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

import java.io.IOException;
import java.sql.*;
import java.util.*;
import java.util.logging.Level;

/**
 * @source $URL$
 */
public class HanaDialect extends BasicSQLDialect {

    //geometry type to class map
    final static Map<String, Class> TYPE_TO_CLASS_MAP = new HashMap<String, Class>() {
        {
            put("GEOMETRY", Geometry.class);
            put("GEOGRAPHY", Geometry.class);
            put("POINT", Point.class);
            put("LINESTRING", LineString.class);
            put("POLYGON", Polygon.class);
            put("MULTIPOINT", MultiPoint.class);
            put("MULTILINESTRING", MultiLineString.class);
            put("MULTIPOLYGON", MultiPolygon.class);
            put("GEOMETRYCOLLECTION", GeometryCollection.class);
            put("BYTEA", byte[].class);
        }
    };

    //geometry class to type map
    final static Map<Class, String> CLASS_TO_TYPE_MAP = new HashMap<Class, String>() {
        {
            put(Geometry.class, "GEOMETRY");
            put(Point.class, "POINT");
            put(LineString.class, "LINESTRING");
            put(Polygon.class, "POLYGON");
            put(MultiPoint.class, "MULTIPOINT");
            put(MultiLineString.class, "MULTILINESTRING");
            put(MultiPolygon.class, "MULTIPOLYGON");
            put(GeometryCollection.class, "GEOMETRYCOLLECTION");
            put(byte[].class, "BYTEA");
        }
    };

    public HanaDialect(JDBCDataStore dataStore) {
        super(dataStore);
    }

    boolean looseBBOXEnabled = false;

    boolean estimatedExtentsEnabled = false;

    boolean functionEncodingEnabled = false;

    @Override
    public void initializeConnection(Connection cx) throws SQLException {
        super.initializeConnection(cx);
    }

    @Override
    public boolean includeTable(String schemaName, String tableName,
                                Connection cx) throws SQLException {
        if (tableName.equals("ST_GEOMETRY_COLUMNS")) {
            return false;
        } else if (tableName.startsWith("ST_SPATIAL_REFERENCE_SYSTEMS")) {
            return false;
        }
        return true;
    }

    ThreadLocal<WKBAttributeIO> wkbReader = new ThreadLocal<WKBAttributeIO>();

    @Override
    public Geometry decodeGeometryValue(GeometryDescriptor descriptor,
                                        ResultSet rs, String column, GeometryFactory factory, Connection cx)
            throws IOException, SQLException {
        WKBAttributeIO reader = getWKBReader(factory);
        return (Geometry) reader.read(rs, column);
    }

    public Geometry decodeGeometryValue(GeometryDescriptor descriptor,
                                        ResultSet rs, int column, GeometryFactory factory, Connection cx)
            throws IOException, SQLException {
        WKBAttributeIO reader = getWKBReader(factory);
        return (Geometry) reader.read(rs, column);
    }

    private WKBAttributeIO getWKBReader(GeometryFactory factory) {
        WKBAttributeIO reader = wkbReader.get();
        if (reader == null) {
            reader = new WKBAttributeIO(factory);
            wkbReader.set(reader);
        } else {
            reader.setGeometryFactory(factory);
        }
        return reader;
    }

    @Override
    public void encodeGeometryColumn(GeometryDescriptor gatt, String prefix, int srid,
                                     StringBuffer sql) {
        encodeGeometryColumn(gatt, prefix, srid, null, sql);
    }

    @Override
    public void encodeGeometryColumn(GeometryDescriptor gatt, String prefix, int srid, Hints hints,
                                     StringBuffer sql) {

        boolean geography = "geography".equals(gatt.getUserData().get(
                JDBCDataStore.JDBC_NATIVE_TYPENAME));

        if (geography) {
            sql.append("encode(ST_AsBinary(");
            encodeColumnName(prefix, gatt.getLocalName(), sql);
            sql.append("),'base64')");
        } else {
            boolean force2D = hints != null && hints.containsKey(Hints.FEATURE_2D) &&
                    Boolean.TRUE.equals(hints.get(Hints.FEATURE_2D));

            if (force2D) {
                sql.append("encode(ST_AsBinary(ST_Force_2D(");
                encodeColumnName(prefix, gatt.getLocalName(), sql);
                sql.append(")),'base64')");
            } else {
                sql.append("encode(ST_AsEWKB(");
                encodeColumnName(prefix, gatt.getLocalName(), sql);
                sql.append("),'base64')");
            }
        }
    }

    @Override
    public void encodeGeometryEnvelope(String tableName, String geometryColumn,
                                       StringBuffer sql) {
        sql.append("ST_AsText(ST_Envelope(ST_GeomFromText(\""+ geometryColumn+ "\")))");
    }

    @Override
    public List<ReferencedEnvelope> getOptimizedBounds(String schema, SimpleFeatureType featureType,
                                                       Connection cx) throws SQLException, IOException {
//        if (!estimatedExtentsEnabled)
//            return null;
//
//        String tableName = featureType.getTypeName();
//
//        Statement st = null;
//        ResultSet rs = null;
//
//        List<ReferencedEnvelope> result = new ArrayList<ReferencedEnvelope>();
//        Savepoint savePoint = null;
//        try {
//            st = cx.createStatement();
//            if (!cx.getAutoCommit()) {
//                savePoint = cx.setSavepoint();
//            }
//
//            for (AttributeDescriptor att : featureType.getAttributeDescriptors()) {
//                if (att instanceof GeometryDescriptor) {
//                    // use estimated extent (optimizer statistics)
//                    StringBuffer sql = new StringBuffer();
//                    sql.append("select ST_AsText(ST_Envelope())");
//                    if (schema != null) {
//                        sql.append(schema);
//                        sql.append("', '");
//                    }
//                    sql.append(tableName);
//                    sql.append("', '");
//                    sql.append(att.getName().getLocalPart());
//                    sql.append("'))))");
//                    rs = st.executeQuery(sql.toString());
//
//                    if (rs.next()) {
//                        // decode the geometry
//                        Envelope env = decodeGeometryEnvelope(rs, 1, cx);
//
//                        // reproject and merge
//                        if (!env.isNull()) {
//                            CoordinateReferenceSystem crs = ((GeometryDescriptor) att)
//                                    .getCoordinateReferenceSystem();
//                            result.add(new ReferencedEnvelope(env, crs));
//                        }
//                    }
//                    rs.close();
//                }
//            }
//        } catch (SQLException e) {
//            if (savePoint != null) {
//                cx.rollback(savePoint);
//            }
//            LOGGER.log(Level.WARNING, "Failed to use ST_Estimated_Extent, falling back on envelope aggregation", e);
//            return null;
//        } finally {
//            if (savePoint != null) {
//                cx.releaseSavepoint(savePoint);
//            }
//            dataStore.closeSafe(rs);
//            dataStore.closeSafe(st);
//        }
        return null;
    }

    @Override
    public Envelope decodeGeometryEnvelope(ResultSet rs, int column,
                                           Connection cx) throws SQLException, IOException {
        try {
            String envelope = rs.getString(column);
            if (envelope != null)
                return new WKTReader().read(envelope).getEnvelopeInternal();
            else
                // empty one
                return new Envelope();
        } catch (ParseException e) {
            throw (IOException) new IOException(
                    "Error occurred parsing the bounds WKT").initCause(e);
        }
    }

    @Override
    public Class<?> getMapping(ResultSet columnMetaData, Connection cx)
            throws SQLException {

        String typeName = columnMetaData.getString("TYPE_NAME");
        if ("uuid".equalsIgnoreCase(typeName)) {
            return UUID.class;
        }

        String gType = null;
        if ("geometry".equalsIgnoreCase(typeName)) {
            gType = lookupGeometryType(columnMetaData, cx, "geometry_columns", "f_geometry_column");
        } else if ("geography".equalsIgnoreCase(typeName)) {
            gType = lookupGeometryType(columnMetaData, cx, "geography_columns", "f_geography_column");
        } else {
            return null;
        }

        // decode the type into
        if (gType == null) {
            // it's either a generic geography or geometry not registered in the medatata tables
            return Geometry.class;
        } else {
            Class geometryClass = (Class) TYPE_TO_CLASS_MAP.get(gType.toUpperCase());
            if (geometryClass == null) {
                geometryClass = Geometry.class;
            }

            return geometryClass;
        }
    }

    String lookupGeometryType(ResultSet columnMetaData, Connection cx, String gTableName,
                              String gColumnName) throws SQLException {

        // grab the information we need to proceed
        String tableName = columnMetaData.getString("TABLE_NAME");
        String columnName = columnMetaData.getString("COLUMN_NAME");
        String schemaName = columnMetaData.getString("TABLE_SCHEM");

        // first attempt, try with the geometry metadata
        Connection conn = null;
        Statement statement = null;
        ResultSet result = null;

        try {
            String sqlStatement = "SELECT TYPE FROM " + gTableName + " WHERE " //
                    + "F_TABLE_SCHEMA = '" + schemaName + "' " //
                    + "AND F_TABLE_NAME = '" + tableName + "' " //
                    + "AND " + gColumnName + " = '" + columnName + "'";

            LOGGER.log(Level.FINE, "Geometry type check; {0} ", sqlStatement);
            statement = cx.createStatement();
            result = statement.executeQuery(sqlStatement);

            if (result.next()) {
                return result.getString(1);
            }
        } finally {
            dataStore.closeSafe(result);
            dataStore.closeSafe(statement);
        }

        return null;
    }

    @Override
    public void handleUserDefinedType(ResultSet columnMetaData, ColumnMetadata metadata,
                                      Connection cx) throws SQLException {

        String tableName = columnMetaData.getString("TABLE_NAME");
        String columnName = columnMetaData.getString("COLUMN_NAME");
        String schemaName = columnMetaData.getString("SCHEMA_NAME");

        String sql = "SELECT udt_name FROM information_schema.columns " +
                " WHERE table_schema = '" + schemaName + "' " +
                "   AND table_name = '" + tableName + "' " +
                "   AND column_name = '" + columnName + "' ";
        LOGGER.fine(sql);

        Statement st = cx.createStatement();
        try {
            ResultSet rs = st.executeQuery(sql);
            try {
                if (rs.next()) {
                    metadata.setTypeName(rs.getString(1));
                }
            } finally {
                dataStore.closeSafe(rs);
            }
        } finally {
            dataStore.closeSafe(st);
        }
    }

    @Override
    public Integer getGeometrySRID(String schemaName, String tableName,
                                   String columnName, Connection cx) throws SQLException {

//        // first attempt, try with the geometry metadata
//        Statement statement = null;
//        ResultSet result = null;
//        Integer srid = null;
//        try {
//            schemaName = "SYS";
//            // try geometry_columns
//            try {
//                String sqlStatement = "SELECT SRS_ID FROM ST_GEOMETRY_COLUMNS WHERE " //
//                        + "SCHEMA_NAME = '" + schemaName + "' " //
//                        + "AND TABLE_NAME = '" + tableName + "' " //
//                        + "AND COLUMN_NAME = '" + columnName + "'";
//
//                LOGGER.log(Level.FINE, "Geometry srid check; {0} ", sqlStatement);
//                statement = cx.createStatement();
//                result = statement.executeQuery(sqlStatement);
//
//                if (result.next()) {
//                    srid = result.getInt(1);
//                }
//            } catch (SQLException e) {
//                LOGGER.log(Level.WARNING, "Failed to retrieve information about "
//                        + schemaName + "." + tableName + "." + columnName
//                        + " from the geometry_columns table, checking the first geometry instead", e);
//            } finally {
//                dataStore.closeSafe(result);
//            }
//
//            // fall back on inspection of the first geometry, assuming uniform srid (fair assumption
//            // an unpredictable srid makes the table un-queriable)
//            //JD: In postgis 2.0 forward there is no way to leave a geometry srid unset since
//            // geometry_columns is a view populated from system tables, so we check for 0 and take
//            // that to mean unset
//
//            if (srid == null) {
//                return 4326;
//            }
//        } finally {
//            dataStore.closeSafe(result);
//            dataStore.closeSafe(statement);
//        }
//
//        return srid;
        return 4326;
    }

    @Override
    public String getSequenceForColumn(String schemaName, String tableName,
                                       String columnName, Connection cx) throws SQLException {
//        Statement st = cx.createStatement();
//        try {
//            // pg_get_serial_sequence oddity: table name needs to be
//            // escaped with "", whilst column name, doesn't...
//            String sql = "SELECT pg_get_serial_sequence('\"";
//            if (schemaName != null && !"".equals(schemaName))
//                sql += schemaName + "\".\"";
//            sql += tableName + "\"', '" + columnName + "')";
//
//            dataStore.getLogger().fine(sql);
//            ResultSet rs = st.executeQuery(sql);
//            try {
//                if (rs.next()) {
//                    return rs.getString(1);
//                }
//            } finally {
//                dataStore.closeSafe(rs);
//            }
//        } finally {
//            dataStore.closeSafe(st);
//        }

        return "SEQ1";
    }

    @Override
    public Object getNextSequenceValue(String schemaName, String sequenceName,
                                       Connection cx) throws SQLException {
        Statement st = cx.createStatement();
        try {
            String sql = "SELECT sequenceName.NEXTVAL FROM DUMMY";

            dataStore.getLogger().fine(sql);
            ResultSet rs = st.executeQuery(sql);
            try {
                if (rs.next()) {
                    return rs.getLong(1);
                }
            } finally {
                dataStore.closeSafe(rs);
            }
        } finally {
            dataStore.closeSafe(st);
        }

        return null;
    }

    @Override
    public boolean lookupGeneratedValuesPostInsert() {
        return true;
    }

    @Override
    public Object getLastAutoGeneratedValue(String schemaName, String tableName, String columnName,
                                            Connection cx) throws SQLException {
        return 1;
    }

    @Override
    public void registerClassToSqlMappings(Map<Class<?>, Integer> mappings) {
        super.registerClassToSqlMappings(mappings);

        // jdbc metadata for geom columns reports DATA_TYPE=1111=Types.OTHER
        mappings.put(Geometry.class, Types.OTHER);
        mappings.put(UUID.class, Types.OTHER);
    }

    @Override
    public void registerSqlTypeNameToClassMappings(
            Map<String, Class<?>> mappings) {
        super.registerSqlTypeNameToClassMappings(mappings);

        mappings.put("ST_GEOMETRY", Geometry.class);
        mappings.put("TEXT", String.class);
        mappings.put("BIGINT", Long.class);
        mappings.put("INTEGER", Integer.class);
        mappings.put("VARCHAR", Boolean.class);
        mappings.put("VARCHAR", String.class);
        mappings.put("DOUBLE", Double.class);
        mappings.put("REAL", Float.class);
        mappings.put("SMALLINT", Short.class);
        mappings.put("TIME", Time.class);
        mappings.put("TIMESTAMP", Timestamp.class);
    }

    @Override
    public void registerSqlTypeToSqlTypeNameOverrides(
            Map<Integer, String> overrides) {
        overrides.put(Types.VARCHAR, "VARCHAR");
        overrides.put(Types.BOOLEAN, "BOOL");
    }

    @Override
    public String getGeometryTypeName(Integer type) {
        return "ST_GEOMETRY";
    }

    @Override
    public void encodePrimaryKey(String column, StringBuffer sql) {
        encodeColumnName(column, sql);
        sql.append(" SERIAL PRIMARY KEY");
    }

    /**
     * Creates GEOMETRY_COLUMN registrations and spatial indexes for all
     * geometry columns
     */
    @Override
    public void postCreateTable(String schemaName,
                                SimpleFeatureType featureType, Connection cx) throws SQLException {
        schemaName = schemaName != null ? schemaName : "public";
        String tableName = featureType.getName().getLocalPart();

        Statement st = null;
        try {
            st = cx.createStatement();

            // register all geometry columns in the database
            for (AttributeDescriptor att : featureType
                    .getAttributeDescriptors()) {
                if (att instanceof GeometryDescriptor) {
                    GeometryDescriptor gd = (GeometryDescriptor) att;

                    // lookup or reverse engineer the srid
                    int srid = -1;
                    if (gd.getUserData().get(JDBCDataStore.JDBC_NATIVE_SRID) != null) {
                        srid = (Integer) gd.getUserData().get(
                                JDBCDataStore.JDBC_NATIVE_SRID);
                    } else if (gd.getCoordinateReferenceSystem() != null) {
                        try {
                            Integer result = CRS.lookupEpsgCode(gd
                                    .getCoordinateReferenceSystem(), true);
                            if (result != null)
                                srid = result;
                        } catch (Exception e) {
                            LOGGER.log(Level.FINE, "Error looking up the "
                                    + "epsg code for metadata "
                                    + "insertion, assuming -1", e);
                        }
                    }

                    // grab the geometry type
                    String geomType = CLASS_TO_TYPE_MAP.get(gd.getType()
                            .getBinding());
                    if (geomType == null)
                        geomType = "ST_GEOMETRY";

                    String sql = null;
                    // register the geometry type, first remove and eventual
                    // leftover, then write out the real one
                    sql =
                            "DELETE FROM ST_GEOMETRY_COLUMNS"
                                    + " WHERE SCHEMA_NAME = '" + schemaName + "'" //
                                    + " AND TABLE_NAME = '" + tableName + "'" //
                                    + " AND COLUMN_NAME = '" + gd.getLocalName() + "'";

                    LOGGER.fine(sql);
                    st.execute(sql);

                    // add the spatial index
                    sql =
                            "CREATE INDEX \"spatial_" + tableName //
                                    + "_" + gd.getLocalName().toLowerCase() + "\""//
                                    + " ON " //
                                    + "\"" + schemaName + "\"" //
                                    + "." //
                                    + "\"" + tableName + "\"" //
                                    + " USING GIST (" //
                                    + "\"" + gd.getLocalName() + "\"" //
                                    + ")";
                    LOGGER.fine(sql);
                    st.execute(sql);
                }
            }
            if (!cx.getAutoCommit()) {
                cx.commit();
            }
        } finally {
            dataStore.closeSafe(st);
        }
    }

    @Override
    public void postDropTable(String schemaName, SimpleFeatureType featureType, Connection cx)
            throws SQLException {
        Statement st = cx.createStatement();
        String tableName = featureType.getTypeName();

        try {
            //remove all the geometry_column entries
            String sql =
                    "DELETE FROM GEOMETRY_COLUMNS"
                            + " WHERE SCHEMA_NAME = '" + schemaName + "'"
                            + " AND TABLE_NAME = '" + tableName + "'";
            LOGGER.fine(sql);
            st.execute(sql);
        } finally {
            dataStore.closeSafe(st);
        }
    }

    @Override
    public void encodeGeometryValue(Geometry value, int srid, StringBuffer sql)
            throws IOException {
        if (value == null || value.isEmpty()) {
            sql.append("NULL");
        } else {
            if (value instanceof LinearRing) {
                //postgis does not handle linear rings, convert to just a line string
                value = value.getFactory().createLineString(((LinearRing) value).getCoordinateSequence());
            }

            sql.append("ST_GeomFromText('" + value.toText() + "', " + srid + ")");
        }
    }

    @Override
    public FilterToSQL createFilterToSQL() {
        HanaFilterToSQL sql = new HanaFilterToSQL(this);
        sql.setLooseBBOXEnabled(looseBBOXEnabled);
        sql.setFunctionEncodingEnabled(functionEncodingEnabled);
        return sql;
    }

    @Override
    public void onSelect(Statement select, Connection cx, SimpleFeatureType featureType) throws SQLException {
        super.onSelect(select, cx, featureType);
    }

    @Override
    public boolean isLimitOffsetSupported() {
        return true;
    }

    @Override
    public void applyLimitOffset(StringBuffer sql, int limit, int offset) {
        if (limit >= 0 && limit < Integer.MAX_VALUE) {
            sql.append(" LIMIT " + limit);
            if (offset > 0) {
                sql.append(" OFFSET " + offset);
            }
        } else if (offset > 0) {
            sql.append(" OFFSET " + offset);
        }
    }

    @Override
    public void encodeValue(Object value, Class type, StringBuffer sql) {
        if (byte[].class.equals(type)) {
            byte[] input = (byte[]) value;
            encodeByteArrayAsHex(input, sql);
        } else {
            super.encodeValue(value, type, sql);
        }
    }

    void encodeByteArrayAsHex(byte[] input, StringBuffer sql) {
        StringBuffer sb = new StringBuffer("\\x");
        for (int i = 0; i < input.length; i++) {
            sb.append(String.format("%02x", input[i]));
        }
        super.encodeValue(sb.toString(), String.class, sql);
    }

    @Override
    public int getDefaultVarcharSize() {
        return -1;
    }

}
