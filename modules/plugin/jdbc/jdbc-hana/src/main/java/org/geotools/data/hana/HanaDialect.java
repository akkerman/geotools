package org.geotools.data.hana;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import org.geotools.jdbc.BasicSQLDialect;
import org.geotools.jdbc.JDBCDataStore;
import org.opengis.feature.type.GeometryDescriptor;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Created with IntelliJ IDEA.
 * User: akkerman
 * Date: 12/5/13
 * Time: 12:51 PM
 * To change this template use File | Settings | File Templates.
 */
public class HanaDialect extends BasicSQLDialect {
    protected HanaDialect(JDBCDataStore dataStore) {
        super(dataStore);
    }

    @Override
    public void encodeGeometryValue(Geometry value, int srid, StringBuffer sql) throws IOException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void encodeGeometryEnvelope(String tableName, String geometryColumn, StringBuffer sql) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Envelope decodeGeometryEnvelope(ResultSet rs, int column, Connection cx) throws SQLException, IOException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Geometry decodeGeometryValue(GeometryDescriptor descriptor, ResultSet rs, String column, GeometryFactory factory, Connection cx) throws IOException, SQLException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }
}
