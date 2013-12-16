package org.geotools.data.hana;

import org.geotools.jdbc.JDBCDataStore;
import org.geotools.jdbc.JDBCDataStoreFactory;
import org.geotools.jdbc.SQLDialect;

/**
 * Created with IntelliJ IDEA.
 * User: akkerman
 * Date: 12/5/13
 * Time: 2:31 PM
 * To change this template use File | Settings | File Templates.
 */
public class HanaDataStoreFactory extends JDBCDataStoreFactory {
    @Override
    protected String getDatabaseID() {
        return "sap";
    }

    @Override
    protected String getDriverClassName() {
        return "com.sap.db.jdbc.Driver";
    }

    @Override
    protected SQLDialect createSQLDialect(JDBCDataStore dataStore) {
        return new HanaDialect(dataStore);
    }

    @Override
    protected String getValidationQuery() {
        return "SELECT SRS_OID from SYS.ST_SPATIAL_REFERENCE_SYSTEMS_ WHERE SRS_OID = 151798";
    }

    @Override
    public String getDescription() {
        return "SAP HANA database voor CGis";
    }
}
