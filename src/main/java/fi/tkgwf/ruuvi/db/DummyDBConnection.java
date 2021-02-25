package fi.tkgwf.ruuvi.db;

import fi.tkgwf.ruuvi.bean.EnhancedRuuviMeasurement;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class DummyDBConnection implements DBConnection {

    private static final Logger LOG = LogManager.getLogger(DummyDBConnection.class);

    @Override
    public void save(EnhancedRuuviMeasurement measurement) {
        LOG.debug(measurement);
    }

    @Override
    public void close() {
    }
}
