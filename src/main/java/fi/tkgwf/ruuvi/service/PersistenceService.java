package fi.tkgwf.ruuvi.service;

import fi.tkgwf.ruuvi.bean.EnhancedRuuviMeasurement;

public interface PersistenceService extends AutoCloseable {

    void store(final EnhancedRuuviMeasurement measurement);

}
