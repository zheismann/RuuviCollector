package fi.tkgwf.ruuvi.service;

import fi.tkgwf.ruuvi.service.impl.FirebasePersistenceServiceImpl;

public final class PersistenceServiceFactory {
    private PersistenceServiceFactory() {
        throw new RuntimeException("Should not be instantiated");
    }

    /**
     * Instantiates the PersistenceService implementation based upon the ruuvi-collector.properties
     * @return new instance of a PersistenceService
     */
    public static PersistenceService createPersistenceService() {
        return new FirebasePersistenceServiceImpl();
    }
}
