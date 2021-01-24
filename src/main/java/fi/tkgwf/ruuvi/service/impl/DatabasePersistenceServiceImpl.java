package fi.tkgwf.ruuvi.service.impl;

import fi.tkgwf.ruuvi.bean.EnhancedRuuviMeasurement;
import fi.tkgwf.ruuvi.config.Config;
import fi.tkgwf.ruuvi.db.DBConnection;
import fi.tkgwf.ruuvi.service.PersistenceService;
import fi.tkgwf.ruuvi.strategy.LimitingStrategy;

import java.util.Optional;

public class DatabasePersistenceServiceImpl implements PersistenceService
{
    private final DBConnection db;
    private final LimitingStrategy limitingStrategy;

    public DatabasePersistenceServiceImpl() {
        this( Config.getDBConnection(), Config.getLimitingStrategy());
    }

    public DatabasePersistenceServiceImpl(final DBConnection db, final LimitingStrategy strategy) {
        this.db = db;
        this.limitingStrategy = strategy;
    }

    @Override
    public void close() {
        db.close();
    }

    public void store(final EnhancedRuuviMeasurement measurement) {
        Optional.ofNullable(measurement.getMac())
            .map(Config::getLimitingStrategy)
            .orElse(limitingStrategy)
            .apply(measurement)
            .ifPresent(db::save);
    }
}
